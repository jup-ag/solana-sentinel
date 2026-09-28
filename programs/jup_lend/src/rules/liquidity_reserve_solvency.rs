use crate::programs::liquidity::{LiquidityStateUpdate, TokenReserve};
use crate::rules::cache::{normalize, JupLendCache};
use interface::{
    Alert, DedupScope, GrpcRule, NoConfig, Prefilter, RuleDef, RuleId, Severity, StreamCtx,
};
use parking_lot::Mutex;
use solana_pubkey::Pubkey;
use spl_token_2022_interface::state::Account as TokenAccount;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tracing::debug;
use utility::math::token_amount_to_ui_amount_compact_u128;

/// An unpaired update older than this belongs to a transaction whose other leg
/// was never observed (e.g. a direct SPL transfer into the vault, or an
/// interest-accrual write that touched only the reserve). It can no longer be
/// completed, so it is dropped on the next cache access.
const PENDING_PAIR_TTL: Duration = Duration::from_secs(60);

#[derive(Debug, Clone, Copy)]
struct VaultAccount {
    token_account: TokenAccount,
    /// `None` when the bootstrap mint fetch failed; alerts report base units.
    mint_decimals: Option<u8>,
}

/// The two legs of one transaction's write to a reserve, buffered until both arrive — same transaction means same slot, so the pair is consistent.
#[derive(Debug)]
struct PendingPair {
    seen_at: Instant,
    reserve: Option<TokenReserve>,
    vault: Option<VaultAccount>,
}

impl PendingPair {
    fn empty() -> Self {
        Self {
            seen_at: Instant::now(),
            reserve: None,
            vault: None,
        }
    }
}

#[derive(Debug)]
pub struct LiquidityReserveSolvency {
    pending: Mutex<HashMap<(String, Pubkey), PendingPair>>,
    /// Vault -> reserve lookups and mint decimals come from the shared cache.
    cache: JupLendCache,
}

impl RuleDef for LiquidityReserveSolvency {
    const ID: RuleId = RuleId::new(crate::PROGRAM, "jup_lend_liquidity_reserve_solvency");
    type Config = NoConfig;
    type Deps = JupLendCache;

    fn build(_cfg: NoConfig, deps: &JupLendCache) -> anyhow::Result<Self> {
        Ok(Self {
            pending: Mutex::new(HashMap::new()),
            cache: deps.clone(),
        })
    }
}

impl LiquidityReserveSolvency {
    /// Merge one leg into the pending pair, returning it once complete; expired unpaired entries are swept on every call.
    fn merge_pending(
        &self,
        tx_signature: &str,
        reserve_pubkey: &Pubkey,
        reserve: Option<&TokenReserve>,
        vault: Option<VaultAccount>,
    ) -> Option<PendingPair> {
        let mut pending = self.pending.lock();

        pending.retain(|_, pair| pair.seen_at.elapsed() < PENDING_PAIR_TTL);

        let key = (tx_signature.to_string(), *reserve_pubkey);
        let pair = pending
            .entry(key.clone())
            .or_insert_with(PendingPair::empty);

        if let Some(reserve) = reserve {
            pair.reserve = Some(reserve.clone());
        }
        if let Some(vault) = vault {
            pair.vault = Some(vault);
        }

        if pair.reserve.is_some() && pair.vault.is_some() {
            return pending.remove(&key);
        }

        None
    }

    fn check_solvency(
        reserve_pubkey: &Pubkey,
        pair: &PendingPair,
        tx_signature: &str,
        scope: DedupScope,
    ) -> Option<Alert> {
        // Both legs come from the same transaction, so they describe the same post-transaction state.
        let (reserve, vault) = (pair.reserve.as_ref()?, pair.vault.as_ref()?);

        let vault_pubkey = Pubkey::new_from_array(reserve.vault.0);
        let reserve_mint = Pubkey::new_from_array(reserve.mint.0);

        let (total_supplied, total_borrowed) = normalize(reserve);
        let token_balance = u128::from(vault.token_account.amount);

        // Written as an addition so bad debt (`total_borrowed > total_supplied`) cannot underflow.
        let covered = token_balance + total_borrowed;
        if covered >= total_supplied {
            return None;
        }
        let shortfall = total_supplied - covered;

        debug!(
            reserve = %reserve_pubkey,
            vault = %vault_pubkey,
            mint = %reserve_mint,
            tx_signature,
            shortfall,
            token_balance,
            total_supplied,
            total_borrowed,
            "jup_lend: vault balance below supplied minus borrowed",
        );

        let alert = Alert::for_rule(Self::ID)
            .severity(Severity::Critical)
            .meta("reserve", reserve_pubkey)
            .meta("vault", vault_pubkey)
            .meta("reserve_mint", reserve_mint)
            .meta("tx_sig", tx_signature)
            .key([reserve_pubkey.to_string(), vault_pubkey.to_string()])
            .scope(scope);

        let alert = match vault.mint_decimals {
            Some(decimals) => {
                let ui_shortfall = token_amount_to_ui_amount_compact_u128(shortfall, decimals);
                let ui_total_supplied =
                    token_amount_to_ui_amount_compact_u128(total_supplied, decimals);
                let ui_total_borrowed =
                    token_amount_to_ui_amount_compact_u128(total_borrowed, decimals);
                let ui_token_balance =
                    token_amount_to_ui_amount_compact_u128(token_balance, decimals);
                alert
                    .message(format!(
                        "JUP Lend: reserve vault short by {ui_shortfall} tokens (supplied \
                         {ui_total_supplied}, borrowed {ui_total_borrowed}, vault balance \
                         {ui_token_balance})"
                    ))
                    .meta("shortfall", ui_shortfall)
                    .meta("total_supplied", ui_total_supplied)
                    .meta("total_borrowed", ui_total_borrowed)
                    .meta("vault_balance", ui_token_balance)
            }
            // Mint fetch failed at bootstrap; report base units rather than nothing.
            None => alert
                .message(format!(
                    "JUP Lend: reserve vault short by {shortfall} base units (supplied \
                     {total_supplied}, borrowed {total_borrowed}, vault balance {token_balance})",
                ))
                .meta("shortfall", shortfall)
                .meta("total_supplied", total_supplied)
                .meta("total_borrowed", total_borrowed)
                .meta("vault_balance", token_balance),
        };

        Some(alert.build())
    }
}

impl GrpcRule for LiquidityReserveSolvency {
    type Event = LiquidityStateUpdate;

    /// Reserves by owner; their vaults come from the shared cache's dynamic prefilter.
    fn static_prefilter(_cfg: &NoConfig) -> Prefilter {
        TokenReserve::subscription()
    }

    fn evaluate(&self, parsed: &LiquidityStateUpdate, _ctx: &StreamCtx) -> Option<Alert> {
        match parsed {
            LiquidityStateUpdate::TokenReserve {
                reserve_pubkey,
                reserve,
                tx_signature,
            } => {
                self.cache.update_reserve(*reserve_pubkey, reserve.clone());

                let pair = self.merge_pending(tx_signature, reserve_pubkey, Some(reserve), None)?;
                Self::check_solvency(reserve_pubkey, &pair, tx_signature, DedupScope::Monitor)
            }
            LiquidityStateUpdate::VaultAccount {
                vault_pubkey,
                token_account,
                tx_signature,
            } => {
                let reserve_pubkey = self.cache.reserve_for_vault(vault_pubkey)?;

                let vault = VaultAccount {
                    token_account: *token_account,
                    mint_decimals: self.cache.mint_decimals(&token_account.mint),
                };
                let pair = self.merge_pending(tx_signature, &reserve_pubkey, None, Some(vault))?;
                Self::check_solvency(&reserve_pubkey, &pair, tx_signature, DedupScope::Monitor)
            }
            _ => None,
        }
    }
}
