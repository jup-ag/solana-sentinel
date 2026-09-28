//! Reserve state shared by every jup_lend rule. Fetched once before any rule
//! is built, refreshed from the stream, and re-scanned periodically so vaults
//! of reserves created after startup get subscribed.

use crate::programs::liquidity::{shares_to_tokens, TokenReserve, JUP_LIQUIDITY_PROGRAM_ID};
use anyhow::Context;
use async_trait::async_trait;
use borsh::BorshDeserialize;
use interface::{BootstrapCtx, Prefilter, ProgramDeps};
use parking_lot::Mutex;
use solana_pubkey::Pubkey;
use solana_rpc_client::api::config::{
    RpcAccountInfoConfig, RpcProgramAccountsConfig, UiAccountEncoding,
};
use solana_rpc_client::api::filter::{Memcmp, RpcFilterType};
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};
use utility::rpc::try_fetch_mints_from_rpc;

const REFRESH_INTERVAL: Duration = Duration::from_secs(5 * 60);

#[derive(Debug, Default)]
struct Inner {
    /// Reserve pubkey -> latest known reserve state, seeded at bootstrap and refreshed as account updates stream in.
    reserves: Mutex<HashMap<Pubkey, TokenReserve>>,
    /// Vault token account -> owning reserve; kept in step with `reserves`.
    vault_to_reserve: Mutex<HashMap<Pubkey, Pubkey>>,
    /// Reserve mint -> decimals; display-only (a fetch failure degrades alerts to base units).
    mint_decimals: Mutex<HashMap<Pubkey, u8>>,
}

#[derive(Debug, Clone, Default)]
pub struct JupLendCache(Arc<Inner>);

/// Supplied and borrowed legs of a reserve in token units.
pub fn normalize(reserve: &TokenReserve) -> (u128, u128) {
    // `with_interest` holds share counts scaled by the exchange price; `interest_free` is already token units.
    let supplied = shares_to_tokens(
        reserve.total_supply_with_interest,
        reserve.supply_exchange_price,
    ) + u128::from(reserve.total_supply_interest_free);
    let borrowed = shares_to_tokens(
        reserve.total_borrow_with_interest,
        reserve.borrow_exchange_price,
    ) + u128::from(reserve.total_borrow_interest_free);
    (supplied, borrowed)
}

#[async_trait]
impl ProgramDeps for JupLendCache {
    async fn bootstrap(ctx: &BootstrapCtx) -> anyhow::Result<Self> {
        let cache = Self::default();
        let (reserves, mints) = cache.scan(&ctx.rpc).await?;
        info!(reserves, mints, "jup_lend: shared reserve cache fetched");
        Ok(cache)
    }

    /// Vaults are plain token accounts and can only be subscribed by pubkey.
    fn dynamic_prefilter(&self) -> Prefilter {
        Prefilter::new().accounts(self.0.vault_to_reserve.lock().keys().copied())
    }

    fn refresh_interval(&self) -> Option<Duration> {
        Some(REFRESH_INTERVAL)
    }

    async fn refresh(&self, ctx: &BootstrapCtx) -> anyhow::Result<()> {
        let (reserves, mints) = self.scan(&ctx.rpc).await?;
        info!(reserves, mints, "jup_lend: reserve cache refreshed");
        Ok(())
    }
}

impl JupLendCache {
    /// Scan every reserve on chain. New reserves are inserted; known ones keep the
    /// stream's fresher state. Returns the reserve and mint counts.
    async fn scan(&self, rpc: &RpcClient) -> anyhow::Result<(usize, usize)> {
        let fetched = fetch_reserves(rpc).await?;

        let mut mints: Vec<Pubkey> = fetched
            .values()
            .map(|reserve| Pubkey::new_from_array(reserve.mint.0))
            .collect();
        mints.sort_unstable();
        mints.dedup();

        {
            let mut reserves = self.0.reserves.lock();
            let mut vaults = self.0.vault_to_reserve.lock();
            for (pubkey, reserve) in fetched {
                vaults.insert(Pubkey::new_from_array(reserve.vault.0), pubkey);
                reserves.entry(pubkey).or_insert(reserve);
            }
        }

        // Decimals are display-only; a fetch failure degrades alerts to base units rather than disabling the rules.
        match try_fetch_mints_from_rpc(rpc, &mints, None).await {
            Ok(fetched) => {
                let mut decimals = self.0.mint_decimals.lock();
                for (mint_pubkey, mint) in fetched {
                    decimals.insert(mint_pubkey, mint.decimals as u8);
                }
            }
            Err(e) => warn!(
                error = ?e,
                "jup_lend: failed to fetch reserve mint accounts; alerts will report base units",
            ),
        }

        Ok((self.reserve_count(), self.0.mint_decimals.lock().len()))
    }

    /// Snapshot of every reserve, for seeding per-rule state.
    pub fn reserves(&self) -> HashMap<Pubkey, TokenReserve> {
        self.0.reserves.lock().clone()
    }

    /// Latest known state of one reserve.
    pub fn reserve(&self, pubkey: &Pubkey) -> Option<TokenReserve> {
        self.0.reserves.lock().get(pubkey).cloned()
    }

    pub fn reserve_count(&self) -> usize {
        self.0.reserves.lock().len()
    }

    /// The reserve a vault token account belongs to.
    pub fn reserve_for_vault(&self, vault: &Pubkey) -> Option<Pubkey> {
        self.0.vault_to_reserve.lock().get(vault).copied()
    }

    /// Replace a reserve's cached state with a newer observation from the account stream.
    pub fn update_reserve(&self, pubkey: Pubkey, reserve: TokenReserve) -> Option<TokenReserve> {
        self.0
            .vault_to_reserve
            .lock()
            .insert(Pubkey::new_from_array(reserve.vault.0), pubkey);
        self.0.reserves.lock().insert(pubkey, reserve)
    }

    /// Decimals of a reserve's mint, if the mint fetch succeeded.
    pub fn reserve_mint_decimals(&self, reserve: &TokenReserve) -> Option<u8> {
        self.mint_decimals(&Pubkey::new_from_array(reserve.mint.0))
    }

    pub fn mint_decimals(&self, mint: &Pubkey) -> Option<u8> {
        self.0.mint_decimals.lock().get(mint).copied()
    }
}

async fn fetch_reserves(rpc: &RpcClient) -> anyhow::Result<HashMap<Pubkey, TokenReserve>> {
    let config = RpcProgramAccountsConfig {
        filters: Some(vec![RpcFilterType::Memcmp(Memcmp::new_raw_bytes(
            TokenReserve::DISCRIMINATOR_OFFSET,
            TokenReserve::DISCRIMINATOR.to_vec(),
        ))]),
        account_config: RpcAccountInfoConfig {
            encoding: Some(UiAccountEncoding::Base64),
            commitment: Some(rpc.commitment()),
            ..Default::default()
        },
        with_context: Some(false),
        ..Default::default()
    };

    let accounts = rpc
        .get_program_ui_accounts_with_config(&JUP_LIQUIDITY_PROGRAM_ID, config)
        .await
        .context("failed to fetch jup_lend TokenReserve accounts")?;

    let mut reserves = HashMap::new();
    for (pubkey, acc) in accounts {
        let reserve = acc
            .data
            .decode()
            .context("account data not in a decodable encoding")
            .and_then(|data| {
                let body = data
                    .get(TokenReserve::DISCRIMINATOR_OFFSET + TokenReserve::DISCRIMINATOR.len()..)
                    .context("account data shorter than the discriminator prefix")?;
                TokenReserve::try_from_slice(body).map_err(Into::into)
            });
        match reserve {
            Ok(reserve) => {
                reserves.insert(pubkey, reserve);
            }
            Err(e) => warn!(
                reserve = %pubkey,
                error = ?e,
                "jup_lend: failed to decode TokenReserve; reserve excluded from all rules",
            ),
        }
    }
    Ok(reserves)
}
