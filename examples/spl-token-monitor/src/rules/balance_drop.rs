//! Account rule: a watched token account's balance fell by at least `drop_bps`
//! between two consecutive updates. Shows `StateCache` seeded in `bootstrap`.

use crate::PROGRAM;
use async_trait::async_trait;
use serde::Deserialize;
use shipstern_spl_token_parser::account::Account as TokenState;
use shipstern_spl_token_parser::TokenProgramState;
use solana_program_pack::Pack;
use solana_pubkey::Pubkey;
use solana_sentinel::utility::pubkey::deserialize_pubkey_vec;
use solana_sentinel::utility::rpc::try_fetch_accounts_with_slot;
use solana_sentinel::{
    Alert, BootstrapCtx, GrpcRule, RuleDef, RuleId, Severity, StateCache, StreamCtx, Subscription,
};

#[derive(Debug, Deserialize)]
pub struct BalanceDropConfig {
    #[serde(deserialize_with = "deserialize_pubkey_vec")]
    pub accounts: Vec<Pubkey>,
    /// Drop between consecutive updates, in basis points (2000 = 20%).
    pub drop_bps: u64,
}

#[derive(Debug)]
pub struct BalanceDrop {
    accounts: Vec<Pubkey>,
    drop_bps: u64,
    /// Last seen balance per account. An unknown key is stored as the baseline
    /// and skipped, so an account missing from bootstrap still gets evaluated later.
    last: StateCache<Pubkey, u64>,
}

#[async_trait]
impl RuleDef for BalanceDrop {
    const ID: RuleId = RuleId::new(PROGRAM, "balance_drop");
    type Config = BalanceDropConfig;
    type Deps = ();

    fn build(cfg: Self::Config, _deps: &()) -> anyhow::Result<Self> {
        anyhow::ensure!(
            (1..=10_000).contains(&cfg.drop_bps),
            "drop_bps must be within 1..=10000"
        );
        Ok(Self {
            accounts: cfg.accounts,
            drop_bps: cfg.drop_bps,
            last: StateCache::default(),
        })
    }

    /// Seed the cache from RPC so the first streamed update is already a comparison.
    async fn bootstrap(&mut self, ctx: &BootstrapCtx) -> anyhow::Result<()> {
        let (fetched, _slot) = try_fetch_accounts_with_slot(&ctx.rpc, &self.accounts, None).await?;
        self.last
            .seed(fetched.iter().filter_map(|(pubkey, account)| {
                let state = spl_token_interface::state::Account::unpack(&account.data).ok()?;
                Some((*pubkey, state.amount))
            }));
        Ok(())
    }
}

impl GrpcRule for BalanceDrop {
    type Event = TokenProgramState;

    /// Subscribe to exactly these pubkeys; the parser's own prefilter adds the owner.
    fn static_subscription(cfg: &Self::Config) -> Subscription {
        Subscription::new().accounts(cfg.accounts.iter().copied())
    }

    fn evaluate(&self, ev: &Self::Event, ctx: &StreamCtx) -> Option<Alert> {
        let TokenState::TokenAccount(token) = ev.account.as_ref()? else {
            return None;
        };
        let account = ctx.account?;
        let previous = self.last.replace(Self::ID, account, token.amount)?;
        if previous == 0 || token.amount >= previous {
            return None;
        }
        let drop_bps = (previous - token.amount) * 10_000 / previous;
        if drop_bps < self.drop_bps {
            return None;
        }
        Some(
            Alert::for_rule(Self::ID)
                .severity(Severity::Critical)
                .message(format!(
                    "{account} balance dropped {}.{:02}% ({previous} -> {})",
                    drop_bps / 100,
                    drop_bps % 100,
                    token.amount
                ))
                .meta("account", account)
                .meta("previous", previous)
                .meta("current", token.amount)
                .meta("slot", ctx.slot)
                .key([account])
                .scope(ctx.tx_scope())
                .build(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use shipstern_spl_token_parser::AccountParser;
    use solana_program_option::COption;
    use solana_sentinel::testing::{account_update, parse, rule_from_toml, stream_ctx};
    use solana_sentinel::{assert_fires, GrpcRule};
    use spl_token_interface::state::{Account, AccountState};

    fn token_account(amount: u64) -> Vec<u8> {
        let state = Account {
            mint: Pubkey::new_unique(),
            owner: Pubkey::new_unique(),
            amount,
            delegate: COption::None,
            state: AccountState::Initialized,
            is_native: COption::None,
            delegated_amount: 0,
            close_authority: COption::None,
        };
        let mut data = vec![0u8; Account::LEN];
        Account::pack(state, &mut data).unwrap();
        data
    }

    #[tokio::test]
    async fn fires_once_balance_falls_past_threshold() {
        let account = Pubkey::new_unique();
        let rule: BalanceDrop =
            rule_from_toml(&format!("accounts = [\"{account}\"]\ndrop_bps = 2000"));
        let mut ctx = stream_ctx(1, Some("sig"));
        ctx.account = Some(account);
        let update =
            |amount| account_update(account, spl_token_interface::ID, token_account(amount));

        // Without bootstrap the first update is the baseline and never fires.
        let first = parse(&AccountParser, &update(1_000)).await;
        assert!(GrpcRule::evaluate(&rule, &first, &ctx).is_none());
        // 10% down: below the threshold.
        let dip = parse(&AccountParser, &update(900)).await;
        assert!(GrpcRule::evaluate(&rule, &dip, &ctx).is_none());
        // 900 -> 600 is a third: fires.
        let alert = assert_fires!(rule, parse(&AccountParser, &update(600)).await, ctx);
        assert_eq!(alert.metadata["previous"], "900");
    }
}
