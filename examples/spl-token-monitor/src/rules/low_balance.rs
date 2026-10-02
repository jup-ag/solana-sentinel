//! Poll rule: a watched token account is below `min_amount` at fetch time.
//! Poll rules see what the program's `PollSource` parsed, not the stream.

use crate::{TokenBalance, PROGRAM};
use serde::Deserialize;
use solana_pubkey::Pubkey;
use solana_sentinel::utility::pubkey::deserialize_pubkey_vec;
use solana_sentinel::{Alert, DedupScope, Deps, PollCtx, PollRule, RuleId, Severity};
use std::collections::HashSet;
use std::time::Duration;

#[derive(Debug, Deserialize)]
pub struct LowBalanceConfig {
    #[serde(deserialize_with = "deserialize_pubkey_vec")]
    pub accounts: Vec<Pubkey>,
    pub min_amount: u64,
    /// Overrides the program's default poll cadence for this rule.
    pub poll_interval_seconds: Option<u64>,
}

#[derive(Debug)]
pub struct LowBalance {
    watched: HashSet<Pubkey>,
    min_amount: u64,
}

impl PollRule for LowBalance {
    const ID: RuleId = RuleId::new(PROGRAM, "low_balance");
    type Config = LowBalanceConfig;
    type Item = TokenBalance;

    /// What the poll loop fetches for this rule; same-cadence rules share one request.
    fn accounts(cfg: &Self::Config) -> HashSet<Pubkey> {
        cfg.accounts.iter().copied().collect()
    }

    fn poll_interval(cfg: &Self::Config) -> Option<Duration> {
        cfg.poll_interval_seconds.map(Duration::from_secs)
    }

    fn build(cfg: Self::Config, _deps: &Deps) -> anyhow::Result<Self> {
        Ok(Self {
            watched: cfg.accounts.into_iter().collect(),
            min_amount: cfg.min_amount,
        })
    }

    fn evaluate(&self, item: &Self::Item, ctx: &PollCtx) -> Option<Alert> {
        if !self.watched.contains(&item.account) || item.amount >= self.min_amount {
            return None;
        }
        Some(
            Alert::for_rule(Self::ID)
                .severity(Severity::Warn)
                .message(format!(
                    "{} holds {} < {} (slot {})",
                    item.account, item.amount, self.min_amount, ctx.slot
                ))
                .meta("account", item.account)
                .meta("mint", item.mint)
                .meta("amount", item.amount)
                // Monitor scope + per-account key: one alert per cooldown while it stays low.
                .key([item.account])
                .scope(DedupScope::Monitor)
                .build(),
        )
    }
}
