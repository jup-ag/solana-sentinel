//! Account rule: a watched account's SOL balance is under `min_lamports`.
//! Monitor-scoped, so it alerts once per cooldown while the balance stays low.

use crate::system::PROGRAM;
use serde::Deserialize;
use solana_pubkey::Pubkey;
use solana_sentinel::raw::RawAccount;
use solana_sentinel::utility::pubkey::deserialize_pubkey_vec;
use solana_sentinel::{
    Alert, DedupScope, Deps, RuleId, Severity, StreamCtx, StreamRule, Subscription,
};
use std::collections::HashSet;

#[derive(Debug, Deserialize)]
pub struct LamportsBelowConfig {
    #[serde(deserialize_with = "deserialize_pubkey_vec")]
    pub accounts: Vec<Pubkey>,
    pub min_lamports: u64,
}

#[derive(Debug)]
pub struct LamportsBelow {
    watched: HashSet<Pubkey>,
    min_lamports: u64,
}

impl StreamRule for LamportsBelow {
    const ID: RuleId = RuleId::new(PROGRAM, "lamports_below");
    type Config = LamportsBelowConfig;
    type Event = RawAccount;

    fn subscription(cfg: &Self::Config) -> Subscription {
        Subscription::new().accounts(cfg.accounts.iter().copied())
    }

    fn build(cfg: Self::Config, _deps: &Deps) -> anyhow::Result<Self> {
        anyhow::ensure!(!cfg.accounts.is_empty(), "accounts must not be empty");
        Ok(Self {
            watched: cfg.accounts.into_iter().collect(),
            min_lamports: cfg.min_lamports,
        })
    }

    fn evaluate(&self, acc: &RawAccount, ctx: &StreamCtx) -> Option<Alert> {
        if !self.watched.contains(&acc.pubkey) || acc.lamports >= self.min_lamports {
            return None;
        }
        Some(
            Alert::for_rule(Self::ID)
                .severity(Severity::Warn)
                .message(format!(
                    "{} holds {} lamports < {} (slot {})",
                    acc.pubkey, acc.lamports, self.min_lamports, ctx.slot
                ))
                .meta("account", acc.pubkey)
                .meta("lamports", acc.lamports)
                .meta("min_lamports", self.min_lamports)
                .key([acc.pubkey])
                .scope(DedupScope::Monitor)
                .build(),
        )
    }
}
