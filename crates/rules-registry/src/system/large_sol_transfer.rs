//! Instruction rule: a `Transfer` / `TransferWithSeed` out of a watched
//! account moves at least `min_lamports`.

use crate::system::instruction::{decode, SystemIx};
use crate::system::PROGRAM;
use serde::Deserialize;
use solana_pubkey::Pubkey;
use solana_sentinel::raw::RawInstruction;
use solana_sentinel::utility::pubkey::deserialize_pubkey_vec;
use solana_sentinel::{Alert, Deps, RuleId, Severity, StreamCtx, StreamRule, Subscription};
use std::collections::HashSet;

#[derive(Debug, Deserialize)]
pub struct LargeSolTransferConfig {
    /// Accounts to watch as the transfer source.
    #[serde(deserialize_with = "deserialize_pubkey_vec")]
    pub accounts: Vec<Pubkey>,
    pub min_lamports: u64,
}

#[derive(Debug)]
pub struct LargeSolTransfer {
    watched: HashSet<Pubkey>,
    min_lamports: u64,
}

impl StreamRule for LargeSolTransfer {
    const ID: RuleId = RuleId::new(PROGRAM, "large_sol_transfer");
    type Config = LargeSolTransferConfig;
    type Event = RawInstruction;

    fn subscription(cfg: &Self::Config) -> Subscription {
        Subscription::new().tx_accounts_include(cfg.accounts.iter().copied())
    }

    fn build(cfg: Self::Config, _deps: &Deps) -> anyhow::Result<Self> {
        anyhow::ensure!(!cfg.accounts.is_empty(), "accounts must not be empty");
        Ok(Self {
            watched: cfg.accounts.into_iter().collect(),
            min_lamports: cfg.min_lamports,
        })
    }

    fn evaluate(&self, ix: &RawInstruction, ctx: &StreamCtx) -> Option<Alert> {
        let SystemIx::Transfer { from, to, lamports } = decode(ix)? else {
            return None;
        };
        if !self.watched.contains(&from) || lamports < self.min_lamports {
            return None;
        }
        Some(
            Alert::for_rule(Self::ID)
                .severity(Severity::Warn)
                .message(format!(
                    "{lamports} lamports left {from} for {to} in {}",
                    ctx.tx_sig()
                ))
                .meta("from", from)
                .meta("to", to)
                .meta("lamports", lamports)
                .meta("slot", ctx.slot)
                .key([from.to_string(), ctx.tx_sig().to_string()])
                .scope(ctx.tx_scope())
                .build(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::system::SYSTEM_PROGRAM_ID;
    use solana_sentinel::assert_fires;
    use solana_sentinel::raw::RawInstructionParser;
    use solana_sentinel::testing::{config_from_toml, ix_update, parse, stream_ctx};

    fn transfer(lamports: u64) -> Vec<u8> {
        let mut data = 2u32.to_le_bytes().to_vec();
        data.extend_from_slice(&lamports.to_le_bytes());
        data
    }

    #[tokio::test]
    async fn fires_on_large_transfer_from_watched_account() {
        let from = Pubkey::new_unique();
        let to = Pubkey::new_unique();
        let cfg = config_from_toml(&format!("accounts = [\"{from}\"]\nmin_lamports = 1000"));
        let rule = LargeSolTransfer::build(cfg, &Deps::default()).unwrap();
        let ctx = stream_ctx(1, Some("sig"));

        let big = ix_update(SYSTEM_PROGRAM_ID, &[from, to], transfer(5_000));
        let alert = assert_fires!(rule, parse(&RawInstructionParser, &big).await, ctx);
        assert_eq!(alert.metadata["lamports"], "5000");

        let small = ix_update(SYSTEM_PROGRAM_ID, &[from, to], transfer(10));
        assert!(rule
            .evaluate(&parse(&RawInstructionParser, &small).await, &ctx)
            .is_none());
        let other_way = ix_update(SYSTEM_PROGRAM_ID, &[to, from], transfer(5_000));
        assert!(rule
            .evaluate(&parse(&RawInstructionParser, &other_way).await, &ctx)
            .is_none());
    }
}
