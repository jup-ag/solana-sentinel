//! Instruction rule: a `Transfer` / `TransferChecked` out of a watched token
//! account at or above `min_amount`.

use crate::PROGRAM;
use serde::Deserialize;
use shipstern_spl_token_parser::instruction::Instruction;
use shipstern_spl_token_parser::TokenProgram;
use solana_pubkey::Pubkey;
use solana_sentinel::utility::pubkey::deserialize_pubkey_vec;
use solana_sentinel::utility::pubkey::to_solana_pubkey;
use solana_sentinel::{Alert, Deps, RuleId, Severity, StreamCtx, StreamRule, Subscription};
use std::collections::HashSet;

/// The `[rules.config]` table of this rule in `rules.toml`.
#[derive(Debug, Deserialize)]
pub struct LargeTransferConfig {
    /// Token accounts to watch as the transfer source.
    #[serde(deserialize_with = "deserialize_pubkey_vec")]
    pub accounts: Vec<Pubkey>,
    /// Raw token amount (not UI amount) that triggers the alert.
    pub min_amount: u64,
}

#[derive(Debug)]
pub struct LargeTransfer {
    watched: HashSet<Pubkey>,
    min_amount: u64,
}

impl StreamRule for LargeTransfer {
    const ID: RuleId = RuleId::new(PROGRAM, "large_transfer");
    type Config = LargeTransferConfig;
    type Event = TokenProgram;

    /// Only transactions touching a watched account reach the parser.
    fn subscription(cfg: &Self::Config) -> Subscription {
        Subscription::new().tx_accounts_include(cfg.accounts.iter().copied())
    }

    fn build(cfg: Self::Config, _deps: &Deps) -> anyhow::Result<Self> {
        anyhow::ensure!(!cfg.accounts.is_empty(), "accounts must not be empty");
        Ok(Self {
            watched: cfg.accounts.into_iter().collect(),
            min_amount: cfg.min_amount,
        })
    }

    fn evaluate(&self, ev: &Self::Event, ctx: &StreamCtx) -> Option<Alert> {
        let (source, destination, amount) = match ev.instruction.as_ref()? {
            Instruction::Transfer(t) => (
                to_solana_pubkey(t.accounts.source.0),
                to_solana_pubkey(t.accounts.destination.0),
                t.args.amount,
            ),
            Instruction::TransferChecked(t) => (
                to_solana_pubkey(t.accounts.source.0),
                to_solana_pubkey(t.accounts.destination.0),
                t.args.amount,
            ),
            _ => return None,
        };
        if !self.watched.contains(&source) || amount < self.min_amount {
            return None;
        }
        Some(
            Alert::for_rule(Self::ID)
                .severity(Severity::Warn)
                .message(format!("{amount} tokens left {source} in {}", ctx.tx_sig()))
                .meta("source", source)
                .meta("destination", destination)
                .meta("amount", amount)
                .meta("slot", ctx.slot)
                // Same source in the same transaction collapses; a new transfer alerts again.
                .key([source.to_string(), ctx.tx_sig().to_string()])
                .scope(ctx.tx_scope())
                .build(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use shipstern_spl_token_parser::InstructionParser;
    use solana_sentinel::assert_fires;
    use solana_sentinel::testing::{config_from_toml, ix_update, parse, stream_ctx};

    /// SPL Token `Transfer`: tag 3 followed by the little-endian amount.
    fn transfer_ix(amount: u64) -> Vec<u8> {
        let mut data = vec![3u8];
        data.extend_from_slice(&amount.to_le_bytes());
        data
    }

    #[tokio::test]
    async fn fires_on_large_transfer_from_watched_account() {
        let source = Pubkey::new_unique();
        let destination = Pubkey::new_unique();
        let owner = Pubkey::new_unique();
        let cfg = config_from_toml(&format!("accounts = [\"{source}\"]\nmin_amount = 1000"));
        let rule = LargeTransfer::build(cfg, &Deps::default()).unwrap();
        let ctx = stream_ctx(1, Some("sig"));

        let big = ix_update(
            spl_token_interface::ID,
            &[source, destination, owner],
            transfer_ix(5_000),
        );
        let alert = assert_fires!(rule, parse(&InstructionParser, &big).await, ctx);
        assert_eq!(alert.metadata["amount"], "5000");

        let small = ix_update(
            spl_token_interface::ID,
            &[source, destination, owner],
            transfer_ix(10),
        );
        assert!(rule
            .evaluate(&parse(&InstructionParser, &small).await, &ctx)
            .is_none());
    }
}
