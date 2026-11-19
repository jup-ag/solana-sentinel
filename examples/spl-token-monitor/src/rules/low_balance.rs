//! Poll rule: a watched token account is below `min_amount` at fetch time.
//! Poll rules see what the program's `PollSource` parsed, not the stream.
//! With `[rules.config.refill]` it also counteracts: Riposte tops the account up from the
//! signer's associated token account of the same mint.

use crate::{TokenBalance, PROGRAM};
use anyhow::Context;
use serde::Deserialize;
use solana_pubkey::Pubkey;
use solana_sentinel::utility::pubkey::deserialize_pubkey_vec;
use solana_sentinel::{
    Alert, CounterMeasure, CounteractCtx, DedupScope, Deps, PollCtx, PollRule, RuleId, Severity,
};
use std::collections::HashSet;
use std::time::Duration;

/// Transfer costs ~5k CU; headroom for the runtime.
const REFILL_CU: u32 = 10_000;

const ATA_PROGRAM_ID: Pubkey =
    Pubkey::from_str_const("ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL");

/// Legacy Token associated token account of `owner` for `mint`.
fn ata(owner: &Pubkey, mint: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(
        &[owner.as_ref(), spl_token_interface::ID.as_ref(), mint.as_ref()],
        &ATA_PROGRAM_ID,
    )
    .0
}

#[derive(Debug, Deserialize)]
pub struct LowBalanceConfig {
    #[serde(deserialize_with = "deserialize_pubkey_vec")]
    pub accounts: Vec<Pubkey>,
    pub min_amount: u64,
    /// Overrides the program's default poll cadence for this rule.
    pub poll_interval_seconds: Option<u64>,
    /// Top up a low account from the ATA of the program's Riposte signer
    /// (`[riposte.programs] spl_token`); fund that ATA with the same mint.
    #[serde(default)]
    pub refill: Option<RefillConfig>,
}

#[derive(Debug, Deserialize)]
pub struct RefillConfig {
    /// Raw units sent per countermeasure.
    pub amount: u64,
}

#[derive(Debug)]
pub struct LowBalance {
    watched: HashSet<Pubkey>,
    min_amount: u64,
    refill: Option<RefillConfig>,
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
        if let Some(r) = &cfg.refill {
            anyhow::ensure!(r.amount > 0, "refill.amount must be > 0");
        }
        Ok(Self {
            watched: cfg.accounts.into_iter().collect(),
            min_amount: cfg.min_amount,
            refill: cfg.refill,
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

    /// Send `refill.amount` from the signer's ATA. Not safe to land twice: a repeat moves it
    /// again, and a drained reserve pulls `amount` per Riposte cooldown. Watch only accounts you own.
    fn counteract(
        &self,
        alert: &Alert,
        ctx: &CounteractCtx,
    ) -> anyhow::Result<Option<CounterMeasure>> {
        let Some(refill) = &self.refill else {
            return Ok(None);
        };
        // `evaluate` above set both.
        let account: Pubkey = alert.metadata["account"].parse().context("account")?;
        let mint: Pubkey = alert.metadata["mint"].parse().context("mint")?;
        let from = ata(&ctx.authority, &mint);
        anyhow::ensure!(from != account, "the signer's ATA is the low account itself");
        let transfer = spl_token_interface::instruction::transfer(
            &spl_token_interface::ID,
            &from,
            &account,
            &ctx.authority,
            &[],
            refill.amount,
        )?;
        Ok(Some(CounterMeasure {
            ixs: vec![transfer],
            compute_units: REFILL_CU,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sentinel::testing::{config_from_toml, poll_ctx};

    #[test]
    fn refills_low_account_from_signer_ata() {
        let (account, mint) = (Pubkey::new_unique(), Pubkey::new_unique());
        let cfg = config_from_toml(&format!(
            "accounts = [\"{account}\"]\nmin_amount = 100\nrefill = {{ amount = 500 }}"
        ));
        let rule = LowBalance::build(cfg, &Deps::default()).unwrap();
        let item = TokenBalance { account, mint, amount: 10 };
        let alert = rule.evaluate(&item, &poll_ctx(1)).expect("below min_amount");

        // Transfer(signer's ATA → account, authority as signer), amount 500.
        let authority = Pubkey::new_unique();
        let from = ata(&authority, &mint);
        let cm = rule.counteract(&alert, &CounteractCtx { authority }).unwrap().expect("refill configured");
        let ix = &cm.ixs[0];
        let accounts: Vec<_> = ix.accounts.iter().map(|a| (a.pubkey, a.is_signer)).collect();
        assert_eq!(accounts, vec![(from, false), (account, false), (authority, true)]);
        assert_eq!(ix.data[1..], 500u64.to_le_bytes());
    }
}
