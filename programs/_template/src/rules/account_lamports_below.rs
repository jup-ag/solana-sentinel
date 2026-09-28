//! Account rule: alert when a watched account's balance drops below a floor.

use interface::raw::RawAccount;
use interface::{Alert, GrpcRule, Prefilter, RuleDef, RuleId, Severity, StreamCtx};
use serde::Deserialize;
use solana_pubkey::Pubkey;
use utility::pubkey::deserialize_pubkey_vec;

#[derive(Debug, Deserialize)]
pub struct AccountLamportsBelowConfig {
    #[serde(deserialize_with = "deserialize_pubkey_vec")]
    pub accounts: Vec<Pubkey>,
    pub min_lamports: u64,
}

#[derive(Debug)]
pub struct AccountLamportsBelow {
    config: AccountLamportsBelowConfig,
}

impl RuleDef for AccountLamportsBelow {
    // `rules.toml` refers to this rule as program = "template", name = "account_lamports_below".
    const ID: RuleId = RuleId::new(crate::PROGRAM, "account_lamports_below");
    type Config = AccountLamportsBelowConfig;
    type Deps = ();

    fn build(config: Self::Config, _deps: &()) -> anyhow::Result<Self> {
        Ok(Self { config })
    }
}

impl GrpcRule for AccountLamportsBelow {
    type Event = RawAccount;

    /// Subscribe exactly the configured accounts.
    fn static_prefilter(cfg: &Self::Config) -> Prefilter {
        Prefilter::new().accounts(cfg.accounts.iter().copied())
    }

    fn evaluate(&self, acc: &RawAccount, ctx: &StreamCtx) -> Option<Alert> {
        if !self.config.accounts.contains(&acc.pubkey) || acc.lamports >= self.config.min_lamports {
            return None;
        }
        Some(
            Alert::for_rule(Self::ID)
                .severity(Severity::Warn)
                .message(format!(
                    "Account {} balance {} below {}",
                    acc.pubkey, acc.lamports, self.config.min_lamports
                ))
                .meta("account", acc.pubkey)
                .meta("lamports", acc.lamports)
                // Same account + same balance collapses to one alert; the scope is per transaction.
                .key([acc.pubkey.to_string(), acc.lamports.to_string()])
                .scope(ctx.tx_scope())
                .build(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use interface::raw::RawAccountParser;
    use interface::testing::{account_update, parse, rule_from_toml, stream_ctx};

    #[tokio::test]
    async fn fires_when_balance_is_below_floor() {
        let watched = Pubkey::new_unique();
        let rule: AccountLamportsBelow =
            rule_from_toml(&format!("accounts = [\"{watched}\"]\nmin_lamports = 1000"));

        let mut update = account_update(watched, Pubkey::new_unique(), vec![]);
        update.account.as_mut().unwrap().lamports = 999;
        let event = parse(&RawAccountParser, &update).await;

        let alert = interface::assert_fires!(rule, event, stream_ctx(1, Some("sig")));
        assert_eq!(alert.metadata["lamports"], "999");
    }
}
