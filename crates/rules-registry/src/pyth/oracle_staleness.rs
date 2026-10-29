//! Poll rule: a feed's last publish is older than its per-feed threshold, by wall clock.
//! Keyed on the feed alone, so a slow-but-alive feed alerts once per cooldown.

use crate::pyth::parser::PriceFeedUpdate;
use crate::pyth::PROGRAM;
use serde::Deserialize;
use solana_pubkey::Pubkey;
use solana_sentinel::utility::pubkey::deserialize_pubkey_map;
use solana_sentinel::{Alert, Deps, PollCtx, PollRule, RuleId, Severity};
use std::collections::{HashMap, HashSet};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::debug;

#[derive(Debug, Deserialize)]
pub struct OracleStalenessConfig {
    /// Staleness threshold in seconds, keyed by feed account.
    #[serde(deserialize_with = "deserialize_pubkey_map")]
    pub thresholds: HashMap<Pubkey, u32>,
    pub poll_interval_seconds: Option<u64>,
}

#[derive(Debug)]
pub struct OracleStaleness {
    thresholds: HashMap<Pubkey, u32>,
}

impl PollRule for OracleStaleness {
    const ID: RuleId = RuleId::new(PROGRAM, "oracle_staleness");
    type Config = OracleStalenessConfig;
    type Item = PriceFeedUpdate;

    fn accounts(cfg: &Self::Config) -> HashSet<Pubkey> {
        cfg.thresholds.keys().copied().collect()
    }

    fn poll_interval(cfg: &Self::Config) -> Option<Duration> {
        cfg.poll_interval_seconds.map(Duration::from_secs)
    }

    fn build(cfg: Self::Config, _deps: &Deps) -> anyhow::Result<Self> {
        anyhow::ensure!(!cfg.thresholds.is_empty(), "thresholds must not be empty");
        Ok(Self {
            thresholds: cfg.thresholds,
        })
    }

    fn evaluate(&self, item: &PriceFeedUpdate, _ctx: &PollCtx) -> Option<Alert> {
        let threshold = *self.thresholds.get(&item.pubkey)?;
        let published = item.feed.publish_timestamp_seconds();
        // `.max(0)` guards against a publish time in the future (clock skew).
        let staleness_seconds = unix_now().saturating_sub(published).max(0);
        debug!(oracle = %item.pubkey, staleness_seconds, threshold, "oracle staleness");
        if staleness_seconds <= i64::from(threshold) {
            return None;
        }
        Some(
            Alert::for_rule(Self::ID)
                .severity(Severity::Critical)
                .message(format!(
                    "Pyth oracle feed {} is stale (last update {:?} ago)",
                    item.pubkey,
                    Duration::from_secs(staleness_seconds.unsigned_abs()),
                ))
                .meta("oracle", item.pubkey)
                .meta("feed_id", item.feed.feed_id())
                .meta("threshold", threshold)
                .meta("last_published_timestamp", published)
                .meta("staleness_seconds", staleness_seconds)
                .key([item.pubkey])
                .build(),
        )
    }
}

fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs().cast_signed())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pyth::parser::fixtures::price_update_v2;
    use crate::pyth::parser::PriceFeedSource;
    use crate::pyth::PYTH_RECEIVER_PROGRAM_ID;
    use solana_account::Account;
    use solana_sentinel::testing::{config_from_toml, poll_ctx};
    use solana_sentinel::PollSource;

    #[test]
    fn polled_feed_past_threshold_fires() {
        let (fresh, stale) = (Pubkey::new_unique(), Pubkey::new_unique());
        let cfg = config_from_toml(&format!("[thresholds]\n\"{fresh}\" = 60\n\"{stale}\" = 60"));
        let rule = OracleStaleness::build(cfg, &Deps::default()).unwrap();
        let account = |publish_time| Account {
            lamports: 1,
            data: price_update_v2(15_000_000_000, -8, publish_time),
            owner: PYTH_RECEIVER_PROGRAM_ID,
            executable: false,
            rent_epoch: 0,
        };
        let fetched = HashMap::from([
            (fresh, account(unix_now() - 5)),
            (stale, account(unix_now() - 300)),
        ]);

        let alerts: Vec<_> = PriceFeedSource
            .parse(&fetched, &poll_ctx(1))
            .iter()
            .filter_map(|item| rule.evaluate(item, &poll_ctx(1)))
            .collect();
        assert_eq!(alerts.len(), 1);
        assert_eq!(alerts[0].metadata["oracle"], stale.to_string());
    }
}
