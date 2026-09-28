use crate::parser::PriceFeedUpdate;
use chrono::Utc;
use interface::{Alert, PollCtx, Prefilter, RpcRule, RuleDef, RuleId, Severity};
use serde::Deserialize;
use solana_pubkey::Pubkey;
use std::collections::{HashMap, HashSet};
use std::time::Duration;
use tracing::debug;
use utility::pubkey::deserialize_pubkey_map;

#[derive(Debug, Deserialize)]
pub struct OracleStalenessConfig {
    /// Staleness threshold in seconds per oracle feed.
    #[serde(deserialize_with = "deserialize_pubkey_map")]
    pub thresholds: HashMap<Pubkey, u32>,
}

#[derive(Debug)]
pub struct OracleStaleness {
    config: OracleStalenessConfig,
}

impl RuleDef for OracleStaleness {
    const ID: RuleId = RuleId::new(crate::PROGRAM, "oracle_staleness");
    type Config = OracleStalenessConfig;
    type Deps = ();

    fn build(config: Self::Config, _deps: &()) -> anyhow::Result<Self> {
        Ok(Self { config })
    }
}

impl RpcRule for OracleStaleness {
    type Item = PriceFeedUpdate;

    fn accounts(cfg: &Self::Config) -> HashSet<Pubkey> {
        cfg.thresholds.keys().copied().collect()
    }

    fn evaluate(&self, item: &PriceFeedUpdate, _ctx: &PollCtx) -> Option<Alert> {
        let _ = Prefilter::new;
        let oracle_pubkey = item.pubkey;
        let threshold = self.config.thresholds.get(&oracle_pubkey).copied()?;
        let publish_timestamp = item.feed.publish_timestamp_seconds();

        // Wall-clock staleness: how old is the freshest data we have, right now.
        // `.max(0)` guards against a publish timestamp in the future (clock skew).
        let timestamp_now = Utc::now().timestamp();
        let staleness_seconds = timestamp_now.saturating_sub(publish_timestamp).max(0);

        debug!(
            "oracle feed {} staleness: {:?} (threshold: {:?})",
            oracle_pubkey,
            Duration::from_secs(staleness_seconds as u64),
            Duration::from_secs(threshold as u64),
        );

        if staleness_seconds <= i64::from(threshold) {
            return None;
        }

        Some(
            Alert::for_rule(Self::ID)
                .severity(Severity::Critical)
                .message(format!(
                    "Pythnet Oracle feed {} is stale (Last Update {:?} ago)",
                    oracle_pubkey,
                    Duration::from_mins((staleness_seconds / 60) as u64),
                ))
                .meta("oracle", oracle_pubkey)
                .meta("threshold", threshold)
                .meta("last_published_timestamp", publish_timestamp)
                .meta("staleness_seconds", staleness_seconds)
                // Key on the oracle alone: including the publish timestamp would mint a fresh
                // dedup key on every late publish, so a slow-but-alive feed would alert each time.
                .key([oracle_pubkey])
                .build(),
        )
    }
}
