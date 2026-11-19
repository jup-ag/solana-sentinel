use crate::claim::{self, OpenClaims};
use domain::alert::Alert;
use domain::rules::RuleId;
use parking_lot::Mutex;
use redis::cluster_async::ClusterConnection;
use std::collections::HashMap;
use std::ops::Sub;
use std::sync::Arc;
use std::time::Duration;

/// Claim TTL for rules without a cooldown period, unless overridden.
const DEFAULT_TTL: Duration = Duration::from_secs(10 * 60);

#[derive(Clone)]
enum Backend {
    Redis {
        conn: ClusterConnection,
        owner: String,
        open: OpenClaims,
    },
    /// Single-process stand-in; a "fleet" of one.
    Memory(Arc<Mutex<HashMap<String, i64>>>),
}

// Layer 2 — inter-node dedup. Identifies a single alert occurrence as seen
// by any node in the fleet and is used to gate repeated alerts from other nodes
#[derive(Clone)]
pub struct FleetGate {
    backend: Backend,
    periods: HashMap<RuleId, Duration>,
    default_ttl: Duration,
    key_prefix: String,
    strict: bool,
}

impl FleetGate {
    /// `namespace` prefixes every Redis key; all pods of one deployment must share it.
    /// `pod_id` owns this node's claims until they are settled.
    pub fn new(
        redis_conn: ClusterConnection,
        periods: HashMap<RuleId, Duration>,
        namespace: &str,
        pod_id: &str,
    ) -> Self {
        Self {
            backend: Backend::Redis {
                conn: redis_conn,
                owner: pod_id.to_string(),
                open: OpenClaims::default(),
            },
            periods,
            default_ttl: DEFAULT_TTL,
            key_prefix: format!("{namespace}:alerts:fleet_gate_key"),
            strict: false,
        }
    }

    /// No fleet Redis: dedup within this process only.
    pub fn in_memory(periods: HashMap<RuleId, Duration>) -> Self {
        Self {
            backend: Backend::Memory(Default::default()),
            periods,
            default_ttl: DEFAULT_TTL,
            key_prefix: "memory".to_string(),
            strict: false,
        }
    }

    /// Claim TTL for rules that have no cooldown period of their own.
    pub fn with_default_ttl(mut self, ttl: Duration) -> Self {
        self.default_ttl = ttl;
        self
    }

    /// For actions that must not repeat: one claim per condition (not per occurrence) for the
    /// whole period, and an unsettled claim is never taken over, even by its own pod.
    #[must_use]
    pub fn strict(mut self) -> Self {
        self.strict = true;
        self
    }

    pub async fn check(&mut self, alert: &Alert) -> anyhow::Result<bool> {
        let period = self.periods.get(&alert.rule).unwrap_or(&self.default_ttl);

        // Strict: from claim time, since a past EXAT is accepted and expires at once, so an
        // item that queued past its period would claim nothing. Otherwise from `fired_at`,
        // minus 1 s so the fleet key never outlives the local cooldown key by a boundary second.
        let now = chrono::Utc::now().timestamp();
        let ttl_timestamp = if self.strict {
            now.saturating_add(period.as_secs() as i64)
        } else {
            alert
                .fired_at
                .timestamp()
                .saturating_add(period.as_secs() as i64)
                .sub(1)
        };

        let key = self.key(alert);
        let strict = self.strict;

        match &mut self.backend {
            Backend::Redis { conn, owner, open } => {
                let owner = claim_owner(owner, strict, alert);
                claim::claim(conn, open, &key, &owner, ttl_timestamp).await
            }
            Backend::Memory(map) => Ok(memory_set_nx(&mut map.lock(), key, ttl_timestamp, now)),
        }
    }

    pub async fn confirm(&mut self, alert: &Alert) -> anyhow::Result<()> {
        let key = self.key(alert);
        let strict = self.strict;
        match &mut self.backend {
            Backend::Redis { conn, owner, open } => {
                claim::confirm(conn, open, &key, &claim_owner(owner, strict, alert)).await
            }
            Backend::Memory(_) => Ok(()),
        }
    }

    pub async fn release(&mut self, alert: &Alert) -> anyhow::Result<()> {
        let key = self.key(alert);
        let strict = self.strict;
        match &mut self.backend {
            Backend::Redis { conn, owner, open } => {
                claim::release(conn, open, &key, &claim_owner(owner, strict, alert)).await
            }
            Backend::Memory(map) => {
                map.lock().remove(&key);
                Ok(())
            }
        }
    }

    fn key(&self, alert: &Alert) -> String {
        // The local key is the condition alone; the fleet key adds the occurrence (tx, slot).
        let key = if self.strict {
            alert.local_gate_key()
        } else {
            alert.fleet_gate_key()
        };
        format!("{}:{key}", self.key_prefix)
    }
}

/// Strict claims are owned by the alert, not the pod: a lost-reply retry still sees its own
/// claim, while a restarted pod (new alert) never takes one over.
fn claim_owner(pod: &str, strict: bool, alert: &Alert) -> String {
    if strict {
        format!("{pod}:{}", alert.id)
    } else {
        pod.to_string()
    }
}

/// In-memory equivalent of `SET NX EXAT`; expired keys are dropped first.
fn memory_set_nx(map: &mut HashMap<String, i64>, key: String, expiry: i64, now: i64) -> bool {
    map.retain(|_, exp| *exp > now);
    if map.contains_key(&key) {
        return false;
    }
    map.insert(key, expiry);
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use domain::alert::{Alert, DedupKey, DedupScope, HashKey, Severity};
    use domain::programs::Program;
    use domain::rules::RuleId;
    use redis::cluster::ClusterClient;
    use std::collections::BTreeMap;
    use std::time::Duration;
    use testcontainers::ImageExt;
    use testcontainers_modules::redis::{Redis, REDIS_PORT};
    use testcontainers_modules::testcontainers::runners::AsyncRunner;
    use testcontainers_modules::testcontainers::ContainerAsync;

    async fn redis_conn() -> (ClusterConnection, ContainerAsync<Redis>) {
        let container = Redis::default()
            .with_tag("7-alpine")
            .with_cmd(["redis-server", "--cluster-enabled", "yes"])
            .start()
            .await
            .unwrap();

        let port = container.get_host_port_ipv4(REDIS_PORT).await.unwrap();
        let url = format!("redis://127.0.0.1:{port}");

        // Bootstrap the single-node cluster over a plain connection.
        let mut boot = redis::Client::open(url.clone())
            .unwrap()
            .get_connection_manager()
            .await
            .unwrap();

        for (k, v) in [
            ("cluster-announce-ip", "127.0.0.1".to_string()),
            ("cluster-announce-port", port.to_string()),
        ] {
            let _: () = redis::cmd("CONFIG")
                .arg("SET")
                .arg(k)
                .arg(v)
                .query_async(&mut boot)
                .await
                .unwrap();
        }

        let _: () = redis::cmd("CLUSTER")
            .arg("ADDSLOTSRANGE")
            .arg(0)
            .arg(16383)
            .query_async(&mut boot)
            .await
            .unwrap();

        // Slots are assigned asynchronously — state reads `fail` for a beat.
        let mut ready = false;
        for _ in 0..40 {
            let info: String = redis::cmd("CLUSTER")
                .arg("INFO")
                .query_async(&mut boot)
                .await
                .unwrap();
            if info.contains("cluster_state:ok") {
                ready = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(ready, "single-node cluster never reached cluster_state:ok");

        let client = ClusterClient::builder(vec![url]).build().unwrap();
        let conn = client.get_async_connection().await.unwrap();
        (conn, container)
    }

    const RULE: RuleId = RuleId::new(Program::new("test_program"), "authority_changed");

    // Same seed => same dedup key, so two calls build genuine duplicates of one condition.
    fn build_alert(seed: usize) -> Alert {
        let map = BTreeMap::from([("key", format!("value_{seed}"))]);
        Alert::new(
            RULE,
            Severity::Critical,
            format!("condition_{seed}"),
            map,
            &HashKey::new(&[format!("condition_{seed}")]),
            DedupScope::Tx(format!("sig_{seed}")),
        )
    }

    fn periods(period: Duration) -> HashMap<RuleId, Duration> {
        HashMap::from([(RULE, period)])
    }

    #[tokio::test]
    #[ignore = "needs Docker (testcontainers); run with --ignored"]
    async fn concurrent_duplicate_checks_send_exactly_once() {
        let (conn, _c) = redis_conn().await;

        // 16 separate gates (one per node) each rebuild their own alert from one seed:
        // distinct objects, one shared key, racing on the cluster.
        let handles: Vec<_> = (0..16)
            .map(|i| {
                let node = format!("node_{i}");
                let mut gate = FleetGate::new(
                    conn.clone(),
                    periods(Duration::from_secs(60)),
                    "test",
                    &node,
                );
                tokio::spawn(async move { gate.check(&build_alert(0)).await.unwrap() })
            })
            .collect();

        let mut sent = 0;
        for h in handles {
            if h.await.unwrap() {
                sent += 1;
            }
        }
        assert_eq!(sent, 1, "exactly one node across the fleet sends");
    }

    #[tokio::test]
    #[ignore = "needs Docker (testcontainers); run with --ignored"]
    async fn strict_gate_blocks_restart_takeover_and_other_occurrences() {
        let (conn, _c) = redis_conn().await;
        let strict = || FleetGate::new(conn.clone(), periods(Duration::from_secs(60)), "test", "pod").strict();

        let alert = build_alert(0);
        assert!(strict().check(&alert).await.unwrap());
        // A retried SET whose first reply was lost: same alert, key already ours.
        assert!(strict().check(&alert).await.unwrap(), "lost-reply retry suppressed");
        // Same pod after a crash before confirm: the replay raises a new alert.
        assert!(!strict().check(&build_alert(0)).await.unwrap(), "restart took over");

        // Same condition in another tx: the strict key ignores the occurrence.
        let mut other_tx = build_alert(0);
        other_tx.fleet_gate_key = DedupKey::new(RULE, "other-occurrence".into());
        assert!(!strict().check(&other_tx).await.unwrap(), "second occurrence sent");
    }
}
