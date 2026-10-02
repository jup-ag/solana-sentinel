use crate::local_gate::set_nx_exat;
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
    Redis(ClusterConnection),
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
}

impl FleetGate {
    /// `namespace` prefixes every Redis key; all pods of one deployment must share it.
    pub fn new(
        redis_conn: ClusterConnection,
        periods: HashMap<RuleId, Duration>,
        namespace: &str,
    ) -> Self {
        Self {
            backend: Backend::Redis(redis_conn),
            periods,
            default_ttl: DEFAULT_TTL,
            key_prefix: format!("{namespace}:alerts:fleet_gate_key"),
        }
    }

    /// No fleet Redis: dedup within this process only.
    pub fn in_memory(periods: HashMap<RuleId, Duration>) -> Self {
        Self {
            backend: Backend::Memory(Default::default()),
            periods,
            default_ttl: DEFAULT_TTL,
            key_prefix: "memory".to_string(),
        }
    }

    /// Claim TTL for rules that have no cooldown period of their own.
    pub fn with_default_ttl(mut self, ttl: Duration) -> Self {
        self.default_ttl = ttl;
        self
    }

    pub async fn check(&mut self, alert: &Alert) -> anyhow::Result<bool> {
        let period = self.periods.get(&alert.rule).unwrap_or(&self.default_ttl);

        // Subtract 1 second so the fleet key never outlives the cooldown key by a boundary
        // second, which would suppress the first alert after cooldown expiry.
        let ttl_timestamp = alert
            .fired_at
            .timestamp()
            .saturating_add(period.as_secs() as i64)
            .sub(1);

        let key = format!("{}:{}", self.key_prefix, alert.fleet_gate_key());

        match &mut self.backend {
            Backend::Redis(conn) => set_nx_exat(conn, &key, ttl_timestamp).await,
            Backend::Memory(map) => Ok(memory_set_nx(
                &mut map.lock(),
                key,
                ttl_timestamp,
                alert.fired_at.timestamp(),
            )),
        }
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
    use domain::alert::{Alert, DedupScope, HashKey, Severity};
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
            .map(|_| {
                let mut gate =
                    FleetGate::new(conn.clone(), periods(Duration::from_secs(60)), "test");
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
}
