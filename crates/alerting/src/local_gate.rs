use domain::alert::Alert;
use domain::rules::RuleId;
use parking_lot::Mutex;
use redis::aio::ConnectionManager;
use std::collections::HashMap;
use std::time::Duration;

/// Where the gate keeps its keys: per-pod Redis in production, process memory for local runs.
#[derive(Clone)]
enum Backend {
    Redis {
        conn: Box<ConnectionManager>,
        key_prefix: String,
    },
    /// Cooldown key -> unix expiry (seconds). Swept lazily on each check.
    Memory(std::sync::Arc<Mutex<HashMap<String, i64>>>),
}

// Layer 1 — intra-node dedup. Suppresses repeated alerts for the same condition
// within that rule's cooldown period, with a per-rule TTL.
#[derive(Clone)]
pub struct LocalGate {
    periods: HashMap<RuleId, Duration>,
    backend: Backend,
}

impl LocalGate {
    /// `namespace` prefixes every Redis key (one per deployment, e.g. the service name).
    pub fn new(
        redis_conn: ConnectionManager,
        periods: HashMap<RuleId, Duration>,
        pod_id: &str,
        namespace: &str,
    ) -> Self {
        Self {
            periods,
            backend: Backend::Redis {
                conn: Box::new(redis_conn),
                key_prefix: format!("{pod_id}:{namespace}:alerts:local_gate_key"),
            },
        }
    }

    /// No Redis: keys live in this process and are lost on restart.
    pub fn in_memory(periods: HashMap<RuleId, Duration>) -> Self {
        Self {
            periods,
            backend: Backend::Memory(Default::default()),
        }
    }

    // Returns true if this alert should be sent, false if it's still within its
    // cooldown window. A rule with no configured period is never suppressed.
    pub async fn check(&mut self, alert: &Alert) -> anyhow::Result<bool> {
        let Some(&period) = self.periods.get(&alert.rule) else {
            return Ok(true);
        };

        let ttl_timestamp = alert
            .fired_at
            .timestamp()
            .saturating_add(period.as_secs() as i64);

        match &mut self.backend {
            Backend::Redis { conn, key_prefix } => {
                let key = format!("{key_prefix}:{}", alert.local_gate_key());
                set_nx_exat(conn.as_mut(), &key, ttl_timestamp).await
            }
            Backend::Memory(map) => Ok(memory_set_nx(
                &mut map.lock(),
                alert.local_gate_key().to_string(),
                ttl_timestamp,
                alert.fired_at.timestamp(),
            )),
        }
    }
}

/// `SET key "" NX EXAT ts`: `Some(())` means we set it (send), `None` means present (suppress).
pub(crate) async fn set_nx_exat(
    conn: &mut impl redis::aio::ConnectionLike,
    key: &str,
    ttl_timestamp: i64,
) -> anyhow::Result<bool> {
    let set: Option<()> = redis::cmd("SET")
        .arg(key)
        .arg("")
        .arg("NX")
        .arg("EXAT")
        .arg(ttl_timestamp)
        .query_async(conn)
        .await?;
    Ok(set.is_some())
}

/// In-memory equivalent of `SET NX EXAT`; expired keys are dropped first.
pub(crate) fn memory_set_nx(
    map: &mut HashMap<String, i64>,
    key: String,
    expiry: i64,
    now: i64,
) -> bool {
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
    use std::collections::{BTreeMap, HashMap};
    use std::time::Duration;
    use testcontainers::runners::AsyncRunner;
    use testcontainers::ImageExt;
    use testcontainers_modules::redis::Redis;

    async fn redis_conn() -> (ConnectionManager, testcontainers::ContainerAsync<Redis>) {
        // EXAT requires Redis >= 6.2; the module's default image tag is older.
        let container = Redis::default().with_tag("7-alpine").start().await.unwrap();
        let port = container.get_host_port_ipv4(6379).await.unwrap();
        let client = redis::Client::open(format!("redis://127.0.0.1:{port}")).unwrap();
        let conn = ConnectionManager::new(client).await.unwrap();
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
        let gate = LocalGate::new(conn, periods(Duration::from_secs(60)), "TEST_NODE", "test");

        // Each task rebuilds its own alert from one seed: separate objects, one dedup key.
        let handles: Vec<_> = (0..16)
            .map(|_| {
                let mut g = gate.clone();
                tokio::spawn(async move { g.check(&build_alert(0)).await.unwrap() })
            })
            .collect();

        let mut sent = 0;
        for h in handles {
            if h.await.unwrap() {
                sent += 1;
            }
        }
        assert_eq!(
            sent, 1,
            "SET NX must elect exactly one sender across the race"
        );
    }

    #[tokio::test]
    #[ignore = "needs Docker (testcontainers); run with --ignored"]
    async fn alert_sent_again_after_cooldown_expires() {
        let (conn, _c) = redis_conn().await;
        let mut gate = LocalGate::new(conn, periods(Duration::from_secs(1)), "TEST_NODE", "test");

        assert!(
            gate.check(&build_alert(0)).await.unwrap(),
            "first should send"
        );
        assert!(
            !gate.check(&build_alert(0)).await.unwrap(),
            "duplicate suppressed"
        );

        tokio::time::sleep(Duration::from_millis(1_200)).await;

        assert!(
            gate.check(&build_alert(0)).await.unwrap(),
            "after cooldown expiry the same condition should send again"
        );
    }
}
