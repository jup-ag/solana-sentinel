use crate::cooldown_gate::{memory_set_nx, set_nx_exat};
use domain::alert::Alert;
use domain::rules::RuleId;
use parking_lot::Mutex;
use redis::cluster_async::ClusterConnection;
use std::collections::HashMap;
use std::ops::Sub;
use std::sync::Arc;
use std::time::Duration;

const ALERTS_FLEET_GATE_KEY_REDIS_TTL_SECONDS: usize = 10 * 60; // 10 mins

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
            key_prefix: format!("{namespace}:alerts:fleet_gate_key"),
        }
    }

    /// No fleet Redis: dedup within this process only.
    pub fn in_memory(periods: HashMap<RuleId, Duration>) -> Self {
        Self {
            backend: Backend::Memory(Default::default()),
            periods,
            key_prefix: "memory".to_string(),
        }
    }

    pub async fn check(&mut self, alert: &Alert) -> anyhow::Result<bool> {
        let period = match self.periods.get(&alert.rule) {
            Some(p) => p,
            None => &Duration::from_secs(ALERTS_FLEET_GATE_KEY_REDIS_TTL_SECONDS as u64),
        };

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

#[cfg(test)]
mod tests {
    use super::*;
    use domain::alert::{Alert, DedupScope, HashKey, Severity};
    use domain::programs::Program;
    use domain::rules::RuleId;
    use redis::cluster::ClusterClient;
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use testcontainers::ImageExt;
    use testcontainers_modules::redis::{Redis, REDIS_PORT};
    use testcontainers_modules::testcontainers::runners::AsyncRunner;
    use testcontainers_modules::testcontainers::ContainerAsync;

    const TEST_PROGRAMS: [&str; 10] = [
        "jupiter_swap",
        "jupiter_perps",
        "jupiter_doves",
        "squads_v4",
        "bpf_loader_upgradeable",
        "solana_vote",
        "kamino_lending",
        "meteora_dlmm",
        "raydium_launchlab",
        "pumpfun",
    ];

    const TEST_RULES: [&str; 10] = [
        "authority_changed",
        "upgrade_authority_set",
        "reference_price_drift",
        "oracle_staleness",
        "large_withdrawal",
        "commission_changed",
        "multisig_threshold_changed",
        "vote_account_hijack",
        "reserve_liquidity_drop",
        "unexpected_cpi",
    ];

    static SEQ: AtomicUsize = AtomicUsize::new(0);

    fn next() -> usize {
        SEQ.fetch_add(1, Ordering::Relaxed)
    }

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

    // Same seed => identical value, including scope. Two build_alert calls with
    // the same rule + seed produce the same fleet_gate_key, i.e. genuine
    // duplicates. Different seeds => distinct conditions.

    fn build_rule(seed: usize) -> RuleId {
        let program = TEST_PROGRAMS[seed % TEST_PROGRAMS.len()];
        let rule = TEST_RULES[seed % TEST_RULES.len()];
        RuleId::new(Program::new(program), rule)
    }

    fn build_scope(seed: usize) -> DedupScope {
        match seed % 3 {
            0 => DedupScope::Tx(format!("sig_{seed}")),
            1 => DedupScope::Slot(seed as u64),
            _ => DedupScope::Monitor,
        }
    }

    fn build_alert(rule: RuleId, seed: usize) -> Alert {
        let map = BTreeMap::from([("key", format!("value_{seed}")), ("rule", rule.to_string())]);
        Alert::new(
            rule,
            Severity::Critical,
            format!("condition_{seed}"),
            map,
            &HashKey::new(&[format!("condition_{seed}")]),
            build_scope(seed),
        )
    }

    fn random_rule() -> RuleId {
        build_rule(next())
    }

    fn periods_with(rule: RuleId, period: Duration) -> HashMap<RuleId, Duration> {
        let mut m = HashMap::new();
        m.insert(rule, period);
        m
    }

    #[tokio::test]
    #[ignore = "needs Docker (testcontainers); run with --ignored"]
    async fn first_sighting_is_sent() {
        let (conn, _c) = redis_conn().await;
        let rule = random_rule();
        let mut gate = FleetGate::new(conn, periods_with(rule, Duration::from_secs(60)), "test");
        let alert = build_alert(rule, next());

        assert!(gate.check(&alert).await.unwrap());
    }

    #[tokio::test]
    #[ignore = "needs Docker (testcontainers); run with --ignored"]
    async fn duplicate_sighting_is_suppressed() {
        let (conn, _c) = redis_conn().await;
        let rule = random_rule();
        let mut gate = FleetGate::new(conn, periods_with(rule, Duration::from_secs(60)), "test");

        // Rebuilt from the same seed => same shared key => genuine duplicate.
        let seed = next();
        let alert = build_alert(rule, seed);
        let duplicate = build_alert(rule, seed);

        assert!(gate.check(&alert).await.unwrap());
        assert!(!gate.check(&duplicate).await.unwrap());
    }

    #[tokio::test]
    #[ignore = "needs Docker (testcontainers); run with --ignored"]
    async fn different_conditions_are_independent() {
        let (conn, _c) = redis_conn().await;
        let rule = random_rule();
        let mut gate = FleetGate::new(conn, periods_with(rule, Duration::from_secs(60)), "test");

        // Same rule, different seeds => different shared keys.
        let seed_a = next();
        let alert_a = build_alert(rule, seed_a);
        let alert_b = build_alert(rule, next());

        assert!(gate.check(&alert_a).await.unwrap());
        assert!(!gate.check(&build_alert(rule, seed_a)).await.unwrap());
        assert!(gate.check(&alert_b).await.unwrap());
    }

    #[tokio::test]
    #[ignore = "needs Docker (testcontainers); run with --ignored"]
    async fn mixed_stream_sends_each_condition_once() {
        let (conn, _c) = redis_conn().await;
        let rule = random_rule();
        let mut gate = FleetGate::new(conn, periods_with(rule, Duration::from_secs(60)), "test");

        // 8 distinct conditions with fixed seeds, then rebuilt duplicates of the
        // even-indexed ones, rotated so dups don't sit next to their originals.
        let seeds: Vec<usize> = (0..8).map(|_| next()).collect();
        let distinct: Vec<Alert> = seeds.iter().map(|&s| build_alert(rule, s)).collect();
        let dups: Vec<Alert> = seeds
            .iter()
            .enumerate()
            .filter(|(i, _)| i % 2 == 0)
            .map(|(_, &s)| build_alert(rule, s))
            .collect();

        let mut stream: Vec<&Alert> = distinct.iter().collect();
        stream.extend(dups.iter());
        stream.rotate_left(3);

        let mut sent = 0usize;
        for alert in stream {
            if gate.check(alert).await.unwrap() {
                sent += 1;
            }
        }

        assert_eq!(sent, distinct.len());
    }

    #[tokio::test]
    #[ignore = "needs Docker (testcontainers); run with --ignored"]
    async fn separate_gates_share_state_across_the_fleet() {
        let (conn, _c) = redis_conn().await;
        let rule = random_rule();

        // Two nodes each build their OWN alert from the same seed — separate
        // objects, identical shared key — the real inter-node dedup case.
        let seed = next();
        let mut node_a = FleetGate::new(
            conn.clone(),
            periods_with(rule, Duration::from_secs(60)),
            "test",
        );
        let mut node_b = FleetGate::new(
            conn.clone(),
            periods_with(rule, Duration::from_secs(60)),
            "test",
        );

        assert!(node_a.check(&build_alert(rule, seed)).await.unwrap());
        assert!(!node_b.check(&build_alert(rule, seed)).await.unwrap());
    }

    #[tokio::test]
    #[ignore = "needs Docker (testcontainers); run with --ignored"]
    async fn concurrent_duplicate_checks_send_exactly_once() {
        let (conn, _c) = redis_conn().await;
        let rule = random_rule();

        // Each task rebuilds its own alert from one shared seed: 16 distinct
        // objects, one identical shared key, racing on Redis.
        let seed = next();

        let mut handles = Vec::new();
        for _ in 0..16 {
            let mut gate = FleetGate::new(
                conn.clone(),
                periods_with(rule, Duration::from_secs(60)),
                "test",
            );
            let a = build_alert(rule, seed);
            handles.push(tokio::spawn(async move { gate.check(&a).await.unwrap() }));
        }

        let mut sent = 0;
        for h in handles {
            if h.await.unwrap() {
                sent += 1;
            }
        }
        assert_eq!(sent, 1);
    }
}
