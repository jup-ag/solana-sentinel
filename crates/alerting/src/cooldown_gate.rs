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
pub struct CooldownGate {
    periods: HashMap<RuleId, Duration>,
    backend: Backend,
}

impl CooldownGate {
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
                key_prefix: format!("{pod_id}:{namespace}:alerts:cooldown_gate_key"),
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
                let key = format!("{key_prefix}:{}", alert.cooldown_gate_key());
                set_nx_exat(conn.as_mut(), &key, ttl_timestamp).await
            }
            Backend::Memory(map) => Ok(memory_set_nx(
                &mut map.lock(),
                alert.cooldown_gate_key().to_string(),
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
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use testcontainers::runners::AsyncRunner;
    use testcontainers::ImageExt;
    use testcontainers_modules::redis::Redis;

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

    // Monotonic counter for generating distinct seeds across the test binary.
    static SEQ: AtomicUsize = AtomicUsize::new(0);

    fn next() -> usize {
        SEQ.fetch_add(1, Ordering::Relaxed)
    }

    async fn redis_conn() -> (ConnectionManager, testcontainers::ContainerAsync<Redis>) {
        // EXAT requires Redis >= 6.2; the module's default image tag is older.
        let container = Redis::default().with_tag("7-alpine").start().await.unwrap();
        let port = container.get_host_port_ipv4(6379).await.unwrap();
        let client = redis::Client::open(format!("redis://127.0.0.1:{port}")).unwrap();
        let conn = ConnectionManager::new(client).await.unwrap();
        (conn, container)
    }

    // Same seed => identical value. Two `build_alert(rule, seed)` calls with the
    // same rule and seed produce alerts with the same cooldown key, i.e. genuine
    // duplicates. Different seeds produce distinct conditions.

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

    fn random_alert(rule: RuleId) -> Alert {
        build_alert(rule, next())
    }

    fn periods_with(rule: RuleId, period: Duration) -> HashMap<RuleId, Duration> {
        let mut m = HashMap::new();
        m.insert(rule, period);
        m
    }

    fn test_pod_id() -> &'static str {
        "TEST_NODE"
    }

    #[tokio::test]
    #[ignore = "needs Docker (testcontainers); run with --ignored"]
    async fn first_alert_is_sent() {
        let (conn, _c) = redis_conn().await;
        let rule = random_rule();
        let mut gate = CooldownGate::new(
            conn,
            periods_with(rule, Duration::from_secs(60)),
            test_pod_id(),
            "test",
        );
        let alert = random_alert(rule);

        assert!(
            gate.check(&alert).await.unwrap(),
            "first sighting should send"
        );
    }

    #[tokio::test]
    #[ignore = "needs Docker (testcontainers); run with --ignored"]
    async fn repeat_within_cooldown_is_suppressed() {
        let (conn, _c) = redis_conn().await;
        let rule = random_rule();
        let mut gate = CooldownGate::new(
            conn,
            periods_with(rule, Duration::from_secs(60)),
            test_pod_id(),
            "test",
        );

        // Two separately-built alerts from the same seed => genuine duplicates
        // (same cooldown key), not just the same object checked twice.
        let seed = next();
        let alert = build_alert(rule, seed);
        let duplicate = build_alert(rule, seed);

        assert!(gate.check(&alert).await.unwrap(), "first should send");
        assert!(
            !gate.check(&duplicate).await.unwrap(),
            "duplicate within cooldown should be suppressed"
        );
    }

    #[tokio::test]
    #[ignore = "needs Docker (testcontainers); run with --ignored"]
    async fn alert_sent_again_after_cooldown_expires() {
        let (conn, _c) = redis_conn().await;
        let rule = random_rule();
        let mut gate = CooldownGate::new(
            conn,
            periods_with(rule, Duration::from_secs(1)),
            test_pod_id(),
            "test",
        );

        let seed = next();
        let alert = build_alert(rule, seed);
        let duplicate = build_alert(rule, seed);

        assert!(gate.check(&alert).await.unwrap(), "first should send");
        assert!(
            !gate.check(&duplicate).await.unwrap(),
            "duplicate suppressed"
        );

        tokio::time::sleep(Duration::from_millis(1_200)).await;

        assert!(
            gate.check(&duplicate).await.unwrap(),
            "after cooldown expiry the same condition should send again"
        );
    }

    #[tokio::test]
    #[ignore = "needs Docker (testcontainers); run with --ignored"]
    async fn rule_without_period_is_never_suppressed() {
        let (conn, _c) = redis_conn().await;
        let rule = random_rule();
        let mut gate = CooldownGate::new(conn, HashMap::new(), test_pod_id(), "test");

        let seed = next();
        let alert = build_alert(rule, seed);
        let duplicate = build_alert(rule, seed);

        assert!(gate.check(&alert).await.unwrap(), "no period => send");
        assert!(
            gate.check(&duplicate).await.unwrap(),
            "no period => even an exact duplicate is never suppressed"
        );
    }

    #[tokio::test]
    #[ignore = "needs Docker (testcontainers); run with --ignored"]
    async fn different_conditions_are_independent() {
        let (conn, _c) = redis_conn().await;
        let rule = random_rule();
        let mut gate = CooldownGate::new(
            conn,
            periods_with(rule, Duration::from_secs(60)),
            test_pod_id(),
            "test",
        );

        // Same rule, different seeds => different cooldown keys.
        let alert_a = random_alert(rule);
        let alert_b = random_alert(rule);

        assert!(gate.check(&alert_a).await.unwrap(), "A first send");
        assert!(!gate.check(&alert_a).await.unwrap(), "A repeat suppressed");
        assert!(
            gate.check(&alert_b).await.unwrap(),
            "B is a different condition and must send despite A being in cooldown"
        );
    }

    #[tokio::test]
    #[ignore = "needs Docker (testcontainers); run with --ignored"]
    async fn mixed_stream_sends_each_condition_once() {
        let (conn, _c) = redis_conn().await;
        let rule = random_rule();
        let mut gate = CooldownGate::new(
            conn,
            periods_with(rule, Duration::from_secs(60)),
            test_pod_id(),
            "test",
        );

        // 8 distinct conditions, each with a fixed seed so we can rebuild exact
        // duplicates. Then append duplicates of the even-indexed ones, rebuilt
        // from the same seeds, and rotate so dups don't sit next to originals.
        let seeds: Vec<usize> = (0..8).map(|_| next()).collect();
        let distinct: Vec<Alert> = seeds.iter().map(|&s| build_alert(rule, s)).collect();
        let dups: Vec<Alert> = seeds
            .iter()
            .enumerate()
            .filter(|(i, _)| i % 2 == 0)
            .map(|(_, &s)| build_alert(rule, s)) // rebuilt duplicate, same key
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

        assert_eq!(
            sent,
            distinct.len(),
            "each distinct condition sends once; rebuilt duplicates are suppressed"
        );
    }

    #[tokio::test]
    #[ignore = "needs Docker (testcontainers); run with --ignored"]
    async fn concurrent_duplicate_checks_send_exactly_once() {
        let (conn, _c) = redis_conn().await;
        let rule = random_rule();
        let gate = CooldownGate::new(
            conn,
            periods_with(rule, Duration::from_secs(60)),
            test_pod_id(),
            "test",
        );

        // Each task rebuilds its OWN alert from the same seed — separate objects,
        // identical cooldown key — a realistic fleet race on one condition.
        let seed = next();

        let mut handles = Vec::new();
        for _ in 0..16 {
            let mut g = gate.clone();
            let a = build_alert(rule, seed);
            handles.push(tokio::spawn(async move { g.check(&a).await.unwrap() }));
        }

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
}
