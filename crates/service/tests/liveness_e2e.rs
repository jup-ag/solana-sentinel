//! End to end: a program on a scripted source, the health program's `feed_liveness` and
//! `canary` rules, the real tap / gates / batching / watchdog, in-memory sinks. The source
//! first feeds traffic, then starves one rule's input, then stalls; a sink then dies.
//! Paused tokio time: minutes of silence run instantly.

use solana_pubkey::Pubkey;
use solana_sentinel::raw::{RawAccount, RawAccountParser};
use solana_sentinel::{
    Alert, Deps, NoConfig, Program, ProgramSpec, RuleId, Severity, StreamCtx, StreamRule,
    StreamRules, Subscription,
};
use solana_sentinel_rules_registry::health;
use solana_sentinel_service::testing::{rules_from_toml, FakeUpdate, Harness};
use std::time::Duration;
use tokio::time::{sleep, Instant};

const PROGRAM: Program = Program::new("fake");
const WATCHED: Pubkey = Pubkey::new_from_array([7; 32]);
const OTHER: Pubkey = Pubkey::new_from_array([8; 32]);
const OWNER: Pubkey = Pubkey::new_from_array([9; 32]);

/// Watches one account; `0xFF` in its data is the "exploit".
struct Watch;

impl StreamRule for Watch {
    const ID: RuleId = RuleId::new(PROGRAM, "watch");
    type Config = NoConfig;
    type Event = RawAccount;

    fn subscription(_: &NoConfig) -> Subscription {
        Subscription::new().accounts([WATCHED, OTHER])
    }

    fn build(_: NoConfig, _: &Deps) -> anyhow::Result<Self> {
        Ok(Self)
    }

    fn evaluate(&self, ev: &RawAccount, ctx: &StreamCtx) -> Option<Alert> {
        (ev.pubkey == WATCHED && ev.data == [0xFF]).then(|| {
            Alert::for_rule(Self::ID)
                .severity(Severity::Critical)
                .message("exploit")
                .meta("action", "alert")
                .key([ctx.slot])
                .build()
        })
    }
}

/// Same input, but its own subscription is only the watched account.
struct WatchedOnly;

impl StreamRule for WatchedOnly {
    const ID: RuleId = RuleId::new(PROGRAM, "watched_only");
    type Config = NoConfig;
    type Event = RawAccount;

    fn subscription(_: &NoConfig) -> Subscription {
        Subscription::new().accounts([WATCHED])
    }

    fn build(_: NoConfig, _: &Deps) -> anyhow::Result<Self> {
        Ok(Self)
    }

    fn evaluate(&self, _: &RawAccount, _: &StreamCtx) -> Option<Alert> {
        None
    }
}

fn fake_spec() -> ProgramSpec {
    ProgramSpec::new(PROGRAM).account(
        RawAccountParser,
        StreamRules::new()
            .register::<Watch>()
            .register::<WatchedOnly>(),
    )
}

const RULES: &str = r#"
[[rules]]
program = "fake"
name = "watch"
sinks = ["chat", "store"]

[[rules]]
program = "fake"
name = "watched_only"
sinks = ["chat", "store"]

[[rules]]
program = "health"
name = "feed_liveness"
sinks = ["pager", "store"]
cooldown_period_seconds = 600
[rules.config]
check_interval_seconds = 15
min_silence_seconds = 60
min_samples = 30
# The harness's only pipeline: losing it is losing everything.
critical_feeds = ["fake:account:*"]

[[rules]]
program = "health"
name = "canary"
sinks = ["pager"]
cooldown_period_seconds = 600
[rules.config]
interval_seconds = 60
deadline_seconds = 30
scope = "route"
"#;

fn checks(alerts: &[Alert], check: &str) -> Vec<Alert> {
    alerts
        .iter()
        .filter(|a| a.metadata.get("check").is_some_and(|c| c == check))
        .cloned()
        .collect()
}

#[tokio::test(start_paused = true)]
async fn blind_monitor_is_detected_end_to_end() {
    let mut h = Harness::start(rules_from_toml(RULES).unwrap(), &[fake_spec, health::spec])
        .await
        .unwrap();
    let (chat, store, pager) = (h.sink("chat"), h.sink("store"), h.sink("pager"));

    // 1. Traffic on both accounts, one "exploit" among it.
    let mut slot = 1;
    for i in 0..120 {
        let key = if i % 2 == 0 { WATCHED } else { OTHER };
        let data = if i == 40 { vec![0xFF] } else { vec![0] };
        h.send(FakeUpdate::account(key, OWNER, data, slot)).await;
        slot += 1;
        sleep(Duration::from_secs(1)).await;
    }
    let exploit: Vec<_> = chat
        .incidents()
        .into_iter()
        .filter(|a| a.message == "exploit")
        .collect();
    assert_eq!(exploit.len(), 1, "real alerts still flow");
    assert!(pager.incidents().is_empty(), "healthy: nothing paged");
    for sink in [&chat, &store, &pager] {
        assert!(!sink.canaries().is_empty(), "every sink gets canaries");
    }
    // Canaries go only to their route's sinks, never to the canary rule's own route.
    assert!(chat
        .canaries()
        .iter()
        .all(|c| c.metadata["canary_route"] == "sinks:chat+store"));
    assert!(h.liveness.canary().healthy(Instant::now()));

    // 2. Only OTHER updates: the pipeline stays busy, `watched_only`'s input goes silent.
    for _ in 0..80 {
        h.send(FakeUpdate::account(OTHER, OWNER, vec![0], slot))
            .await;
        slot += 1;
        sleep(Duration::from_secs(1)).await;
    }
    let silent = checks(&pager.incidents(), "feed_silent");
    let feeds: Vec<&str> = silent.iter().map(|a| a.metadata["feed"].as_str()).collect();
    assert_eq!(
        feeds,
        ["fake:rule:watched_only"],
        "one rule's input, not the pipeline"
    );
    assert_eq!(
        silent[0].severity,
        Severity::Warn,
        "one quiet rule input is a Warn"
    );

    // 3. The source stalls: the pipeline goes silent; one alert, Critical (a listed feed),
    //    action = alert.
    h.stall();
    sleep(Duration::from_secs(90)).await;
    let silent = checks(&pager.incidents(), "feed_silent");
    let pipeline: Vec<_> = silent
        .iter()
        .filter(|a| a.metadata["feed"] == "fake:account:raw_account")
        .collect();
    assert_eq!(pipeline.len(), 1, "deduped by the cooldown: {silent:#?}");
    assert_eq!(pipeline[0].severity, Severity::Critical);
    assert_eq!(pipeline[0].metadata["action"], "alert");
    assert_eq!(pipeline[0].metadata["bound_source"], "learned");
    // `watch` is silent with its pipeline: not alerted on its own.
    assert!(!silent
        .iter()
        .any(|a| a.metadata["feed"] == "fake:rule:watch"));
    // The health program's own in-process loops are alive and not reported.
    assert!(!silent
        .iter()
        .any(|a| a.metadata["feed"].starts_with("health:")));
    assert!(store
        .incidents()
        .iter()
        .any(|a| a.metadata.get("check").is_some_and(|c| c == "feed_silent")));

    // 4. The chat sink dies: its canaries stop arriving; the watchdog pages, the heartbeat
    //    gate closes; the other sinks keep accepting theirs.
    chat.set_failing(true);
    sleep(Duration::from_secs(120)).await;
    let missed = checks(&pager.incidents(), "canary_missed");
    assert_eq!(missed.len(), 1, "{missed:#?}");
    assert_eq!(missed[0].metadata["sink"], "chat");
    assert_eq!(missed[0].metadata["route"], "sinks:chat+store");
    assert_eq!(missed[0].severity, Severity::Critical);
    assert!(!h.liveness.canary().healthy(Instant::now()));
    assert!(h
        .liveness
        .canary()
        .check(Instant::now())
        .iter()
        .all(|m| m.sink.as_str() == "chat"));

    // 5. It recovers: the next canary reaches it and the gate opens again.
    chat.set_failing(false);
    sleep(Duration::from_secs(70)).await;
    assert!(h.liveness.canary().healthy(Instant::now()));

    h.shutdown().await;
}
