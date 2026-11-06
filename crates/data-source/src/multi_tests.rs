use super::*;
use shipstern_core::Prefilter;
use std::sync::atomic::{AtomicUsize, Ordering};
use yellowstone_grpc_proto::geyser::{
    SubscribeUpdateAccount, SubscribeUpdateAccountInfo, SubscribeUpdateSlot,
    SubscribeUpdateTransaction, SubscribeUpdateTransactionInfo,
};

const A: usize = 0;
const B: usize = 1;

fn tx(sig: u8, slot: u64, filters: &[&str]) -> SubscribeUpdate {
    SubscribeUpdate {
        filters: filters.iter().map(|f| (*f).to_string()).collect(),
        update_oneof: Some(UpdateOneof::Transaction(SubscribeUpdateTransaction {
            transaction: Some(SubscribeUpdateTransactionInfo {
                signature: vec![sig; 64],
                ..Default::default()
            }),
            slot,
        })),
        created_at: None,
    }
}

fn account(key: u8, slot: u64, data: &[u8], write_version: u64) -> SubscribeUpdate {
    SubscribeUpdate {
        filters: vec!["acc".into()],
        update_oneof: Some(UpdateOneof::Account(SubscribeUpdateAccount {
            account: Some(SubscribeUpdateAccountInfo {
                pubkey: vec![key; 32],
                lamports: 1,
                owner: vec![9; 32],
                data: data.to_vec(),
                write_version,
                ..Default::default()
            }),
            slot,
            is_startup: false,
        })),
        created_at: None,
    }
}

fn slot(slot: u64, status: i32) -> SubscribeUpdate {
    SubscribeUpdate {
        filters: vec!["slots".into()],
        update_oneof: Some(UpdateOneof::Slot(SubscribeUpdateSlot {
            slot,
            parent: None,
            status,
            dead_error: None,
        })),
        created_at: None,
    }
}

fn settings() -> MergeSettings {
    MergeSettings {
        grace: Duration::from_secs(10),
        retention: Duration::from_secs(60),
        max_entries: 10_000,
    }
}

fn merger(n: usize) -> (Merger, SourceHealth, Instant) {
    let health = SourceHealth::new();
    let names: Vec<String> = (0..n).map(|i| format!("c{i}")).collect();
    health.register(&names);
    let mut m = Merger::new(n, settings(), health.clone());
    let now = Instant::now();
    for i in 0..n {
        m.connected(i, false, now);
    }
    (m, health, now)
}

fn secs(s: u64) -> Duration {
    Duration::from_secs(s)
}

fn snap(h: &SourceHealth, now: Instant) -> Vec<observability::source_health::ChildSnapshot> {
    h.snapshot(now, secs(3600))
}

#[test]
fn duplicate_transaction_is_forwarded_once_from_the_first_child() {
    let (mut m, h, t0) = merger(2);
    assert!(m.on_update(A, tx(1, 100, &["f"]), t0).is_some());
    assert!(m
        .on_update(B, tx(1, 100, &["f"]), t0 + Duration::from_millis(300))
        .is_none());
    let s = snap(&h, t0 + secs(1));
    assert_eq!((s[A].totals.delivered, s[A].totals.first), (1, 1));
    assert_eq!((s[B].totals.delivered, s[B].totals.first), (1, 0));
    assert_eq!(s[B].delay_p50, Some(Duration::from_millis(300)));
}

#[test]
fn out_of_order_deliveries_dedup_per_item() {
    let (mut m, h, t0) = merger(2);
    let mut out = Vec::new();
    for (child, sig) in [(A, 1), (B, 2), (A, 2), (B, 1), (A, 3), (B, 3)] {
        out.extend(m.on_update(child, tx(sig, 100, &["f"]), t0));
    }
    assert_eq!(out.len(), 3);
    m.sweep(t0 + secs(11));
    let s = snap(&h, t0 + secs(11));
    assert_eq!(s[A].totals.first, 2);
    assert_eq!(s[B].totals.first, 1);
    assert_eq!(s[A].totals.missing + s[B].totals.missing, 0);
    assert_eq!(s[A].totals.expected, 3);
}

#[test]
fn a_later_copy_forwards_only_filter_names_not_yet_sent() {
    let (mut m, _, t0) = merger(2);
    m.on_update(A, tx(1, 100, &["f1"]), t0).unwrap();
    let extra = m.on_update(B, tx(1, 100, &["f1", "f2"]), t0).unwrap();
    assert_eq!(extra.filters, vec!["f2".to_string()]);
    assert!(m.on_update(B, tx(1, 100, &["f2"]), t0).is_none());
}

#[test]
fn account_updates_dedup_on_data_not_write_version() {
    let (mut m, _, t0) = merger(2);
    // gRPC reports a write version; PubSub has none (0). Same state, same slot: one event.
    assert!(m.on_update(A, account(7, 100, b"x", 42), t0).is_some());
    assert!(m.on_update(B, account(7, 100, b"x", 0), t0).is_none());
    // An intermediate write in the same slot is its own event.
    assert!(m.on_update(A, account(7, 100, b"y", 43), t0).is_some());
    // Next slot, same data: a new event too.
    assert!(m.on_update(B, account(7, 101, b"y", 0), t0).is_some());
}

#[test]
fn one_child_missing_items_is_counted_after_the_grace_window() {
    let (mut m, h, t0) = merger(2);
    for sig in 0..100u8 {
        m.on_update(A, tx(sig, 100, &["f"]), t0);
        if sig % 10 != 0 {
            m.on_update(B, tx(sig, 100, &["f"]), t0);
        }
    }
    // Inside the grace window nothing is missing yet.
    m.sweep(t0 + secs(5));
    assert_eq!(snap(&h, t0 + secs(5))[B].totals.missing, 0);

    m.sweep(t0 + secs(11));
    let s = snap(&h, t0 + secs(11));
    assert_eq!((s[B].totals.expected, s[B].totals.missing), (100, 10));
    assert_eq!(s[B].totals.missing_pct(), Some(10.0));
    assert_eq!((s[A].totals.expected, s[A].totals.missing), (90, 0));
}

#[test]
fn delivery_after_the_grace_window_is_late_but_still_deduped() {
    let (mut m, h, t0) = merger(2);
    m.on_update(B, slot(100, 0), t0);
    m.on_update(A, tx(1, 100, &["f"]), t0);
    m.sweep(t0 + secs(11));
    assert!(m.on_update(B, tx(1, 100, &["f"]), t0 + secs(12)).is_none());
    let s = snap(&h, t0 + secs(12));
    assert_eq!((s[B].totals.missing, s[B].totals.late), (1, 1));
}

#[test]
fn silent_child_is_idle_and_never_blamed_for_missing_items() {
    let (mut m, h, t0) = merger(2);
    for i in 0..50 {
        m.on_update(A, slot(100 + i, 0), t0 + secs(i));
        m.on_update(
            A,
            tx(u8::try_from(i).unwrap(), 100 + i, &["f"]),
            t0 + secs(i),
        );
    }
    m.sweep(t0 + secs(70));
    let s = snap(&h, t0 + secs(70));
    assert!(s[B].idle_for >= secs(70));
    assert!(s[A].idle_for < secs(30));
    assert_eq!(
        s[B].totals.expected, 0,
        "silence is its own alert, not divergence"
    );
    assert_eq!(s[B].max_slot, 0);
    assert_eq!(s[B].slot_lag, 149);
}

#[test]
fn items_before_a_childs_first_slot_are_not_expected_from_it() {
    // B (re)connected at slot 200; A replays slots 150.. from a checkpoint.
    let (mut m, h, t0) = merger(2);
    m.on_update(B, slot(200, 0), t0);
    m.on_update(A, tx(1, 150, &["f"]), t0);
    m.on_update(A, tx(2, 200, &["f"]), t0);
    m.sweep(t0 + secs(11));
    let s = snap(&h, t0 + secs(11));
    assert_eq!((s[B].totals.expected, s[B].totals.missing), (1, 1));
}

#[test]
fn disconnected_child_is_not_expected_to_deliver() {
    let (mut m, h, t0) = merger(2);
    m.on_update(B, slot(100, 0), t0);
    m.disconnected(B, "stream failed");
    m.on_update(A, tx(1, 100, &["f"]), t0);
    m.sweep(t0 + secs(11));
    let s = snap(&h, t0 + secs(11));
    assert_eq!(s[B].totals.expected, 0);
    assert!(!s[B].up);
    assert_eq!(s[B].last_error.as_deref(), Some("stream failed"));
}

#[test]
fn slots_dedup_by_slot_and_status() {
    let (mut m, _, t0) = merger(2);
    assert!(m.on_update(A, slot(100, 0), t0).is_some());
    assert!(m.on_update(B, slot(100, 0), t0).is_none());
    assert!(m.on_update(B, slot(100, 1), t0).is_some());
}

#[test]
fn keys_are_forgotten_after_retention() {
    let (mut m, _, t0) = merger(2);
    m.on_update(A, tx(1, 100, &["f"]), t0).unwrap();
    m.sweep(t0 + secs(61));
    assert!(m.on_update(B, tx(1, 100, &["f"]), t0 + secs(61)).is_some());
}

#[test]
fn max_entries_bounds_memory() {
    let health = SourceHealth::new();
    health.register(&["a".into(), "b".into()]);
    let mut m = Merger::new(
        2,
        MergeSettings {
            max_entries: 10,
            ..settings()
        },
        health,
    );
    let t0 = Instant::now();
    for sig in 0..100u8 {
        m.on_update(A, tx(sig, 100, &["f"]), t0);
    }
    assert!(m.dedup.len() <= 10 && m.covers.len() <= 10);
}

#[test]
fn config_parses_and_validates() {
    let toml = r#"
        grace_seconds = 20
        [[sources]]
        name = "triton"
        type = "yellowstone_grpc"
        endpoint = "https://grpc.example"
        timeout = 60
        [[sources]]
        name = "other-ws"
        type = "websocket"
        url = "wss://ws.example"
        rpc_url = "https://rpc.example"
        commitment = "confirmed"
    "#;
    let cfg: MultiConfig = config_from_toml(toml);
    assert_eq!(cfg.sources.len(), 2);
    assert_eq!(cfg.grace_seconds, 20);
    assert_eq!(cfg.retention_seconds, 300);
    assert!(matches!(
        cfg.sources[0].source,
        ChildSourceConfig::YellowstoneGrpc(_)
    ));
    cfg.validate().unwrap();

    let mut dup = cfg.clone();
    dup.sources[1].name = "triton".into();
    assert!(dup.validate().is_err());
    let mut empty = cfg;
    empty.sources.clear();
    assert!(empty.validate().is_err());
}

fn config_from_toml(s: &str) -> MultiConfig {
    config::Config::builder()
        .add_source(config::File::from_str(s, config::FileFormat::Toml))
        .build()
        .and_then(config::Config::try_deserialize)
        .unwrap()
}

// ---- the async source, with fake children ----

#[derive(Debug, Clone, Copy)]
enum Then {
    Pend,
    Fail,
    Panic,
}

#[derive(Debug)]
struct Fake {
    script: Vec<(Duration, SubscribeUpdate)>,
    then: Then,
    connects: Arc<AtomicUsize>,
    filter_sets: Arc<parking_lot::Mutex<Vec<usize>>>,
}

impl Fake {
    fn new(script: Vec<(Duration, SubscribeUpdate)>, then: Then) -> Self {
        Self {
            script,
            then,
            connects: Arc::default(),
            filter_sets: Arc::default(),
        }
    }
}

#[async_trait]
impl SourceTrait for Fake {
    async fn connect(&self, ctx: SourceContext) -> Result<SourceExitStatus, shipstern::Error> {
        let SourceContext {
            filters,
            tx,
            mut filter_updates,
            ..
        } = ctx;
        let n = self.connects.fetch_add(1, Ordering::SeqCst);
        self.filter_sets.lock().push(filters.parsers_filters.len());
        // Only the first connect plays the script, so a restart does not replay it.
        if n == 0 {
            for (delay, update) in &self.script {
                tokio::time::sleep(*delay).await;
                if tx.send(Ok(update.clone())).await.is_err() {
                    return Ok(SourceExitStatus::ReceiverDropped);
                }
            }
        }
        match self.then {
            Then::Fail if n == 0 => Err(shipstern::Error::Other("boom".into())),
            Then::Panic if n == 0 => panic!("fake child panicked"),
            _ => loop {
                if filter_updates.changed().await.is_err() {
                    std::future::pending::<()>().await;
                }
                let f = filter_updates.borrow_and_update().parsers_filters.len();
                self.filter_sets.lock().push(f);
            },
        }
    }
}

fn filters(n: usize) -> Filters {
    Filters::new(
        (0..n)
            .map(|i| {
                (
                    format!("p{i}"),
                    Prefilter::builder().slots().build().unwrap(),
                )
            })
            .collect(),
    )
}

async fn collect(rx: &mut mpsc::Receiver<Item>, n: usize) -> Vec<SubscribeUpdate> {
    let mut out = Vec::new();
    while out.len() < n {
        let item = tokio::time::timeout(secs(5), rx.recv())
            .await
            .expect("timed out waiting for updates")
            .expect("source ended");
        out.push(item.expect("multi source never forwards errors"));
    }
    out
}

fn ms(n: u64) -> Duration {
    Duration::from_millis(n)
}

#[tokio::test]
async fn merged_stream_dedups_across_children() {
    let a = Fake::new(
        vec![(ms(0), tx(1, 10, &["f"])), (ms(0), tx(2, 10, &["f"]))],
        Then::Pend,
    );
    let b = Fake::new(
        vec![
            (ms(20), tx(2, 10, &["f"])),
            (ms(0), tx(1, 10, &["f"])),
            (ms(0), tx(3, 10, &["f"])),
        ],
        Then::Pend,
    );
    let health = SourceHealth::new();
    let source = MultiSource::from_children(
        vec![("a".into(), Arc::new(a)), ("b".into(), Arc::new(b))],
        MergeSettings::default(),
        health.clone(),
    );
    let (tx, mut rx) = mpsc::channel(64);
    let run = tokio::spawn(async move { source.connect(SourceContext::new(filters(1), tx)).await });
    let got = collect(&mut rx, 3).await;
    let mut sigs: Vec<u8> = got
        .iter()
        .map(|u| match &u.update_oneof {
            Some(UpdateOneof::Transaction(t)) => t.transaction.as_ref().unwrap().signature[0],
            _ => unreachable!(),
        })
        .collect();
    sigs.sort_unstable();
    assert_eq!(sigs, vec![1, 2, 3]);
    // Nothing else comes through.
    assert!(tokio::time::timeout(ms(200), rx.recv()).await.is_err());
    let s = health.snapshot(Instant::now(), secs(60));
    assert_eq!((s[0].totals.first, s[1].totals.first), (2, 1));
    drop(rx);
    run.abort();
}

#[tokio::test]
async fn a_failing_or_panicking_child_is_restarted_and_does_not_stop_the_others() {
    for then in [Then::Fail, Then::Panic] {
        let bad = Fake::new(vec![(ms(0), tx(1, 10, &["f"]))], then);
        let bad_connects = Arc::clone(&bad.connects);
        let good = Fake::new(
            vec![(ms(100), tx(2, 10, &["f"])), (ms(100), tx(3, 10, &["f"]))],
            Then::Pend,
        );
        let health = SourceHealth::new();
        let source = MultiSource::from_children(
            vec![
                ("bad".into(), Arc::new(bad)),
                ("good".into(), Arc::new(good)),
            ],
            MergeSettings::default(),
            health.clone(),
        );
        let (tx, mut rx) = mpsc::channel(64);
        let run =
            tokio::spawn(async move { source.connect(SourceContext::new(filters(1), tx)).await });
        let got = collect(&mut rx, 3).await;
        assert_eq!(got.len(), 3, "{then:?}");
        // Restart after the 1s backoff.
        tokio::time::timeout(secs(5), async {
            while bad_connects.load(Ordering::SeqCst) < 2 {
                tokio::time::sleep(ms(50)).await;
            }
        })
        .await
        .expect("bad child restarted");
        let s = health.snapshot(Instant::now(), secs(60));
        assert_eq!(s[0].restarts, 1, "{then:?}");
        assert!(s[0].last_error.is_some());
        assert!(s[1].up);
        assert!(!run.is_finished(), "multi source keeps running ({then:?})");
        run.abort();
    }
}

#[tokio::test]
async fn filter_updates_reach_every_child() {
    let a = Fake::new(Vec::new(), Then::Pend);
    let b = Fake::new(Vec::new(), Then::Pend);
    let (sa, sb) = (Arc::clone(&a.filter_sets), Arc::clone(&b.filter_sets));
    let source = MultiSource::from_children(
        vec![("a".into(), Arc::new(a)), ("b".into(), Arc::new(b))],
        MergeSettings::default(),
        SourceHealth::new(),
    );
    let (tx, _rx) = mpsc::channel(64);
    let (updates_tx, updates_rx) = watch::channel(filters(1));
    let mut ctx = SourceContext::new(filters(1), tx);
    ctx.filter_updates = updates_rx;
    let run = tokio::spawn(async move { source.connect(ctx).await });
    tokio::time::sleep(ms(100)).await;
    updates_tx.send_replace(filters(3));
    tokio::time::timeout(secs(5), async {
        loop {
            if sa.lock().as_slice() == [1, 3] && sb.lock().as_slice() == [1, 3] {
                break;
            }
            tokio::time::sleep(ms(20)).await;
        }
    })
    .await
    .expect("both children got the initial and the updated filters");
    run.abort();
}

#[tokio::test]
async fn invalid_config_fails_connect() {
    let source = MultiSource::from_config(MultiConfig {
        sources: Vec::new(),
        grace_seconds: 30,
        retention_seconds: 300,
        max_entries: 10,
        health: SourceHealth::new(),
    });
    let (tx, _rx) = mpsc::channel(1);
    assert!(source
        .connect(SourceContext::new(filters(1), tx))
        .await
        .is_err());
}
