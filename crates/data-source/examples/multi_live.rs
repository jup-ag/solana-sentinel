//! Live check of the `multi` source: two (or more) children on real endpoints, the
//! same filters, for a few minutes; prints delivery share, delays and gaps.
//!
//! Children come from one of:
//! - `--config <file>`: a `MultiConfig` TOML (`[[sources]]` tables, same keys as the
//!   service's `[[data_source.sources]]` but with the data-source crate's field names:
//!   `x-token`, `rpc_url`, ...). Keep it git-ignored: it holds endpoints and tokens.
//! - default: the Solana CLI's configured RPC (`~/.config/solana/cli/config.yml`), split
//!   Triton/rpcpool-style into a Yellowstone gRPC child (endpoint = scheme + host,
//!   x-token = the path token) and a PubSub child on the same RPC. Same provider twice:
//!   it tests the code, not provider independence.
//!
//! Endpoints and tokens are never printed. Run:
//!   cargo run -p solana-sentinel-data-source --example multi_live -- --minutes 10

use anyhow::{bail, Context};
use async_trait::async_trait;
use observability::source_health::SourceHealth;
use shipstern::sources::{FromConfig, SourceContext, SourceExitStatus, SourceTrait};
use shipstern_core::{Filters, Prefilter};
use solana_pubkey::Pubkey;
use solana_sentinel_data_source::multi::MergeSettings;
use solana_sentinel_data_source::rpc::build_rpc_client;
use solana_sentinel_data_source::{
    ChildConfig, ChildSourceConfig, MultiConfig, MultiSource, WebsocketConfig,
    YellowstoneGrpcConfig,
};
use solana_signature::Signature;
use std::collections::{BTreeMap, HashMap};
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof;
use yellowstone_grpc_proto::geyser::SubscribeUpdate;

/// Jup Lend: liquidity, lending, vaults.
const DEFAULT_TX_ACCOUNTS: &[&str] = &[
    "jupeiUmn818Jg1ekPURTpr4mFo29p46vygyykFJ3wZC",
    "jup3YeL8QhtSx1e253b2FDvsMNC87fDrgQZivbrndc9",
    "jupr81YtYssSyPt8jbnGuiWon5f6x9TcDEFxYe3Bdzi",
];
/// Items first seen this soon after the first delivery are left out of the gap report.
const WARMUP: Duration = Duration::from_secs(20);
/// Accounts owned by Jup Lend vaults.
const DEFAULT_OWNERS: &[&str] = &["jupr81YtYssSyPt8jbnGuiWon5f6x9TcDEFxYe3Bdzi"];

struct Args {
    minutes: u64,
    config: Option<String>,
    tx_accounts: Vec<String>,
    owners: Vec<String>,
    commitment: String,
    grace_seconds: u64,
}

fn parse_args() -> anyhow::Result<Args> {
    let mut a = Args {
        minutes: 10,
        config: None,
        tx_accounts: Vec::new(),
        owners: Vec::new(),
        commitment: "confirmed".into(),
        grace_seconds: 30,
    };
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        let mut val = || it.next().with_context(|| format!("{flag} needs a value"));
        match flag.as_str() {
            "--minutes" => a.minutes = val()?.parse()?,
            "--config" => a.config = Some(val()?),
            "--tx" => a.tx_accounts.push(val()?),
            "--owner" => a.owners.push(val()?),
            "--commitment" => a.commitment = val()?,
            "--grace" => a.grace_seconds = val()?.parse()?,
            other => bail!("unknown flag {other}"),
        }
    }
    if a.tx_accounts.is_empty() && a.owners.is_empty() {
        a.tx_accounts = DEFAULT_TX_ACCOUNTS
            .iter()
            .map(|s| (*s).to_string())
            .collect();
        a.owners = DEFAULT_OWNERS.iter().map(|s| (*s).to_string()).collect();
    }
    Ok(a)
}

/// RPC URL and optional websocket URL from the Solana CLI config.
fn cli_rpc() -> anyhow::Result<(String, Option<String>)> {
    let home = std::env::var("HOME").context("HOME unset")?;
    let path = format!("{home}/.config/solana/cli/config.yml");
    let text = std::fs::read_to_string(&path).with_context(|| format!("reading {path}"))?;
    let field = |name: &str| {
        text.lines()
            .find_map(|l| l.trim().strip_prefix(&format!("{name}:")))
            .map(|v| v.trim().trim_matches(['"', '\'']).to_string())
            .filter(|v| !v.is_empty())
    };
    let rpc = field("json_rpc_url").context("no json_rpc_url in the Solana CLI config")?;
    if rpc.contains("mainnet-beta.solana.com") || rpc.contains("api.devnet") {
        bail!("the Solana CLI points at a public RPC; refusing (configure a private one)");
    }
    Ok((rpc, field("websocket_url")))
}

/// Triton/rpcpool layout: `https://host/<token>` serves JSON-RPC, PubSub (`wss://host/<token>`)
/// and Yellowstone gRPC (`https://host`, `x-token: <token>`).
fn children_from_cli(commitment: &str) -> anyhow::Result<Vec<ChildConfig>> {
    let (rpc, ws) = cli_rpc()?;
    let (scheme, rest) = rpc.split_once("://").context("RPC URL without scheme")?;
    let (host, path) = rest.split_once('/').unwrap_or((rest, ""));
    let token = path.trim_matches('/');
    if token.is_empty() || token.contains('/') {
        bail!("RPC URL path is not a single token; pass --config with explicit children");
    }
    let ws = ws.unwrap_or_else(|| {
        let ws_scheme = if scheme == "https" { "wss" } else { "ws" };
        format!("{ws_scheme}://{host}/{token}")
    });
    let grpc_commitment = match commitment {
        "processed" => shipstern::CommitmentLevel::Processed,
        "finalized" => shipstern::CommitmentLevel::Finalized,
        _ => shipstern::CommitmentLevel::Confirmed,
    };
    Ok(vec![
        ChildConfig {
            name: "grpc".into(),
            source: ChildSourceConfig::YellowstoneGrpc(YellowstoneGrpcConfig {
                endpoint: format!("{scheme}://{host}"),
                x_token: Some(token.to_string()),
                timeout: 60,
                commitment_level: Some(grpc_commitment),
                from_slot: None,
                accounts_data_slice: Vec::new(),
                max_decoding_message_size: Some(16 * 1024 * 1024),
                accept_compression: None,
                auto_reconnect: true,
                reconnect_max_retries: None,
                reconnect_slot_retention: None,
            }),
        },
        ChildConfig {
            name: "pubsub".into(),
            source: ChildSourceConfig::Websocket(WebsocketConfig {
                url: ws,
                rpc_url: rpc,
                rpc_headers: HashMap::new(),
                // PubSub cannot serve getTransaction at processed.
                commitment: if commitment == "finalized" {
                    "finalized"
                } else {
                    "confirmed"
                }
                .to_string(),
            }),
        },
    ])
}

fn filters(args: &Args) -> anyhow::Result<Filters> {
    let keys = |v: &[String]| -> anyhow::Result<Vec<Pubkey>> {
        v.iter()
            .map(|s| Pubkey::from_str(s).with_context(|| format!("bad pubkey {s}")))
            .collect()
    };
    let mut map = HashMap::new();
    map.insert("slots".to_string(), Prefilter::builder().slots().build()?);
    if !args.tx_accounts.is_empty() {
        map.insert(
            "txs".to_string(),
            Prefilter::builder()
                .transaction_accounts_include(keys(&args.tx_accounts)?)
                .build()?,
        );
    }
    if !args.owners.is_empty() {
        map.insert(
            "accounts".to_string(),
            Prefilter::builder()
                .account_owners(keys(&args.owners)?)
                .build()?,
        );
    }
    Ok(Filters::new(map))
}

/// Per child: when and at which slot it first delivered an item.
type Sightings = Vec<Option<(Instant, u64)>>;

/// One delivery as a child produced it, before the merge.
#[derive(Debug, Clone)]
struct Record {
    child: usize,
    at: Instant,
    kind: &'static str,
    key: Vec<u8>,
    slot: u64,
}

type Log = Arc<parking_lot::Mutex<Vec<Record>>>;

/// Wraps a child and logs every data item it emits, for the per-kind report.
#[derive(Debug)]
struct Tap {
    child: usize,
    inner: Arc<dyn SourceTrait>,
    log: Log,
}

#[async_trait]
impl SourceTrait for Tap {
    async fn connect(&self, ctx: SourceContext) -> Result<SourceExitStatus, shipstern::Error> {
        let (tx, mut rx) = mpsc::channel(4096);
        let mut inner_ctx = SourceContext::new(ctx.filters.clone(), tx);
        inner_ctx.filter_updates = ctx.filter_updates.clone();
        let inner = Arc::clone(&self.inner);
        let run = tokio::spawn(async move { inner.connect(inner_ctx).await });
        while let Some(item) = rx.recv().await {
            if let Ok(u) = &item {
                if let Some((kind, key, slot)) = identity(u) {
                    self.log.lock().push(Record {
                        child: self.child,
                        at: Instant::now(),
                        kind,
                        key,
                        slot,
                    });
                }
            }
            if ctx.tx.send(item).await.is_err() {
                run.abort();
                return Ok(SourceExitStatus::ReceiverDropped);
            }
        }
        run.await
            .map_err(|e| shipstern::Error::Other(format!("child task: {e}").into()))?
    }
}

fn identity(u: &SubscribeUpdate) -> Option<(&'static str, Vec<u8>, u64)> {
    match u.update_oneof.as_ref()? {
        UpdateOneof::Transaction(t) => {
            Some(("tx", t.transaction.as_ref()?.signature.clone(), t.slot))
        }
        UpdateOneof::Account(a) => {
            let mut key = a.account.as_ref()?.pubkey.clone();
            key.extend_from_slice(&a.slot.to_le_bytes());
            Some(("account", key, a.slot))
        }
        _ => None,
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let args = parse_args()?;
    let children = match &args.config {
        Some(path) => {
            let cfg: MultiConfig = config::Config::builder()
                .add_source(config::File::new(path, config::FileFormat::Toml))
                .build()
                .and_then(config::Config::try_deserialize)
                .with_context(|| format!("reading {path}"))?;
            cfg.sources
        }
        None => children_from_cli(&args.commitment)?,
    };
    let names: Vec<String> = children.iter().map(|c| c.name.clone()).collect();
    // getSignatureStatuses for missing transactions, on the first websocket child's RPC.
    let status_rpc = children.iter().find_map(|c| match &c.source {
        ChildSourceConfig::Websocket(w) => build_rpc_client(&w.rpc_url, &w.rpc_headers).ok(),
        _ => None,
    });

    let log: Log = Arc::default();
    let tapped: Vec<(String, Arc<dyn SourceTrait>)> = children
        .into_iter()
        .enumerate()
        .map(|(i, c)| {
            let inner: Arc<dyn SourceTrait> = match c.source {
                ChildSourceConfig::YellowstoneGrpc(y) => Arc::new(
                    shipstern_yellowstone_grpc_source::YellowstoneGrpcSource::from_config(y),
                ),
                ChildSourceConfig::Websocket(w) => {
                    Arc::new(solana_sentinel_data_source::WebsocketSource::from_config(w))
                }
                ChildSourceConfig::Kafka(k) => {
                    Arc::new(solana_sentinel_data_source::KafkaSource::from_config(k))
                }
            };
            let tap: Arc<dyn SourceTrait> = Arc::new(Tap {
                child: i,
                inner,
                log: Arc::clone(&log),
            });
            (c.name, tap)
        })
        .collect();

    let grace = Duration::from_secs(args.grace_seconds);
    let health = SourceHealth::new();
    let source = MultiSource::from_children(
        tapped,
        MergeSettings {
            grace,
            retention: grace * 10,
            ..MergeSettings::default()
        },
        health.clone(),
    );
    let (tx, mut rx) = mpsc::channel(10_000);
    let ctx = SourceContext::new(filters(&args)?, tx);
    println!(
        "multi_live: children {names:?}, gRPC commitment {} (PubSub confirmed), {} min, grace {}s, tx accounts {}, owners {}",
        args.commitment,
        args.minutes,
        args.grace_seconds,
        args.tx_accounts.len(),
        args.owners.len()
    );
    let run = tokio::spawn(async move { source.connect(ctx).await });

    let started = Instant::now();
    let deadline = started + Duration::from_secs(args.minutes * 60);
    let mut forwarded: BTreeMap<&'static str, u64> = BTreeMap::new();
    let mut tick = tokio::time::interval(Duration::from_secs(60));
    tick.tick().await;
    loop {
        tokio::select! {
            () = tokio::time::sleep_until(deadline.into()) => break,
            _ = tick.tick() => print_health(&health, started),
            item = rx.recv() => {
                let Some(item) = item else { break };
                let kind = match item.ok().and_then(|u| u.update_oneof) {
                    Some(UpdateOneof::Transaction(_)) => "tx",
                    Some(UpdateOneof::Account(_)) => "account",
                    Some(UpdateOneof::Slot(_)) => "slot",
                    _ => "other",
                };
                *forwarded.entry(kind).or_default() += 1;
            }
        }
    }
    // Let the last grace windows close before judging.
    println!(
        "stream stopped after {} min; waiting {}s for the last grace window",
        args.minutes, args.grace_seconds
    );
    let drain_until = Instant::now() + grace + Duration::from_secs(2);
    while Instant::now() < drain_until {
        tokio::select! {
            () = tokio::time::sleep_until(drain_until.into()) => {}
            item = rx.recv() => if item.is_none() { break },
        }
    }
    run.abort();

    println!("\n== forwarded to rules (deduplicated) ==\n{forwarded:?}");
    print_health(&health, started);
    let records = std::mem::take(&mut *log.lock());
    report(&records, &names, grace, status_rpc).await;
    Ok(())
}

fn print_health(health: &SourceHealth, started: Instant) {
    let window = started.elapsed() + Duration::from_secs(1);
    println!("\n== health at +{}s ==", started.elapsed().as_secs());
    for c in health.snapshot(Instant::now(), window) {
        let t = c.window;
        let share = |n: u64| {
            if t.delivered == 0 {
                0.0
            } else {
                n as f64 * 100.0 / t.delivered as f64
            }
        };
        println!(
            "{:>8}: up={} restarts={} idle={}s slot={} lag={} delivered={} first={} ({:.1}% of its own) \
             expected={} missing={} ({:.2}%) late={} delay p50={:?} p95={:?}",
            c.name,
            c.up,
            c.restarts,
            c.idle_for.as_secs(),
            c.max_slot,
            c.slot_lag,
            t.delivered,
            t.first,
            share(t.first),
            t.expected,
            t.missing,
            t.missing_pct().unwrap_or(0.0),
            t.late,
            c.delay_p50,
            c.delay_p95,
        );
    }
}

/// Per kind: who delivered what first, delays, and the items one child never delivered.
async fn report(
    log: &[Record],
    names: &[String],
    grace: Duration,
    rpc: Option<solana_rpc_client::nonblocking::rpc_client::RpcClient>,
) {
    let end = log.iter().map(|r| r.at).max();
    let Some(end) = end else {
        println!("no data items delivered");
        return;
    };
    let start = log.iter().map(|r| r.at).min().unwrap_or(end);
    let mut items: BTreeMap<(&'static str, Vec<u8>), Sightings> = BTreeMap::new();
    for r in log {
        let e = items
            .entry((r.kind, r.key.clone()))
            .or_insert_with(|| vec![None; names.len()]);
        if e[r.child].is_none() {
            e[r.child] = Some((r.at, r.slot));
        }
    }
    for kind in ["tx", "account"] {
        let mut total = 0u64;
        let mut first = vec![0u64; names.len()];
        let mut delivered = vec![0u64; names.len()];
        let mut only = vec![0u64; names.len()];
        let mut delays: Vec<Vec<u128>> = vec![Vec::new(); names.len()];
        let mut gaps: Vec<Vec<(u64, Vec<u8>)>> = vec![Vec::new(); names.len()];
        for ((k, key), seen) in &items {
            if *k != kind {
                continue;
            }
            let first_at = seen
                .iter()
                .flatten()
                .map(|(t, _)| *t)
                .min()
                .expect("seen once");
            // Too recent to judge, or from the warm-up while subscriptions were still opening.
            if end.saturating_duration_since(first_at) < grace
                || first_at.saturating_duration_since(start) < WARMUP
            {
                continue;
            }
            total += 1;
            let slot = seen.iter().flatten().map(|(_, s)| *s).max().unwrap_or(0);
            let count = seen.iter().flatten().count();
            for (i, s) in seen.iter().enumerate() {
                match s {
                    Some((t, _)) => {
                        delivered[i] += 1;
                        if *t == first_at {
                            first[i] += 1;
                        } else {
                            delays[i].push(t.saturating_duration_since(first_at).as_millis());
                        }
                        if count == 1 {
                            only[i] += 1;
                        }
                    }
                    None => gaps[i].push((slot, key.clone())),
                }
            }
        }
        println!(
            "\n== {kind}: {total} distinct items (judged after the {}s grace, {}s warm-up \
             excluded) ==",
            grace.as_secs(),
            WARMUP.as_secs()
        );
        for (i, name) in names.iter().enumerate() {
            let d = &mut delays[i];
            d.sort_unstable();
            let p = |q: usize| d.get(d.len().saturating_sub(1) * q / 100).copied();
            println!(
                "{name:>8}: delivered {} ({:.2}%), first {} ({:.1}% share), only-here {}, \
                 never delivered {} | delay behind first p50={}ms p95={}ms max={}ms",
                delivered[i],
                pct(delivered[i], total),
                first[i],
                pct(first[i], total),
                only[i],
                gaps[i].len(),
                fmt_ms(p(50)),
                fmt_ms(p(95)),
                fmt_ms(d.last().copied()),
            );
        }
        for (i, name) in names.iter().enumerate() {
            if gaps[i].is_empty() {
                continue;
            }
            let mut by_slot = gaps[i].clone();
            by_slot.sort();
            let shown: Vec<String> = by_slot
                .iter()
                .take(5)
                .map(|(slot, key)| {
                    if kind == "tx" {
                        format!("{} @{slot}", bs58::encode(key).into_string())
                    } else {
                        format!("{} @{slot}", bs58::encode(&key[..32]).into_string())
                    }
                })
                .collect();
            println!("  {name} never delivered (first 5 by slot): {shown:?}");
            if kind == "tx" {
                if let Some(rpc) = &rpc {
                    confirm_missing(rpc, name, &by_slot).await;
                }
            }
        }
    }
}

/// Were the transactions a child never delivered actually confirmed on chain?
async fn confirm_missing(
    rpc: &solana_rpc_client::nonblocking::rpc_client::RpcClient,
    name: &str,
    gaps: &[(u64, Vec<u8>)],
) {
    let sigs: Vec<Signature> = gaps
        .iter()
        .take(256)
        .filter_map(|(_, k)| Signature::try_from(k.as_slice()).ok())
        .collect();
    let mut confirmed = 0;
    let mut unknown = 0;
    for chunk in sigs.chunks(256) {
        match rpc.get_signature_statuses(chunk).await {
            Ok(r) => {
                for s in r.value {
                    match s {
                        Some(_) => confirmed += 1,
                        None => unknown += 1,
                    }
                }
            }
            Err(_) => {
                println!("  (getSignatureStatuses failed)");
                return;
            }
        }
    }
    println!(
        "  {name}: of {} missing transactions checked, {confirmed} have a status on chain (real \
         gaps), {unknown} have none (dropped forks / not landed)",
        sigs.len()
    );
}

fn pct(n: u64, d: u64) -> f64 {
    if d == 0 {
        0.0
    } else {
        n as f64 * 100.0 / d as f64
    }
}

fn fmt_ms(v: Option<u128>) -> String {
    v.map_or_else(|| "-".to_string(), |v| v.to_string())
}
