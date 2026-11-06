//! Recorder: fetch an incident's slot window over RPC into a replay file.
//!
//! Transactions come from one of two scans:
//! - `signatures` (default): `getSignaturesForAddress` per address, paged back from the end
//!   of the window, then `getTransaction` each. Cheap; some providers drop part of the
//!   signature history, which the header notes when an exploit signature is missing from it.
//! - `blocks`: `getBlock` (`transactionDetails: accounts`) for every slot of the window, kept
//!   when any listed address is among the keys. Complete and in block order, ~4 MB a block.
//!
//! The exploit signatures are always fetched. Block order inside a slot comes from
//! `getBlock` (`signatures`) for slots with more than one recorded transaction. Account
//! snapshots (today's state: the RPC has no historical one) are read for every account in
//! the transactions' token balances, every writable key whose lamports moved, and any
//! `--account`, as templates for the replay's account rebuild.
//!
//! RPC errors go through [`utility::redact::redact_urls`]; the URL itself is never printed.

use crate::replay::format::{
    write_record, AccountRecord, BlockRecord, ReplayHeader, ReplayRecord, TransactionRecord,
    FORMAT_VERSION,
};
use anyhow::{anyhow, bail, Context};
use futures::stream::{self, StreamExt, TryStreamExt};
use serde_json::{json, Value};
use solana_account_decoder_client_types::UiAccount;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_rpc_client_api::request::RpcRequest;
use solana_transaction_status_client_types::EncodedConfirmedTransactionWithStatusMeta;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};
use utility::redact::redact_urls;

/// Endpoints the recorder refuses: public, rate-limited, and not meant for bulk history.
const REFUSED_HOSTS: [&str; 4] = [
    "api.mainnet-beta.solana.com",
    "api.mainnet.solana.com",
    "lite-api.jup.ag",
    "api.devnet.solana.com",
];
const SIGNATURE_PAGE: usize = 1000;
const ACCOUNTS_PER_CALL: usize = 100;
const RETRIES: usize = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum Scan {
    Signatures,
    Blocks,
}

#[derive(Debug, Clone, clap::Parser)]
#[command(
    name = "sentinel-replay-record",
    about = "Record an incident's slot window into a replay file. RPC: $SENTINEL_RECORD_RPC_URL, \
             else json_rpc_url of the Solana CLI config (never printed)."
)]
pub struct RecordArgs {
    /// Output replay file (JSON lines).
    #[arg(long)]
    pub out: PathBuf,
    #[arg(long)]
    pub name: String,
    #[arg(long, default_value = "")]
    pub description: String,
    /// Exploit transactions, first one first: the window is placed around the first.
    #[arg(long = "exploit-signature", required = true)]
    pub exploit_signatures: Vec<String>,
    /// Addresses (programs, vaults, wallets) whose transactions in the window are recorded.
    #[arg(long = "address")]
    pub addresses: Vec<String>,
    /// Extra accounts to snapshot.
    #[arg(long = "account")]
    pub accounts: Vec<String>,
    /// Slots recorded before the first exploit transaction's slot.
    #[arg(long, default_value_t = 150)]
    pub before_slots: u64,
    /// Slots recorded after it.
    #[arg(long, default_value_t = 750)]
    pub after_slots: u64,
    /// Absolute window instead of before/after (both needed).
    #[arg(long, requires = "to_slot")]
    pub from_slot: Option<u64>,
    #[arg(long, requires = "from_slot")]
    pub to_slot: Option<u64>,
    #[arg(long, value_enum, default_value_t = Scan::Signatures)]
    pub scan: Scan,
    /// Per-address cap on recorded transactions (signatures scan), newest dropped first.
    #[arg(long, default_value_t = 20_000)]
    pub max_per_address: usize,
    /// Also record failed transactions.
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    pub include_failed: bool,
    /// Cap on account snapshots.
    #[arg(long, default_value_t = 20_000)]
    pub max_snapshots: usize,
    /// RPC requests in flight.
    #[arg(long, default_value_t = 32)]
    pub concurrency: usize,
}

/// The RPC URL: `$SENTINEL_RECORD_RPC_URL`, else the Solana CLI config's `json_rpc_url`.
pub fn rpc_url() -> anyhow::Result<String> {
    let url = match std::env::var("SENTINEL_RECORD_RPC_URL") {
        Ok(u) if !u.is_empty() => u,
        _ => {
            let home = std::env::var("HOME").context("HOME is not set")?;
            let path = PathBuf::from(home).join(".config/solana/cli/config.yml");
            let text = std::fs::read_to_string(&path)
                .with_context(|| format!("cannot read {}", path.display()))?;
            cli_config_url(&text).context("no json_rpc_url in the Solana CLI config")?
        }
    };
    if let Some(host) = REFUSED_HOSTS.iter().find(|h| url.contains(**h)) {
        bail!("refusing to record from the public endpoint {host}; configure a private RPC");
    }
    Ok(url)
}

fn cli_config_url(yaml: &str) -> Option<String> {
    yaml.lines().find_map(|l| {
        let v = l.strip_prefix("json_rpc_url:")?.trim();
        let v = v.trim_matches(|c| c == '"' || c == '\'');
        (!v.is_empty()).then(|| v.to_string())
    })
}

struct Rpc {
    client: RpcClient,
}

impl Rpc {
    async fn call(&self, method: &'static str, params: Value) -> anyhow::Result<Value> {
        let mut delay = Duration::from_millis(250);
        let mut last = String::new();
        for _ in 0..RETRIES {
            match self
                .client
                .send::<Value>(RpcRequest::Custom { method }, params.clone())
                .await
            {
                Ok(v) => return Ok(v),
                Err(e) => {
                    last = redact_urls(&e.to_string());
                    // Skipped / missing slots are answers, not transient failures.
                    if last.contains("was skipped") || last.contains("not available") {
                        break;
                    }
                    tokio::time::sleep(delay).await;
                    delay *= 2;
                }
            }
        }
        Err(anyhow!("{method}: {last}"))
    }
}

/// Record the window. Returns the header written.
pub async fn record(args: RecordArgs) -> anyhow::Result<ReplayHeader> {
    let url = rpc_url()?;
    let rpc = Arc::new(Rpc {
        client: RpcClient::new_with_timeout(url, Duration::from_secs(120)),
    });
    let conc = args.concurrency.max(1);
    let mut notes = Vec::new();

    let first = fetch_tx(&rpc, &args.exploit_signatures[0])
        .await?
        .ok_or_else(|| anyhow!("first exploit transaction not found"))?;
    let (from, to) = match (args.from_slot, args.to_slot) {
        (Some(f), Some(t)) => (f, t),
        _ => (
            first.slot.saturating_sub(args.before_slots),
            first.slot + args.after_slots,
        ),
    };
    anyhow::ensure!(from <= to, "empty window {from}..{to}");
    info!(name = %args.name, anchor = first.slot, from, to, scan = ?args.scan, "Recording window");

    // (slot, index if known) per signature.
    let mut found: HashMap<String, (u64, Option<u32>)> = HashMap::new();
    let mut block_times: BTreeMap<u64, Option<i64>> = BTreeMap::new();
    match args.scan {
        Scan::Signatures => {
            for address in &args.addresses {
                let sigs = address_signatures(&rpc, address, from, to, args.include_failed).await?;
                let total = sigs.len();
                let mut sigs = sigs;
                // Keep the oldest: the window's start and the exploit matter more than its tail.
                sigs.sort_by_key(|(_, slot)| *slot);
                if total > args.max_per_address {
                    sigs.truncate(args.max_per_address);
                    let note = format!(
                        "{address}: {total} transactions in the window, capped at {}",
                        args.max_per_address
                    );
                    warn!("{note}");
                    notes.push(note);
                }
                info!(%address, transactions = sigs.len(), "Address scanned");
                for (sig, slot) in sigs {
                    found.entry(sig).or_insert((slot, None));
                }
            }
            for sig in &args.exploit_signatures {
                if !found.contains_key(sig) && !args.addresses.is_empty() {
                    notes.push(format!(
                        "exploit signature {sig} missing from the address scan (provider history \
                         gap or not touching the listed addresses); fetched directly"
                    ));
                }
            }
        }
        Scan::Blocks => {
            let wanted: HashSet<&str> = args.addresses.iter().map(String::as_str).collect();
            let results: Vec<(u64, Option<Value>)> = stream::iter(from..=to)
                .map(|slot| {
                    let rpc = Arc::clone(&rpc);
                    async move {
                        let block = rpc
                            .call(
                                "getBlock",
                                json!([slot, {"transactionDetails": "accounts", "rewards": false,
                                    "maxSupportedTransactionVersion": 0, "encoding": "json",
                                    "commitment": "confirmed"}]),
                            )
                            .await;
                        (slot, block.ok())
                    }
                })
                .buffer_unordered(conc)
                .collect()
                .await;
            for (slot, block) in results {
                let Some(block) = block else { continue };
                block_times.insert(slot, block["blockTime"].as_i64());
                for (i, tx) in block["transactions"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .enumerate()
                {
                    if !args.include_failed && !tx["meta"]["err"].is_null() {
                        continue;
                    }
                    let hit = tx["transaction"]["accountKeys"]
                        .as_array()
                        .into_iter()
                        .flatten()
                        .any(|k| k["pubkey"].as_str().is_some_and(|p| wanted.contains(p)));
                    if hit {
                        if let Some(sig) = tx["transaction"]["signatures"][0].as_str() {
                            found.insert(sig.to_string(), (slot, Some(i as u32)));
                        }
                    }
                }
            }
        }
    }
    for sig in &args.exploit_signatures {
        found.entry(sig.clone()).or_insert((0, None));
    }

    // Transactions.
    let sigs: Vec<String> = found.keys().cloned().collect();
    info!(transactions = sigs.len(), "Fetching transactions");
    let fetched: Vec<(String, Option<EncodedConfirmedTransactionWithStatusMeta>)> =
        stream::iter(sigs)
            .map(|sig| {
                let rpc = Arc::clone(&rpc);
                async move { fetch_tx(&rpc, &sig).await.map(|t| (sig, t)) }
            })
            .buffer_unordered(conc)
            .try_collect()
            .await?;
    let mut txs = Vec::new();
    for (sig, tx) in fetched {
        match tx {
            Some(tx) if (from..=to).contains(&tx.slot) => {
                let index = found.get(&sig).and_then(|(_, i)| *i);
                block_times.entry(tx.slot).or_insert(tx.block_time);
                txs.push(TransactionRecord {
                    slot: tx.slot,
                    index,
                    signature: sig,
                    transaction: tx,
                });
            }
            Some(tx) => notes.push(format!(
                "{sig} is at slot {}, outside the window; left out",
                tx.slot
            )),
            None => notes.push(format!("{sig}: getTransaction returned nothing")),
        }
    }

    // Block order inside slots holding more than one recorded transaction.
    let mut per_slot: BTreeMap<u64, usize> = BTreeMap::new();
    for t in txs.iter().filter(|t| t.index.is_none()) {
        *per_slot.entry(t.slot).or_default() += 1;
    }
    let multi: Vec<u64> = per_slot
        .into_iter()
        .filter(|(_, n)| *n > 1)
        .map(|(s, _)| s)
        .collect();
    let orders: Vec<(u64, Option<Value>)> = stream::iter(multi)
        .map(|slot| {
            let rpc = Arc::clone(&rpc);
            async move {
                let b = rpc
                    .call(
                        "getBlock",
                        json!([slot, {"transactionDetails": "signatures", "rewards": false,
                            "maxSupportedTransactionVersion": 0, "commitment": "confirmed"}]),
                    )
                    .await;
                (slot, b.ok())
            }
        })
        .buffer_unordered(conc)
        .collect()
        .await;
    let mut position: HashMap<String, u32> = HashMap::new();
    for (_, block) in orders {
        for (i, s) in block
            .as_ref()
            .and_then(|b| b["signatures"].as_array())
            .into_iter()
            .flatten()
            .enumerate()
        {
            if let Some(s) = s.as_str() {
                position.insert(s.to_string(), i as u32);
            }
        }
    }
    for t in &mut txs {
        if t.index.is_none() {
            t.index = position.get(&t.signature).copied();
        }
    }
    // Window ends, for block-time pacing.
    for slot in [from, to] {
        if let std::collections::btree_map::Entry::Vacant(e) = block_times.entry(slot) {
            e.insert(
                rpc.call("getBlockTime", json!([slot]))
                    .await
                    .ok()
                    .and_then(|v| v.as_i64()),
            );
        }
    }

    // Snapshots.
    let mut wanted: BTreeSet<String> = args.accounts.iter().cloned().collect();
    for t in &txs {
        wanted.extend(snapshot_candidates(&t.transaction));
    }
    let total_wanted = wanted.len();
    let wanted: Vec<String> = wanted.into_iter().take(args.max_snapshots).collect();
    if total_wanted > wanted.len() {
        notes.push(format!(
            "{total_wanted} accounts to snapshot, capped at {}",
            args.max_snapshots
        ));
    }
    info!(accounts = wanted.len(), "Fetching account snapshots");
    let chunks: Vec<Vec<String>> = wanted
        .chunks(ACCOUNTS_PER_CALL)
        .map(<[String]>::to_vec)
        .collect();
    let snaps: Vec<Vec<AccountRecord>> = stream::iter(chunks)
        .map(|chunk| {
            let rpc = Arc::clone(&rpc);
            async move {
                let v = rpc
                    .call(
                        "getMultipleAccounts",
                        json!([chunk, {"encoding": "base64", "commitment": "confirmed"}]),
                    )
                    .await?;
                let slot = v["context"]["slot"].as_u64().unwrap_or_default();
                let mut out = Vec::new();
                for (key, acc) in chunk
                    .iter()
                    .zip(v["value"].as_array().into_iter().flatten())
                {
                    if acc.is_null() {
                        continue;
                    }
                    let account: UiAccount = serde_json::from_value(acc.clone())?;
                    out.push(AccountRecord {
                        pubkey: key.clone(),
                        slot,
                        account,
                    });
                }
                anyhow::Ok(out)
            }
        })
        .buffer_unordered(conc)
        .try_collect()
        .await?;

    let header = ReplayHeader {
        version: FORMAT_VERSION,
        name: args.name.clone(),
        description: args.description.clone(),
        first_slot: from,
        last_slot: to,
        exploit_signatures: args.exploit_signatures.clone(),
        addresses: args.addresses.clone(),
        recorded_at_unix: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or_default(),
        notes,
    };
    txs.sort_by_key(|t| (t.slot, t.index.unwrap_or(u32::MAX)));
    if let Some(dir) = args.out.parent().filter(|d| !d.as_os_str().is_empty()) {
        std::fs::create_dir_all(dir)?;
    }
    let file = std::fs::File::create(&args.out)
        .with_context(|| format!("cannot create {}", args.out.display()))?;
    let mut out = std::io::BufWriter::new(file);
    write_record(&mut out, &ReplayRecord::Header(header.clone()))?;
    for (slot, block_time) in &block_times {
        write_record(
            &mut out,
            &ReplayRecord::Block(BlockRecord {
                slot: *slot,
                block_time: *block_time,
            }),
        )?;
    }
    let tx_count = txs.len();
    for t in txs {
        write_record(&mut out, &ReplayRecord::Transaction(Box::new(t)))?;
    }
    let mut snap_count = 0;
    for a in snaps.into_iter().flatten() {
        snap_count += 1;
        write_record(&mut out, &ReplayRecord::Account(a))?;
    }
    out.flush()?;
    info!(
        out = %args.out.display(),
        transactions = tx_count,
        snapshots = snap_count,
        notes = header.notes.len(),
        "Replay file written"
    );
    Ok(header)
}

async fn fetch_tx(
    rpc: &Rpc,
    sig: &str,
) -> anyhow::Result<Option<EncodedConfirmedTransactionWithStatusMeta>> {
    let v = rpc
        .call(
            "getTransaction",
            json!([sig, {"encoding": "base64", "maxSupportedTransactionVersion": 0,
                "commitment": "confirmed"}]),
        )
        .await?;
    if v.is_null() {
        return Ok(None);
    }
    Ok(Some(serde_json::from_value(v).with_context(|| {
        format!("{sig}: unexpected getTransaction shape")
    })?))
}

/// Signatures touching `address` with slot in `from..=to`, paged back from the window's end.
async fn address_signatures(
    rpc: &Rpc,
    address: &str,
    from: u64,
    to: u64,
    include_failed: bool,
) -> anyhow::Result<Vec<(String, u64)>> {
    let mut before = end_anchor(rpc, to).await?;
    let mut out = Vec::new();
    loop {
        let mut opts = json!({"limit": SIGNATURE_PAGE, "commitment": "confirmed"});
        if let Some(b) = &before {
            opts["before"] = json!(b);
        }
        let page = rpc
            .call("getSignaturesForAddress", json!([address, opts]))
            .await?;
        let entries = page.as_array().cloned().unwrap_or_default();
        if entries.is_empty() {
            break;
        }
        let mut oldest = u64::MAX;
        for e in &entries {
            let slot = e["slot"].as_u64().unwrap_or_default();
            oldest = oldest.min(slot);
            let failed = !e["err"].is_null();
            if (from..=to).contains(&slot) && (include_failed || !failed) {
                if let Some(s) = e["signature"].as_str() {
                    out.push((s.to_string(), slot));
                }
            }
        }
        before = entries
            .last()
            .and_then(|e| e["signature"].as_str())
            .map(str::to_string);
        if oldest < from || entries.len() < SIGNATURE_PAGE {
            break;
        }
    }
    Ok(out)
}

/// A signature from the first non-empty block after `to`, to page back from. `None` (page
/// from the newest) when the window ends at the tip.
async fn end_anchor(rpc: &Rpc, to: u64) -> anyhow::Result<Option<String>> {
    for slot in to + 1..to + 64 {
        let Ok(b) = rpc
            .call(
                "getBlock",
                json!([slot, {"transactionDetails": "signatures", "rewards": false,
                    "maxSupportedTransactionVersion": 0, "commitment": "confirmed"}]),
            )
            .await
        else {
            continue;
        };
        if let Some(sig) = b["signatures"][0].as_str() {
            return Ok(Some(sig.to_string()));
        }
    }
    Ok(None)
}

/// Accounts worth a snapshot: token-balance accounts and writable keys whose lamports moved.
fn snapshot_candidates(tx: &EncodedConfirmedTransactionWithStatusMeta) -> Vec<String> {
    use solana_transaction_status_client_types::option_serializer::OptionSerializer;
    let Some(meta) = tx.transaction.meta.as_ref() else {
        return Vec::new();
    };
    let Some(decoded) = tx.transaction.transaction.decode() else {
        return Vec::new();
    };
    let mut keys: Vec<String> = decoded
        .message
        .static_account_keys()
        .iter()
        .map(ToString::to_string)
        .collect();
    let writable_loaded = match &meta.loaded_addresses {
        OptionSerializer::Some(l) => {
            keys.extend(l.writable.iter().cloned());
            let n = keys.len();
            keys.extend(l.readonly.iter().cloned());
            n
        }
        _ => keys.len(),
    };
    let mut out = Vec::new();
    for balances in [&meta.pre_token_balances, &meta.post_token_balances] {
        if let OptionSerializer::Some(list) = balances {
            for b in list {
                if let Some(k) = keys.get(b.account_index as usize) {
                    out.push(k.clone());
                }
            }
        }
    }
    for (i, k) in keys.iter().enumerate().take(writable_loaded) {
        if meta.pre_balances.get(i) != meta.post_balances.get(i)
            && decoded.message.is_maybe_writable(i, None)
        {
            out.push(k.clone());
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_the_cli_config_url() {
        let yaml = "---\njson_rpc_url: \"https://rpc.example/abc\"\nwebsocket_url: ''\n";
        assert_eq!(
            cli_config_url(yaml).as_deref(),
            Some("https://rpc.example/abc")
        );
        assert_eq!(cli_config_url("websocket_url: x\n"), None);
    }
}
