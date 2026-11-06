//! The replay file: JSON lines, one record each, tagged by `type`.
//!
//! ```text
//! {"type":"header","version":1,"name":"drift-2026-04-01","first_slot":…,"last_slot":…,…}
//! {"type":"block","slot":410344139,"block_time":1775059570}
//! {"type":"transaction","slot":410344139,"index":17,"signature":"5cvV…","transaction":{…getTransaction result, base64…}}
//! {"type":"account","pubkey":"GXWq…","slot":465900000,"account":{…getAccountInfo value, base64…}}
//! ```
//!
//! `transaction` is the raw `getTransaction` result (`encoding: base64`,
//! `maxSupportedTransactionVersion: 0`), so a file is exactly what the RPC said.
//! `account` records are snapshots taken when the file was recorded (the RPC has
//! no historical account state); the source uses them only as templates when it
//! rebuilds account post-states from transaction metadata (see `accounts`).
//! Records may come in any order; the source sorts transactions by slot and index.

use anyhow::Context;
use serde::{Deserialize, Serialize};
use solana_account_decoder_client_types::UiAccount;
use solana_pubkey::Pubkey;
use solana_transaction_status_client_types::EncodedConfirmedTransactionWithStatusMeta;
use std::collections::{BTreeMap, HashMap};
use std::io::{BufRead, Write};
use std::path::Path;
use std::str::FromStr;

pub const FORMAT_VERSION: u32 = 1;

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ReplayRecord {
    Header(ReplayHeader),
    Block(BlockRecord),
    Transaction(Box<TransactionRecord>),
    Account(AccountRecord),
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ReplayHeader {
    pub version: u32,
    pub name: String,
    #[serde(default)]
    pub description: String,
    /// Replay window, inclusive. Every slot in it is emitted, empty or not.
    pub first_slot: u64,
    pub last_slot: u64,
    /// The exploit's transactions, first one first; the drill measures from it.
    #[serde(default)]
    pub exploit_signatures: Vec<String>,
    /// Addresses whose transactions were recorded over the window.
    #[serde(default)]
    pub addresses: Vec<String>,
    #[serde(default)]
    pub recorded_at_unix: i64,
    /// Anything the recorder could not do (capped address, missing transaction, …).
    #[serde(default)]
    pub notes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockRecord {
    pub slot: u64,
    pub block_time: Option<i64>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TransactionRecord {
    pub slot: u64,
    /// Position in the block when the recorder could read it, else absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index: Option<u32>,
    pub signature: String,
    pub transaction: EncodedConfirmedTransactionWithStatusMeta,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountRecord {
    pub pubkey: String,
    /// Slot the snapshot was read at (recording time, not the incident's).
    pub slot: u64,
    pub account: UiAccount,
}

/// A replay file, loaded and sorted.
#[derive(Debug)]
pub struct Recording {
    pub header: ReplayHeader,
    pub block_times: BTreeMap<u64, i64>,
    /// Sorted by slot, then block index (unknown last), then file order.
    pub transactions: Vec<TransactionRecord>,
    pub snapshots: HashMap<Pubkey, UiAccount>,
}

impl Recording {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let file = std::fs::File::open(path)
            .with_context(|| format!("cannot open replay file {}", path.display()))?;
        Self::read(std::io::BufReader::new(file))
            .with_context(|| format!("bad replay file {}", path.display()))
    }

    pub fn read(reader: impl BufRead) -> anyhow::Result<Self> {
        let mut header = None;
        let mut block_times = BTreeMap::new();
        let mut transactions = Vec::new();
        let mut snapshots = HashMap::new();
        for (n, line) in reader.lines().enumerate() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            let record: ReplayRecord =
                serde_json::from_str(&line).with_context(|| format!("line {}", n + 1))?;
            match record {
                ReplayRecord::Header(h) => {
                    anyhow::ensure!(
                        h.version <= FORMAT_VERSION,
                        "replay format version {} is newer than {FORMAT_VERSION}",
                        h.version
                    );
                    header = Some(h);
                }
                ReplayRecord::Block(b) => {
                    if let Some(t) = b.block_time {
                        block_times.insert(b.slot, t);
                    }
                }
                ReplayRecord::Transaction(t) => {
                    if let Some(bt) = t.transaction.block_time {
                        block_times.entry(t.slot).or_insert(bt);
                    }
                    transactions.push((n, *t));
                }
                ReplayRecord::Account(a) => {
                    let key = Pubkey::from_str(&a.pubkey)
                        .with_context(|| format!("line {}: bad pubkey", n + 1))?;
                    snapshots.insert(key, a.account);
                }
            }
        }
        let header = header.context("no header record")?;
        anyhow::ensure!(
            header.first_slot <= header.last_slot,
            "first_slot {} is after last_slot {}",
            header.first_slot,
            header.last_slot
        );
        transactions.sort_by_key(|(line, t)| (t.slot, t.index.unwrap_or(u32::MAX), *line));
        Ok(Self {
            header,
            block_times,
            transactions: transactions.into_iter().map(|(_, t)| t).collect(),
            snapshots,
        })
    }
}

/// Write one record as a JSON line.
pub fn write_record(out: &mut impl Write, record: &ReplayRecord) -> anyhow::Result<()> {
    serde_json::to_writer(&mut *out, record)?;
    out.write_all(b"\n")?;
    Ok(())
}
