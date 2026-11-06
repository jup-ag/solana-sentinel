use super::accounts::TOKEN_PROGRAM;
use super::format::{
    write_record, AccountRecord, BlockRecord, Recording, ReplayHeader, ReplayRecord,
    TransactionRecord, FORMAT_VERSION,
};
use super::*;
use base64::Engine;
use shipstern_core::{Filters, Prefilter};
use solana_account_decoder_client_types::{UiAccount, UiAccountData, UiAccountEncoding};
use solana_message::{Message, VersionedMessage};
use solana_pubkey::Pubkey;
use solana_transaction::versioned::VersionedTransaction;
use solana_transaction_status_client_types::option_serializer::OptionSerializer;
use solana_transaction_status_client_types::{
    EncodedConfirmedTransactionWithStatusMeta, EncodedTransaction,
    EncodedTransactionWithStatusMeta, TransactionBinaryEncoding, UiTransactionStatusMeta,
    UiTransactionTokenBalance,
};
use std::collections::HashMap;
use tokio::sync::mpsc;

const B64: base64::engine::general_purpose::GeneralPurpose =
    base64::engine::general_purpose::STANDARD;

fn token_balance(
    index: u8,
    mint: &Pubkey,
    owner: &Pubkey,
    amount: u64,
) -> UiTransactionTokenBalance {
    serde_json::from_value(serde_json::json!({
        "accountIndex": index,
        "mint": mint.to_string(),
        "owner": owner.to_string(),
        "programId": TOKEN_PROGRAM.to_string(),
        "uiTokenAmount": {"amount": amount.to_string(), "decimals": 6, "uiAmount": null, "uiAmountString": "0"},
    }))
    .unwrap()
}

/// A recorded transfer: payer (signer) moves tokens out of `vault` (writable), `program` readonly.
fn recorded_tx(
    slot: u64,
    payer: &Pubkey,
    vault: &Pubkey,
    program: &Pubkey,
    mint: &Pubkey,
    pre: u64,
    post: u64,
) -> (String, EncodedConfirmedTransactionWithStatusMeta) {
    let ix = solana_message::compiled_instruction::CompiledInstruction {
        program_id_index: 2,
        accounts: vec![1],
        data: vec![1, 2, 3],
    };
    let msg = Message::new_with_compiled_instructions(
        1,
        0,
        1,
        vec![*payer, *vault, *program],
        Default::default(),
        vec![ix],
    );
    let sig = solana_signature::Signature::from([slot as u8; 64]);
    let tx = VersionedTransaction {
        signatures: vec![sig],
        message: VersionedMessage::Legacy(msg),
    };
    let bytes = wincode::serialize(&tx).unwrap();
    let meta: UiTransactionStatusMeta = serde_json::from_value(serde_json::json!({
        "err": null, "status": {"Ok": null}, "fee": 5000,
        "preBalances": [10_000_000, 2_039_280, 1], "postBalances": [9_995_000, 2_039_280, 1],
        "innerInstructions": [], "logMessages": [],
        "preTokenBalances": [], "postTokenBalances": [], "rewards": [],
    }))
    .unwrap();
    let mut meta = meta;
    meta.pre_token_balances = OptionSerializer::Some(vec![token_balance(1, mint, payer, pre)]);
    meta.post_token_balances = OptionSerializer::Some(vec![token_balance(1, mint, payer, post)]);
    (
        sig.to_string(),
        EncodedConfirmedTransactionWithStatusMeta {
            slot,
            transaction: EncodedTransactionWithStatusMeta {
                transaction: EncodedTransaction::Binary(
                    B64.encode(bytes),
                    TransactionBinaryEncoding::Base64,
                ),
                meta: Some(meta),
                version: None,
            },
            block_time: Some(1_700_000_000 + (slot as i64 - 100) * 2 / 5),
        },
    )
}

fn recording_file(vault: &Pubkey, program: &Pubkey) -> Vec<u8> {
    let payer = Pubkey::new_unique();
    let mint = Pubkey::new_unique();
    let mut out = Vec::new();
    write_record(
        &mut out,
        &ReplayRecord::Header(ReplayHeader {
            version: FORMAT_VERSION,
            name: "test".into(),
            first_slot: 100,
            last_slot: 104,
            ..Default::default()
        }),
    )
    .unwrap();
    // Written out of order: the loader sorts.
    for (slot, pre, post) in [(103, 600, 100), (101, 1_000, 600)] {
        let (signature, transaction) = recorded_tx(slot, &payer, vault, program, &mint, pre, post);
        write_record(
            &mut out,
            &ReplayRecord::Transaction(Box::new(TransactionRecord {
                slot,
                index: None,
                signature,
                transaction,
            })),
        )
        .unwrap();
    }
    write_record(
        &mut out,
        &ReplayRecord::Block(BlockRecord {
            slot: 104,
            block_time: Some(1_700_000_002),
        }),
    )
    .unwrap();
    write_record(
        &mut out,
        &ReplayRecord::Account(AccountRecord {
            pubkey: payer.to_string(),
            slot: 999,
            account: UiAccount {
                lamports: 1,
                data: UiAccountData::Binary(String::new(), UiAccountEncoding::Base64),
                owner: accounts::SYSTEM_PROGRAM.to_string(),
                executable: false,
                rent_epoch: 0,
                space: Some(0),
            },
        }),
    )
    .unwrap();
    out
}

fn cfg(speed: f64) -> ReplayConfig {
    ReplayConfig {
        path: PathBuf::new(),
        speed,
        timing: Timing::Nominal,
        slot_ms: 400.0,
        linger_seconds: 0,
        initial_state: true,
        reconstruct_accounts: true,
        emit_log: None,
    }
}

#[test]
fn schedule_orders_slots_transactions_and_rebuilt_accounts() {
    let vault = Pubkey::new_unique();
    let program = Pubkey::new_unique();
    let rec = Recording::read(recording_file(&vault, &program).as_slice()).unwrap();
    assert_eq!(rec.transactions[0].slot, 101, "sorted by slot");
    let s = Schedule::build(&rec, &cfg(1.0));
    assert_eq!(s.transactions, 2);
    assert_eq!(s.undecodable, 0);
    let kinds: Vec<(u64, &str)> = s
        .items
        .iter()
        .map(|i| {
            (
                i.slot,
                match i.item {
                    Item::Slot => "slot",
                    Item::Transaction(_) => "tx",
                    Item::Account => "acct",
                },
            )
        })
        .collect();
    assert_eq!(
        kinds,
        vec![
            (100, "slot"),
            (100, "acct"), // payer pre-state (system wallet snapshot)
            (100, "acct"), // vault pre-state (token balance, synthesised)
            (101, "slot"),
            (101, "tx"),
            (101, "acct"),
            (101, "acct"),
            (102, "slot"),
            (103, "slot"),
            (103, "tx"),
            (103, "acct"),
            (103, "acct"),
            (104, "slot"),
        ]
    );
    let Some(UpdateOneof::Account(a)) = &s.items[2].update.update_oneof else {
        panic!("vault pre-state")
    };
    let a = a.account.as_ref().unwrap();
    assert_eq!(a.pubkey, vault.to_bytes().to_vec());
    assert_eq!(a.data[64..72], 1_000u64.to_le_bytes());
    let Some(UpdateOneof::Account(a)) = &s.items[11].update.update_oneof else {
        panic!("vault post-state")
    };
    assert_eq!(
        a.account.as_ref().unwrap().data[64..72],
        100u64.to_le_bytes()
    );
    assert_eq!(s.offset(104, 1.0), Some(Duration::from_millis(1600)));
    assert_eq!(s.offset(104, 4.0), Some(Duration::from_millis(400)));
    assert_eq!(s.offset(104, 0.0), None);
}

#[test]
fn block_time_pacing_needs_a_long_enough_window() {
    let rec =
        Recording::read(recording_file(&Pubkey::new_unique(), &Pubkey::new_unique()).as_slice())
            .unwrap();
    let mut c = cfg(1.0);
    c.timing = Timing::BlockTime;
    c.slot_ms = 450.0;
    assert!(
        (Schedule::build(&rec, &c).slot_ms - 450.0).abs() < f64::EPSILON,
        "short window: fallback"
    );
}

#[tokio::test(start_paused = true)]
async fn source_paces_matches_and_logs() {
    let vault = Pubkey::new_unique();
    let program = Pubkey::new_unique();
    let dir = std::env::temp_dir().join(format!("replay-test-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("r.jsonl");
    let log = dir.join("emit.jsonl");
    let _ = std::fs::remove_file(&log);
    std::fs::write(&path, recording_file(&vault, &program)).unwrap();

    let mut filters = HashMap::new();
    filters.insert(
        "prog:tx".to_string(),
        Prefilter::builder()
            .transaction_accounts_include([shipstern_core::KeyBytes(program.to_bytes())])
            .build()
            .unwrap(),
    );
    filters.insert(
        "vault:acct".to_string(),
        Prefilter::builder()
            .account_owners([shipstern_core::KeyBytes(TOKEN_PROGRAM.to_bytes())])
            .build()
            .unwrap(),
    );
    let (tx, mut rx) = mpsc::channel(64);
    let mut c = cfg(1.0);
    c.path = path;
    c.emit_log = Some(log.clone());
    let source = ReplaySource::from_config(c);
    let started = Instant::now();
    let status = source
        .connect(SourceContext::new(Filters::new(filters), tx))
        .await
        .unwrap();
    assert!(matches!(status, SourceExitStatus::Completed));
    assert_eq!(
        started.elapsed(),
        Duration::from_millis(1600),
        "4 slots at 400 ms"
    );
    let mut got = Vec::new();
    while let Ok(Ok(u)) = rx.try_recv() {
        got.push(u);
    }
    // 2 transactions + vault pre-state + 2 vault post-states; no slot filter, no wallet filter.
    assert_eq!(got.len(), 5, "{got:?}");
    assert!(got.iter().all(|u| u.filters.len() == 1));
    let lines = std::fs::read_to_string(&log).unwrap();
    let events: Vec<serde_json::Value> = lines
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    assert_eq!(events.first().unwrap()["event"], "start");
    assert_eq!(events.last().unwrap()["event"], "end");
    let txs: Vec<_> = events.iter().filter(|e| e["event"] == "tx").collect();
    assert_eq!(txs.len(), 2);
    assert_eq!(txs[0]["filters"][0], "prog:tx");
    assert_eq!(events.iter().filter(|e| e["event"] == "slot").count(), 5);
    let _ = std::fs::remove_dir_all(&dir);
}
