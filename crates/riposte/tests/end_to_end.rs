use async_trait::async_trait;
use dedup_gate::fleet_gate::FleetGate;
use domain::alert::{Alert, DedupScope};
use domain::counter_measure::CounterMeasure;
use domain::programs::Program;
use domain::rules::RuleId;
use serde_json::json;
use solana_hash::Hash;
use solana_instruction::{AccountMeta, Instruction};
use solana_keypair::Keypair;
use solana_message::VersionedMessage;
use solana_pubkey::Pubkey;
use solana_rpc_client::api::request::RpcRequest;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sentinel_riposte::blockhash::BlockhashCache;
use solana_sentinel_riposte::config::Mode;
use solana_sentinel_riposte::fees::{FeeConfig, PriorityFeeCache};
use solana_sentinel_riposte::halt::HaltSwitch;
use solana_sentinel_riposte::signer::KeypairSigner;
use solana_sentinel_riposte::traits::{Broadcaster, TxSigner};
use solana_sentinel_riposte::Riposte;
use solana_transaction::versioned::VersionedTransaction;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

const RULE: RuleId = RuleId::new(Program::new("vault"), "drain");
const PROGRAM: Pubkey = Pubkey::new_from_array([7; 32]);
const VAULT: Pubkey = Pubkey::new_from_array([9; 32]);

struct Capture(mpsc::Sender<VersionedTransaction>);

#[async_trait]
impl Broadcaster for Capture {
    fn id(&self) -> &str {
        "capture"
    }
    async fn send(&self, tx: &VersionedTransaction) -> anyhow::Result<()> {
        self.0.send(tx.clone()).await?;
        Ok(())
    }
}

fn ix(data: &[u8]) -> Instruction {
    Instruction::new_with_bytes(PROGRAM, data, vec![AccountMeta::new(VAULT, false)])
}

/// Live mode end to end: gate → compose → sign (the program's key) → broadcast → landing
/// (mock RPC reports it finalized in slot 1) → outcome alert on the rule.
#[tokio::test]
async fn counter_measure_is_signed_sent_once_per_condition_and_reported_landed() {
    let signer = KeypairSigner::new(Keypair::new());
    let payer = signer.pubkey();
    let blockhash = BlockhashCache::default();
    let hash = Hash::new_from_array([5; 32]);
    // The mock's block height is 1234; well inside this hash's validity.
    blockhash.set(hash, 10_000);
    let fees = PriorityFeeCache::new(FeeConfig {
        percentile_bps: 7_500,
        min_micro_lamports: 0,
        max_micro_lamports: 5_000,
    });
    fees.set(1_000);
    let (sent_tx, mut sent_rx) = mpsc::channel(256);

    let periods = HashMap::from([(RULE, Duration::from_secs(60))]);
    // Another program's signer must not be the one that signs this rule's countermeasure.
    let other: Arc<dyn TxSigner> = Arc::new(KeypairSigner::new(Keypair::new()));
    let (alerts_tx, mut alerts_rx) = mpsc::channel(8);
    let riposte = Riposte {
        mode: Mode::Live,
        signers: HashMap::from([
            (RULE.program, Arc::new(signer) as Arc<dyn TxSigner>),
            (Program::new("other"), other),
        ]),
        broadcasters: vec![Arc::new(Capture(sent_tx))],
        // One-shot tip for the replayed trigger below; later reads get the mock's default 0.
        rpc: Arc::new(RpcClient::new_mock_with_mocks(
            "succeeds".to_string(),
            HashMap::from([(RpcRequest::GetSlot, json!(10_000))]),
        )),
        blockhash,
        fees,
        allowed_programs: HashSet::from([PROGRAM]),
        gate: FleetGate::in_memory(periods).strict(),
        halt: HaltSwitch::default(),
        alerts: alerts_tx,
    };
    let (queue_tx, queue) = mpsc::channel(8);
    let tracker = TaskTracker::new();
    riposte.start(queue, CancellationToken::new(), &tracker);

    let cm = CounterMeasure {
        ixs: vec![ix(&[1, 2, 3]), ix(&[4])],
        compute_units: 60_000,
    };
    // A replayed trigger far behind the tip: reported, never sent.
    let replayed = Alert::for_rule(RULE).key(["vault-1"]).build();
    queue_tx
        .send((replayed, cm.clone(), Some(1)))
        .await
        .unwrap();
    let report = tokio::time::timeout(Duration::from_secs(5), alerts_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(report.metadata["riposte"], "failed");
    assert!(report.message.contains("behind the tip"));
    assert!(sent_rx.try_recv().is_err(), "replayed trigger sent");

    // Same condition seen in two txs (or replayed): the strict gate lets one through.
    for sig in ["tx-a", "tx-b"] {
        let alert = Alert::for_rule(RULE)
            .key(["vault-9"])
            .scope(DedupScope::Tx(sig.into()))
            .build();
        queue_tx.send((alert, cm.clone(), None)).await.unwrap();
    }

    let tx = tokio::time::timeout(Duration::from_secs(5), sent_rx.recv())
        .await
        .unwrap()
        .unwrap();
    let report = tokio::time::timeout(Duration::from_secs(5), alerts_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(report.rule, RULE);
    assert_eq!(report.metadata["riposte"], "landed");
    assert_eq!(report.metadata["signature"], tx.signatures[0].to_string());
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(sent_rx.try_recv().is_err(), "duplicate tx sent");
    assert!(
        alerts_rx.try_recv().is_err(),
        "second countermeasure reported"
    );

    let msg = &tx.message;
    assert_eq!(msg.static_account_keys()[0], payer);
    assert_eq!(msg.header().num_required_signatures, 1);
    assert_eq!(*msg.recent_blockhash(), hash);
    assert!(tx.signatures[0].verify(payer.as_ref(), &msg.serialize()));

    let VersionedMessage::V1(v1) = msg else {
        panic!("expected a v1 message");
    };
    // 1_000 micro-lamports/CU * 60_000 CU = 60 lamports total.
    assert_eq!(v1.config.priority_fee, Some(60));
    assert_eq!(v1.config.compute_unit_limit, Some(60_000));
    assert_eq!(
        v1.config.loaded_accounts_data_size_limit,
        Some(64 * 1024 * 1024)
    );
    let ixs: Vec<(Pubkey, Vec<u8>)> = v1
        .instructions
        .iter()
        .map(|ix| {
            (
                v1.account_keys[ix.program_id_index as usize],
                ix.data.clone(),
            )
        })
        .collect();
    assert_eq!(ixs, vec![(PROGRAM, vec![1, 2, 3]), (PROGRAM, vec![4])]);
}
