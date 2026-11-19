//! Riposte answers a rule's alert on-chain (e.g. pausing a program) without a human in the loop.
//! Rules queue `(alert, countermeasure)` straight to it, apart from alerting's batching; per item:
//! kill switch → gate (one per condition, across restarts and pods) → one v1 tx (compute budget,
//! allowlisted programs) → sign (the program's key) → send to every broadcaster → track until
//! landed, rebroadcasting → outcome alert on the rule's own sinks.

pub mod balance;
pub mod blockhash;
pub mod broadcast;
pub mod compose;
pub mod config;
pub mod fees;
pub mod halt;
pub mod signer;
pub mod traits;

use crate::blockhash::BlockhashCache;
use crate::config::Mode;
use crate::fees::PriorityFeeCache;
use crate::halt::HaltSwitch;
use crate::traits::{Broadcaster, TxSigner};
use dedup_gate::fleet_gate::FleetGate;
use anyhow::Context;
use domain::alert::{Alert, Severity};
use domain::counter_measure::{CounterMeasure, Trigger};
use domain::programs::Program;
use futures::future::join_all;
use observability::prometheus::metrics::record_countermeasure;
use solana_message::VersionedMessage;
use solana_pubkey::Pubkey;
use solana_rpc_client::api::config::CommitmentConfig;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_signature::Signature;
use solana_transaction::versioned::VersionedTransaction;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, Semaphore};
use tokio::time::{timeout, MissedTickBehavior};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{error, info, warn};

/// Capacity of the queue rules send countermeasures into; full drops (never blocks a rule).
pub const QUEUE: usize = 256;
/// Countermeasures in flight at once (each may track landing for up to a blockhash lifetime).
const MAX_IN_FLIGHT: usize = 64;
const STATUS_POLL: Duration = Duration::from_millis(400);
const REBROADCAST: Duration = Duration::from_secs(2);
/// Per RPC call and per broadcaster send, so a slow endpoint can't stall tracking or shutdown.
const RPC_TIMEOUT: Duration = Duration::from_secs(2);
const SIGN_TIMEOUT: Duration = Duration::from_secs(5);
/// Past a blockhash lifetime (~60-90 s) yet under the 180 s minimum cooldown, so the claim
/// still holds when tracking gives up.
const LAND_TIMEOUT: Duration = Duration::from_secs(120);
/// A stream trigger further behind the tip than this (~60 s) is a replay or a lagging feed.
const MAX_TRIGGER_AGE_SLOTS: u64 = 150;
/// Waiting on a stalled alerting channel must not pin a permit.
const REPORT_TIMEOUT: Duration = Duration::from_secs(5);

pub struct Riposte {
    pub mode: Mode,
    /// Per program: its on-chain authority, which also pays the fee (one signature per tx).
    pub signers: HashMap<Program, Arc<dyn TxSigner>>,
    /// Every one gets the tx; one accepting it is success.
    pub broadcasters: Vec<Arc<dyn Broadcaster>>,
    /// Signature status and block height, to track landing (live mode).
    pub rpc: Arc<RpcClient>,
    pub blockhash: BlockhashCache,
    pub fees: PriorityFeeCache,
    pub allowed_programs: HashSet<Pubkey>,
    /// Strict fleet gate: one claim per condition, from claim time, never taken over. Cloned per
    /// countermeasure; clones share the connection and the in-process open claims.
    pub gate: FleetGate,
    pub halt: HaltSwitch,
    /// Outcome alerts go to alerting under the triggering rule.
    pub alerts: mpsc::Sender<Alert>,
}

/// What became of a countermeasure the kill switch didn't stop.
enum Outcome {
    /// Gate's Redis failed; fail closed, nothing sent.
    GateError(String),
    /// Halted between claim and first broadcast; claim released.
    Halted,
    /// Never left: compose or sign failed; claim released.
    NotSent(String),
    /// Dry run, simulation rejected it; claim released.
    SimulationFailed(String),
    Simulated(Signature),
    Landed(Signature, u64),
    /// Confirmed, but the tx failed on-chain.
    Reverted(Signature, String),
    /// Past its blockhash's last valid height and not seen landed.
    Expired(Signature),
    /// Shutdown, tracking timed out, or the final status read failed; it may have landed.
    Unknown(Signature),
}

impl Outcome {
    /// Whether the tx never left this process, so the condition may be retried.
    fn unsent(&self) -> bool {
        matches!(
            self,
            Self::Halted | Self::NotSent(_) | Self::SimulationFailed(_)
        )
    }
}

impl Riposte {
    /// Drains `queue` (fed by rules' `counteract` via `RiposteCtx::tx` in core) until
    /// cancelled; each countermeasure runs on its own task, at most `MAX_IN_FLIGHT` at once.
    pub fn start(
        self,
        mut queue: mpsc::Receiver<Trigger>,
        cancel: CancellationToken,
        tracker: &TaskTracker,
    ) {
        let this = Arc::new(self);
        let tasks = tracker.clone();
        let in_flight = Arc::new(Semaphore::new(MAX_IN_FLIGHT));
        tracker.spawn(async move {
            loop {
                // A full house backs up into the bounded queue, where drops are counted.
                let permit = tokio::select! {
                    () = cancel.cancelled() => break,
                    permit = Arc::clone(&in_flight).acquire_owned() => permit.expect("never closed"),
                };
                let (alert, cm, slot) = tokio::select! {
                    () = cancel.cancelled() => break,
                    item = queue.recv() => match item {
                        Some(item) => item,
                        None => return,
                    },
                };
                let this = Arc::clone(&this);
                let cancel = cancel.clone();
                tasks.spawn(async move {
                    this.counteract(alert, cm, slot, cancel).await;
                    drop(permit);
                });
            }
            while let Ok((alert, _, _)) = queue.try_recv() {
                warn!(rule = %alert.rule, "shutting down; queued countermeasure dropped");
                record_countermeasure(&alert.rule.to_string(), "dropped");
            }
        });
    }

    async fn counteract(
        &self,
        alert: Alert,
        cm: CounterMeasure,
        slot: Option<u64>,
        cancel: CancellationToken,
    ) {
        let rule = alert.rule.to_string();
        if self.halt.is_halted() {
            warn!(rule = %alert.rule, "riposte halted by kill switch; countermeasure not sent");
            record_countermeasure(&rule, "halted");
            return;
        }
        if let Some(slot) = slot {
            let tip = timeout(
                RPC_TIMEOUT,
                self.rpc.get_slot_with_commitment(CommitmentConfig::confirmed()),
            )
            .await;
            match tip {
                // A replay, or a feed lagging during a real incident: either way a human must know.
                Ok(Ok(tip)) if tip.saturating_sub(slot) > MAX_TRIGGER_AGE_SLOTS => {
                    let why = format!("trigger slot {slot} is {} slots behind the tip (replay or feed lag)", tip - slot);
                    self.report(&alert, &Outcome::NotSent(why)).await;
                    return;
                }
                Ok(Ok(_)) => {}
                // Can't tell a replay from a live trigger: fail closed.
                Ok(Err(_)) | Err(_) => {
                    let why = "could not read the current slot to check the trigger's age".into();
                    self.report(&alert, &Outcome::NotSent(why)).await;
                    return;
                }
            }
        }
        let mut gate = self.gate.clone();
        match gate.check(&alert).await {
            Ok(true) => {}
            Ok(false) => {
                info!(rule = %alert.rule, "riposte countermeasure suppressed by gate");
                record_countermeasure(&rule, "suppressed");
                return;
            }
            Err(e) => {
                self.report(&alert, &Outcome::GateError(format!("{e:#}"))).await;
                return;
            }
        }
        let outcome = self.send(&alert, &cm, &cancel).await;
        // Report first: the outcome is known, and settling may wait on a slow Redis.
        self.report(&alert, &outcome).await;
        let settled = if outcome.unsent() {
            gate.release(&alert).await
        } else {
            gate.confirm(&alert).await
        };
        // An unsettled claim blocks the condition until its TTL: fail closed.
        if let Err(e) = settled {
            error!(rule = %alert.rule, error = %e, "riposte gate settle failed");
        }
    }

    /// Compose, sign, broadcast and (live) track until landed or expired.
    async fn send(
        &self,
        alert: &Alert,
        cm: &CounterMeasure,
        cancel: &CancellationToken,
    ) -> Outcome {
        // Startup checks every responding rule's program has one; this is a backstop.
        let Some(signer) = self.signers.get(&alert.rule.program) else {
            return Outcome::NotSent("no signer for this program".into());
        };
        let (msg, last_valid) = match self.tx_message(cm, &signer.pubkey()) {
            Ok(built) => built,
            Err(e) => return Outcome::NotSent(format!("compose failed: {e:#}")),
        };
        let signature = match timeout(SIGN_TIMEOUT, signer.sign(&msg, alert)).await {
            Ok(Ok(sig)) => sig,
            Ok(Err(e)) => return Outcome::NotSent(format!("signing failed: {e:#}")),
            Err(_) => return Outcome::NotSent(format!("signing timed out after {SIGN_TIMEOUT:?}")),
        };
        // Signing can take seconds; the switch or a shutdown may have flipped meanwhile.
        if self.halt.is_halted() {
            return Outcome::Halted;
        }
        if cancel.is_cancelled() {
            return Outcome::NotSent("shutting down".into());
        }
        let tx = VersionedTransaction {
            signatures: vec![signature],
            message: msg,
        };
        let sent = self.broadcast(alert, &tx).await;
        info!(rule = %alert.rule, %signature, accepted = sent.is_ok(), "riposte countermeasure broadcast");
        match self.mode {
            Mode::DryRun => match sent {
                Ok(()) => Outcome::Simulated(signature),
                Err(e) => Outcome::SimulationFailed(e),
            },
            // A broadcaster error may still have forwarded it, so track it either way.
            Mode::Live => self.land(alert, &tx, signature, last_valid, cancel).await,
        }
    }

    /// Sends to every broadcaster at once; `Err` with every error when none accepted.
    async fn broadcast(&self, alert: &Alert, tx: &VersionedTransaction) -> Result<(), String> {
        let sends = self.broadcasters.iter().map(|b| timeout(RPC_TIMEOUT, b.send(tx)));
        let results = join_all(sends).await;
        let mut errors = Vec::new();
        for (b, result) in self.broadcasters.iter().zip(results) {
            match result.unwrap_or_else(|_| Err(anyhow::anyhow!("timed out after {RPC_TIMEOUT:?}"))) {
                Ok(()) => return Ok(()),
                Err(e) => {
                    warn!(rule = %alert.rule, broadcaster = b.id(), error = ?e, "riposte broadcast failed");
                    errors.push(format!("{}: {e:#}", b.id()));
                }
            }
        }
        Err(errors.join("; "))
    }

    /// Polls the signature until `confirmed`, rebroadcasting, until the blockhash expires.
    async fn land(
        &self,
        alert: &Alert,
        tx: &VersionedTransaction,
        signature: Signature,
        last_valid: u64,
        cancel: &CancellationToken,
    ) -> Outcome {
        let mut poll = tokio::time::interval(STATUS_POLL);
        poll.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let started = Instant::now();
        let mut sent_at = started;
        loop {
            tokio::select! {
                () = cancel.cancelled() => return Outcome::Unknown(signature),
                _ = poll.tick() => {}
            }
            if let Ok(Some(done)) = self.status(alert, signature).await {
                return done;
            }
            if started.elapsed() >= LAND_TIMEOUT {
                warn!(rule = %alert.rule, %signature, "riposte gave up tracking; landing unknown");
                return Outcome::Unknown(signature);
            }
            // Finalized height trails confirmed, so expiry is declared late, never early.
            let height = timeout(
                RPC_TIMEOUT,
                self.rpc
                    .get_block_height_with_commitment(CommitmentConfig::finalized()),
            )
            .await;
            match height {
                Ok(Ok(h)) if h > last_valid => {
                    // A failed status poll must not read as "never landed": look once more.
                    return match self.status(alert, signature).await {
                        Ok(Some(done)) => done,
                        Ok(None) => Outcome::Expired(signature),
                        Err(()) => Outcome::Unknown(signature),
                    };
                }
                Ok(Ok(_)) => {}
                Ok(Err(e)) => warn!(rule = %alert.rule, error = %e, "riposte block height poll failed"),
                Err(_) => warn!(rule = %alert.rule, "riposte block height poll timed out"),
            }
            if sent_at.elapsed() >= REBROADCAST && !self.halt.is_halted() {
                let _ = self.broadcast(alert, tx).await;
                sent_at = Instant::now();
            }
        }
    }

    /// `Some` once the signature is `confirmed`: landed, or reverted on-chain. `Err` when the
    /// RPC read failed (already logged), so "not found" and "couldn't look" stay apart.
    async fn status(&self, alert: &Alert, signature: Signature) -> Result<Option<Outcome>, ()> {
        let resp = match timeout(RPC_TIMEOUT, self.rpc.get_signature_statuses(&[signature])).await {
            Ok(Ok(resp)) => resp,
            Ok(Err(e)) => {
                warn!(rule = %alert.rule, %signature, error = %e, "riposte status poll failed");
                return Err(());
            }
            Err(_) => {
                warn!(rule = %alert.rule, %signature, "riposte status poll timed out");
                return Err(());
            }
        };
        let Some(status) = resp.value.into_iter().next().flatten() else {
            return Ok(None);
        };
        if !status.satisfies_commitment(CommitmentConfig::confirmed()) {
            return Ok(None);
        }
        Ok(Some(match status.err {
            None => Outcome::Landed(signature, status.slot),
            Some(e) => Outcome::Reverted(signature, format!("{e:?}")),
        }))
    }

    /// Metric for every outcome; an alert on the rule's own sinks for the ones a human must see.
    async fn report(&self, alert: &Alert, outcome: &Outcome) {
        let (name, severity, detail, signature) = match outcome {
            Outcome::GateError(e) => ("gate_error", Severity::Critical, format!("gate Redis failed, nothing sent: {e}"), None),
            Outcome::Halted => ("halted", Severity::Warn, "kill switch flipped before broadcast; not sent".into(), None),
            Outcome::NotSent(why) => ("failed", Severity::Critical, why.clone(), None),
            Outcome::SimulationFailed(why) => ("simulation_failed", Severity::Critical, format!("dry-run simulation failed: {why}"), None),
            Outcome::Simulated(sig) => ("simulated", Severity::Info, "dry run, not sent".into(), Some(sig)),
            Outcome::Landed(sig, slot) => ("landed", Severity::Warn, format!("landed in slot {slot}"), Some(sig)),
            Outcome::Reverted(sig, err) => ("reverted", Severity::Critical, format!("failed on-chain: {err}"), Some(sig)),
            Outcome::Expired(sig) => ("expired", Severity::Critical, "not seen landed before its blockhash expired".into(), Some(sig)),
            Outcome::Unknown(sig) => ("unknown", Severity::Critical, "landing not confirmed (shutdown, tracking timeout or RPC failure); check the signature".into(), Some(sig)),
        };
        record_countermeasure(&alert.rule.to_string(), name);
        match severity {
            Severity::Critical => error!(rule = %alert.rule, outcome = name, detail, "riposte countermeasure"),
            _ => info!(rule = %alert.rule, outcome = name, detail, "riposte countermeasure"),
        }
        // Retryable failures repeat per occurrence; keyed on the condition, alerting's cooldown
        // collapses them. Everything else is unique per trigger, so it's never hidden.
        let id = if signature.is_none() {
            alert.local_gate_key().key().to_string()
        } else {
            alert.id.to_string()
        };
        let report = Alert::for_rule(alert.rule)
            .severity(severity)
            .message(format!("Riposte countermeasure {name}: {detail}"))
            .meta("riposte", name)
            .meta("trigger", alert.id)
            .meta_opt("signature", signature)
            .key(["riposte", name, &id])
            .build();
        if let Err(e) = self.alerts.send_timeout(report, REPORT_TIMEOUT).await {
            warn!(rule = %alert.rule, error = %e, "riposte outcome alert dropped");
        }
    }

    /// The message and its blockhash's last valid block height.
    fn tx_message(
        &self,
        cm: &CounterMeasure,
        payer: &Pubkey,
    ) -> anyhow::Result<(VersionedMessage, u64)> {
        let (blockhash, last_valid) = self
            .blockhash
            .get()
            .context("blockhash is stale or missing")?;
        let msg = compose::compose(
            &cm.ixs,
            cm.compute_units,
            self.fees.get(),
            payer,
            blockhash,
            &self.allowed_programs,
        )?;
        Ok((msg, last_valid))
    }
}
