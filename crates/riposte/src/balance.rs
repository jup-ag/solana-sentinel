//! Watches each program signer's SOL: it pays that program's countermeasure fees, and an
//! empty signer means every countermeasure fails exactly when it's needed.

use domain::alert::{Alert, Severity};
use domain::rules::RuleId;
use observability::prometheus::metrics::set_riposte_signer_balance;
use solana_pubkey::Pubkey;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{error, warn};

const REFRESH: Duration = Duration::from_secs(60);
/// One hung read must not hold up the other signers' checks.
const READ_TIMEOUT: Duration = Duration::from_secs(5);

/// `signers`: each program's signer with one of its responding rules, whose sinks get the
/// low-balance alert (one key per signer, so the rule's cooldown collapses repeats).
pub fn spawn_balance_watch(
    rpc: Arc<RpcClient>,
    signers: Vec<(RuleId, Pubkey)>,
    floor_lamports: u64,
    alerts: mpsc::Sender<Alert>,
    cancel: CancellationToken,
    tracker: &TaskTracker,
) {
    tracker.spawn(async move {
        let mut tick = tokio::time::interval(REFRESH);
        loop {
            tokio::select! {
                () = cancel.cancelled() => return,
                _ = tick.tick() => {}
            }
            for (rule, signer) in &signers {
                let program = rule.program;
                let read = tokio::select! {
                    () = cancel.cancelled() => return,
                    read = timeout(READ_TIMEOUT, rpc.get_balance(signer)) => read,
                };
                let lamports = match read {
                    Ok(Ok(lamports)) => lamports,
                    Ok(Err(e)) => {
                        warn!(%program, %signer, error = %e, "riposte signer balance read failed");
                        continue;
                    }
                    Err(_) => {
                        warn!(%program, %signer, "riposte signer balance read timed out");
                        continue;
                    }
                };
                set_riposte_signer_balance(program.as_str(), lamports);
                if lamports >= floor_lamports {
                    continue;
                }
                error!(%program, %signer, lamports, floor_lamports, "riposte signer below balance floor; top it up");
                let alert = Alert::for_rule(*rule)
                    .severity(Severity::Critical)
                    .message(format!(
                        "Riposte signer {signer} for {program} holds {lamports} lamports, below {floor_lamports}; countermeasures will fail to pay fees"
                    ))
                    .meta("riposte", "low_balance")
                    .meta("signer", signer)
                    .meta("lamports", lamports)
                    .key(["riposte", "low_balance", &signer.to_string()])
                    .build();
                if let Err(e) = alerts.send_timeout(alert, READ_TIMEOUT).await {
                    warn!(%program, %signer, error = %e, "riposte low-balance alert dropped");
                }
            }
        }
    });
}
