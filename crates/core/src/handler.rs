//! One shipstern handler for every stream pipeline: record the metric, build
//! the [`StreamCtx`], evaluate the rules, log by severity, forward.

use crate::parser::StreamInput;
use crate::rule::BuiltStreamRule;
use domain::alert::{Alert, Severity};
use observability::prometheus::metrics::record_grpc_event;
use shipstern::{Handler, HandlerResult};
use std::fmt;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

pub struct RuleHandler<Ev> {
    rules: Arc<[BuiltStreamRule<Ev>]>,
    alerts_tx: mpsc::Sender<Alert>,
}

impl<Ev> Clone for RuleHandler<Ev> {
    fn clone(&self) -> Self {
        Self {
            rules: Arc::clone(&self.rules),
            alerts_tx: self.alerts_tx.clone(),
        }
    }
}

impl<Ev> fmt::Debug for RuleHandler<Ev> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RuleHandler")
            .field(
                "rules",
                &self.rules.iter().map(|r| r.id).collect::<Vec<_>>(),
            )
            .finish_non_exhaustive()
    }
}

impl<Ev> RuleHandler<Ev> {
    pub fn new(rules: Vec<BuiltStreamRule<Ev>>, alerts_tx: mpsc::Sender<Alert>) -> Self {
        Self {
            rules: rules.into(),
            alerts_tx,
        }
    }
}

impl<Ev: Send + Sync, R: StreamInput> Handler<Ev, R> for RuleHandler<Ev> {
    async fn handle(&self, value: &Ev, raw: &R) -> HandlerResult<()> {
        record_grpc_event(R::KIND.metric());
        let ctx = raw.stream_ctx();
        for rule in self.rules.iter() {
            if let Some(alert) = (rule.evaluate)(value, &ctx) {
                forward_alert(
                    alert,
                    Some(ctx.slot),
                    ctx.tx_sig.as_deref(),
                    &self.alerts_tx,
                )
                .await;
            }
        }
        Ok(())
    }
}

/// Log an alert by severity and hand it to the alerting service. Fills
/// `tx_sig` metadata from the feed when the rule did not set it.
pub async fn forward_alert(
    mut alert: Alert,
    slot: Option<u64>,
    tx_sig: Option<&str>,
    alerts_tx: &mpsc::Sender<Alert>,
) {
    if let Some(sig) = tx_sig {
        alert
            .metadata
            .entry("tx_sig".to_string())
            .or_insert_with(|| sig.to_string());
    }

    match alert.severity {
        Severity::Info => info!(
            rule = alert.rule.name,
            slot,
            metadata = ?alert.metadata,
            message = %alert.message,
            "Alert raised",
        ),
        Severity::Warn | Severity::Critical => warn!(
            rule = alert.rule.name,
            slot,
            metadata = ?alert.metadata,
            message = %alert.message,
            "Alert raised",
        ),
    }

    if let Err(e) = alerts_tx.send(alert).await {
        error!(error = %e, "Failed to send alert to alerting service");
    }
}
