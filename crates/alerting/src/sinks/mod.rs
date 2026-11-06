use async_trait::async_trait;
use domain::alert::Alert;
use domain::sinks::SinkId;
use tracing::warn;

pub mod betterstack;
pub mod clickhouse;
pub mod jsonl;
pub mod slack;
pub mod stdout;

#[derive(Debug, thiserror::Error)]
pub enum AlertSinkError {
    #[error("clickhouse error: {0}")]
    Clickhouse(#[from] ::clickhouse::error::Error),
    #[error("slack sink error: {0}")]
    SlackSinkError(Box<dyn std::error::Error + Send + Sync>),
    #[error("betterstack sink error: {0}")]
    BetterStackSinkError(String),
    #[error("jsonl sink error: {0}")]
    Jsonl(String),
}

#[async_trait]
pub trait AlertSink: Send + Sync {
    fn id(&self) -> &SinkId;
    async fn send(&self, alert: &Alert) -> Result<(), AlertSinkError>;
    async fn send_batch(&self, alerts: &[Alert]) -> Result<(), AlertSinkError>;

    /// Writes each alert's outcome into `out` as soon as it is known, so a caller that times
    /// out still sees what completed. Default: the batch is all-or-nothing.
    async fn send_each(&self, alerts: &[Alert], out: &mut [Delivery]) {
        let outcome = match self.send_batch(alerts).await {
            Ok(()) => Delivery::Delivered,
            Err(e) => {
                warn!(sink = %self.id(), error = %e, batch_len = alerts.len(), "Failed to deliver alerts to sink");
                Delivery::Failed
            }
        };
        out.fill(outcome);
    }
}

/// One alert's outcome at one sink.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Delivery {
    /// Not attempted, or still in flight when the attempt was cut off.
    Pending,
    Delivered,
    Failed,
    /// The sink asked to wait (rate limit); not a failed attempt.
    Deferred,
}
