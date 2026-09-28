use async_trait::async_trait;
use domain::alert::Alert;
use domain::sinks::SinkId;

pub mod betterstack;
pub mod clickhouse;
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
}

#[async_trait]
pub trait AlertSink: Send + Sync {
    fn id(&self) -> &SinkId;
    async fn send(&self, alert: &Alert) -> Result<(), AlertSinkError>;
    async fn send_batch(&self, alerts: &[Alert]) -> Result<(), AlertSinkError>;
}
