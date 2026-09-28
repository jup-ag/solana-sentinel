//! Prints alerts to stdout as JSON lines. For local runs and demos.

use crate::sinks::{AlertSink, AlertSinkError};
use async_trait::async_trait;
use domain::alert::Alert;
use domain::sinks::SinkId;
use serde_json::json;

pub struct StdoutSink {
    id: SinkId,
}

impl StdoutSink {
    pub fn new(id: SinkId) -> Self {
        Self { id }
    }
}

#[async_trait]
impl AlertSink for StdoutSink {
    fn id(&self) -> &SinkId {
        &self.id
    }

    async fn send(&self, alert: &Alert) -> Result<(), AlertSinkError> {
        let line = json!({
            "id": alert.id,
            "rule": alert.rule.name,
            "program": alert.rule.program.as_str(),
            "severity": alert.severity.to_string(),
            "message": alert.message,
            "metadata": alert.metadata,
            "fired_at": alert.fired_at,
        });
        println!("{line}");
        Ok(())
    }

    async fn send_batch(&self, alerts: &[Alert]) -> Result<(), AlertSinkError> {
        for alert in alerts {
            self.send(alert).await?;
        }
        Ok(())
    }
}
