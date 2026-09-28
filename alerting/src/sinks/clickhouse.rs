use crate::sinks::{AlertSink, AlertSinkError};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use clickhouse::insert::Insert;
use clickhouse::{Client as ClickhouseClient, Row};
use domain::alert::Alert;
use domain::sinks::SinkId;
use serde::Serialize;
use std::time::Duration;
use tracing::warn;
use url::Url;
use uuid::Uuid;

pub const CLICKHOUSE_TABLE_NAME: &str = "alerts";

pub struct ClickhouseSink {
    id: SinkId,
    client: ClickhouseClient,
}

#[derive(Debug, Clone, Row, Serialize)]
struct AlertRow {
    #[serde(with = "clickhouse::serde::uuid")]
    id: Uuid,
    rule: String,
    severity: String,
    message: String,
    metadata: Vec<(String, String)>,
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    fired_at: DateTime<Utc>,
}

impl From<&Alert> for AlertRow {
    fn from(a: &Alert) -> Self {
        Self {
            id: a.id,
            rule: a.rule.to_string(),
            severity: a.severity.to_string(),
            message: a.message.clone(),
            metadata: a
                .metadata
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            fired_at: a.fired_at,
        }
    }
}

impl ClickhouseSink {
    pub fn build(id: SinkId, url: &Url, user: String, password: String, db: String) -> Self {
        let client = ClickhouseClient::default()
            .with_url(url.as_str())
            .with_user(user)
            .with_password(password)
            .with_database(db)
            .with_compression(clickhouse::Compression::Lz4);
        Self { id, client }
    }

    async fn insert_alerts(&self, alerts: &[Alert]) -> Result<(), AlertSinkError> {
        if alerts.is_empty() {
            return Ok(());
        }

        const MAX_ATTEMPTS: u32 = 3;
        const BASE_BACKOFF: Duration = Duration::from_millis(100);

        let rows: Vec<AlertRow> = alerts.iter().map(AlertRow::from).collect();

        let mut attempt = 0;
        loop {
            attempt += 1;
            let result = async {
                let mut insert: Insert<AlertRow> =
                    self.client.insert(CLICKHOUSE_TABLE_NAME).await?;
                for row in &rows {
                    insert.write(row).await?;
                }
                insert.end().await?;
                Ok::<(), AlertSinkError>(())
            }
            .await;

            match result {
                Ok(()) => return Ok(()),
                Err(e) => {
                    if attempt >= MAX_ATTEMPTS {
                        return Err(e);
                    }
                    let backoff = BASE_BACKOFF * 2u32.pow(attempt - 1);
                    warn!(
                        attempt,
                        max_attempts = MAX_ATTEMPTS,
                        error = %e,
                        backoff_ms = backoff.as_millis(),
                        "ClickHouse insert failed, retrying"
                    );
                    tokio::time::sleep(backoff).await;
                }
            }
        }
    }
}

#[async_trait]
impl AlertSink for ClickhouseSink {
    fn id(&self) -> &SinkId {
        &self.id
    }

    async fn send(&self, alert: &Alert) -> Result<(), AlertSinkError> {
        self.insert_alerts(std::slice::from_ref(alert)).await
    }

    async fn send_batch(&self, alerts: &[Alert]) -> Result<(), AlertSinkError> {
        if alerts.is_empty() {
            return Ok(());
        }
        self.insert_alerts(alerts).await
    }
}
