use crate::sinks::{AlertSink, AlertSinkError};
use async_trait::async_trait;
use domain::alert::{Alert, Severity};
use domain::sinks::SinkId;
use reqwest::Client;
use serde::Serialize;
use std::collections::BTreeMap;
use std::time::Duration;
use tracing::warn;
use url::Url;

pub const BETTERSTACK_SINK_ID: &str = "betterstack";

/// Per-incident data. The common fields (`requester_email`, `policy_id`) are
/// supplied once by the reporter via `BetterstackIncident`, not here.
#[derive(Debug, Clone, Serialize)]
pub struct IncidentPayload {
    pub name: String,
    pub summary: String,
    pub critical_alert: bool,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, String>,
}

/// Wire-format body sent to BetterStack: a per-incident payload plus the
/// reporter-wide common fields flattened in alongside it.
#[derive(Debug, Serialize)]
pub struct BetterstackIncident<'a> {
    #[serde(flatten)]
    pub payload: IncidentPayload,
    pub requester_email: &'a str,
    pub policy_id: &'a str,
}

impl IncidentPayload {
    pub fn new(
        name: impl Into<String>,
        summary: impl Into<String>,
        severity: impl Into<Severity>,
        mut metadata: BTreeMap<String, String>,
    ) -> Self {
        let severity = severity.into();

        // Record severity as metadata for visibility in the incident UI.
        metadata.insert("severity".into(), severity.to_string());

        Self {
            name: name.into(),
            summary: summary.into(),
            critical_alert: severity.is_critical(),
            metadata,
        }
    }
}

async fn report_incident(
    incident: IncidentPayload,
    http_client: &Client,
    url: &Url,
    auth_token: &str,
    escalation_policy_id: &str,
    requester_email: &str,
) -> anyhow::Result<()> {
    const MAX_ATTEMPTS: u32 = 3;
    const BASE_BACKOFF: Duration = Duration::from_millis(100);
    let body = BetterstackIncident {
        payload: incident,
        requester_email,
        policy_id: escalation_policy_id,
    };
    let mut attempt = 0;
    loop {
        attempt += 1;
        // Ok(()) = success; Err((error, retryable)) = failed attempt.
        let result: Result<(), (anyhow::Error, bool)> = async {
            let resp = http_client
                .post(url.clone())
                .bearer_auth(auth_token)
                .json(&body)
                .send()
                .await;
            let Ok(resp) = resp else {
                return Err((
                    anyhow::anyhow!("BetterStack incident create failed: no response"),
                    true,
                ));
            };
            let status = resp.status();
            if status.is_success() {
                return Ok(());
            }
            let text = resp.text().await.unwrap_or_default();
            // 4xx (except 429) won't be fixed by retrying.
            let retryable = status.is_server_error() || status.as_u16() == 429;
            Err((
                anyhow::anyhow!("BetterStack incident create failed: HTTP {status}: {text}"),
                retryable,
            ))
        }
        .await;
        match result {
            Ok(()) => return Ok(()),
            Err((err, retryable)) => {
                if !retryable || attempt >= MAX_ATTEMPTS {
                    return Err(err);
                }
                let backoff = BASE_BACKOFF * 2u32.pow(attempt - 1);
                warn!(
                    attempt,
                    max_attempts = MAX_ATTEMPTS,
                    error = %err,
                    backoff_ms = backoff.as_millis(),
                    "BetterStack incident create failed, retrying"
                );
                tokio::time::sleep(backoff).await;
            }
        }
    }
}

pub struct BetterstackSink {
    id: SinkId,
    client: Client,
    incident_api_url: Url,
    incident_auth_token: String,
    escalation_policy_id: String,
    requester_email: String,
}

impl BetterstackSink {
    pub fn build(
        id: SinkId,
        incident_api_url: &str,
        incident_auth_token: String,
        escalation_policy_id: String,
        requester_email: String,
    ) -> Result<Self, AlertSinkError> {
        Ok(Self {
            id,
            client: Client::new(),
            incident_api_url: Url::parse(incident_api_url)
                .map_err(|e| AlertSinkError::BetterStackSinkError(e.to_string()))?,
            incident_auth_token,
            escalation_policy_id,
            requester_email,
        })
    }

    /// Map a domain `Alert` into a BetterStack incident payload.
    fn payload_from_alert(alert: Alert) -> IncidentPayload {
        IncidentPayload::new(
            alert.rule.to_string(), // name: rule identity
            alert.message.clone(),  // summary: the human message
            alert.severity,         // severity: warning or critical
            alert.metadata,
        )
    }
}

#[async_trait]
impl AlertSink for BetterstackSink {
    fn id(&self) -> &SinkId {
        &self.id
    }

    async fn send(&self, alert: &Alert) -> Result<(), AlertSinkError> {
        let payload = Self::payload_from_alert(alert.clone());
        report_incident(
            payload,
            &self.client,
            &self.incident_api_url,
            &self.incident_auth_token,
            &self.escalation_policy_id,
            &self.requester_email,
        )
        .await
        .map_err(|e| AlertSinkError::BetterStackSinkError(e.to_string()))
    }

    async fn send_batch(&self, alerts: &[Alert]) -> Result<(), AlertSinkError> {
        let futures = alerts.iter().map(|alert| self.send(alert));
        let results = futures::future::join_all(futures).await;

        // Report first error but attempt all (BetterStack creates one incident per alert).
        results.into_iter().collect::<Result<Vec<_>, _>>()?;
        Ok(())
    }
}
