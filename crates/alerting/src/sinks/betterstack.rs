use crate::sinks::{AlertSink, AlertSinkError};
use async_trait::async_trait;
use domain::alert::{Alert, Severity};
use domain::sinks::SinkId;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::time::Duration;
use tracing::{info, warn};
use url::Url;

pub const BETTERSTACK_SINK_ID: &str = "betterstack";

/// Metadata key carrying the alert's fleet dedup key, used to find an open incident for it.
pub const DEDUP_METADATA_KEY: &str = "sentinel_dedup_key";

/// Caps the open-incident lookup (send + body) well under the 5s flush deadline,
/// so a stalled GET still leaves time to create the incident.
const LOOKUP_TIMEOUT: Duration = Duration::from_millis(1500);

#[derive(Debug, Deserialize)]
struct IncidentList {
    data: Vec<serde_json::Value>,
}

/// Whether BetterStack has an unresolved incident tagged with `dedup_key`.
async fn has_open_incident(
    http_client: &Client,
    url: &Url,
    auth_token: &str,
    dedup_key: &str,
) -> anyhow::Result<bool> {
    let metadata_filter = format!("metadata[{DEDUP_METADATA_KEY}][][value]");
    let resp = http_client
        .get(url.clone())
        .bearer_auth(auth_token)
        .query(&[
            ("resolved", "false"),
            ("per_page", "1"),
            (metadata_filter.as_str(), dedup_key),
        ])
        .send()
        .await?;
    let status = resp.status();
    if !status.is_success() {
        let text = resp.text().await.unwrap_or_default();
        anyhow::bail!("BetterStack incident lookup failed: HTTP {status}: {text}");
    }
    Ok(!resp.json::<IncidentList>().await?.data.is_empty())
}

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

    /// Dedup key scoped to this sink's escalation policy, so an incident open under
    /// one policy doesn't suppress paging a different one.
    fn dedup_key(&self, alert: &Alert) -> String {
        format!("{}:{}", self.escalation_policy_id, alert.fleet_gate_key)
    }

    /// Map a domain `Alert` into a BetterStack incident payload.
    fn payload_from_alert(alert: Alert, dedup_key: String) -> IncidentPayload {
        let mut metadata = alert.metadata;
        metadata.insert(DEDUP_METADATA_KEY.into(), dedup_key);
        IncidentPayload::new(
            alert.rule.to_string(), // name: rule identity
            alert.message.clone(),  // summary: the human message
            alert.severity,         // severity: warning or critical
            metadata,
        )
    }
}

#[async_trait]
impl AlertSink for BetterstackSink {
    fn id(&self) -> &SinkId {
        &self.id
    }

    async fn send(&self, alert: &Alert) -> Result<(), AlertSinkError> {
        let dedup_key = self.dedup_key(alert);
        // Fail open: a failed or timed-out lookup still creates the incident.
        let lookup = tokio::time::timeout(
            LOOKUP_TIMEOUT,
            has_open_incident(
                &self.client,
                &self.incident_api_url,
                &self.incident_auth_token,
                &dedup_key,
            ),
        )
        .await
        .unwrap_or_else(|_| Err(anyhow::anyhow!("lookup timed out after {LOOKUP_TIMEOUT:?}")));
        match lookup {
            Ok(true) => {
                info!(rule = %alert.rule, %dedup_key, "BetterStack incident already open, skipping");
                return Ok(());
            }
            Ok(false) => {}
            Err(err) => warn!(error = %err, %dedup_key, "BetterStack open-incident check failed"),
        }
        let payload = Self::payload_from_alert(alert.clone(), dedup_key);
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
