//! Posts alerts to Discord, either through a webhook or a bot.
//!
//! Discord renders `embeds` as rich cards and pushes a one-line `content`
//! summary to the channel and to push notifications, so every message carries
//! both.

use crate::sinks::{AlertSink, AlertSinkError, Delivery};
use async_trait::async_trait;
use chrono::SecondsFormat;
use domain::alert::{Alert, Severity};
use domain::sinks::SinkId;
use parking_lot::Mutex;
use reqwest::header::RETRY_AFTER;
use reqwest::{Client as HttpClient, StatusCode};
use serde::Serialize;
use std::time::{Duration, Instant};
use tracing::warn;

const DISCORD_API_BASE: &str = "https://discord.com/api/v10";

// Discord embed limits. Exceeding any of them makes Discord reject the
// *entire* message, so we clamp defensively. Discord counts characters, not
// bytes, so these budgets are in chars.
const EMBED_TITLE_LIMIT: usize = 256;
const EMBED_DESCRIPTION_LIMIT: usize = 4096;
const EMBED_FIELDS_LIMIT: usize = 25;
const FIELD_NAME_LIMIT: usize = 256;
const FIELD_VALUE_LIMIT: usize = 1024;
/// Combined title + description + fields + footer budget for one embed.
const EMBED_TOTAL_LIMIT: usize = 6000;
const CONTENT_LIMIT: usize = 2000;

pub struct DiscordSink {
    pub id: SinkId,
    pub http_client: HttpClient,
    endpoint: Endpoint,
    /// Overrides the webhook's / bot's display name when set.
    username: Option<String>,
    /// Discord's rate-limit deadline; posts before it are deferred, not attempted.
    retry_at: Mutex<Option<Instant>>,
}

/// Where a message goes. A webhook carries its own credential in the URL; a
/// bot needs a token and a channel.
#[derive(Clone)]
enum Endpoint {
    Webhook(String),
    Bot { token: String, channel_id: String },
}

impl Endpoint {
    fn url(&self) -> String {
        match self {
            Self::Webhook(url) => url.clone(),
            Self::Bot { channel_id, .. } => {
                format!("{DISCORD_API_BASE}/channels/{channel_id}/messages")
            }
        }
    }
}

/// A post Discord refused for now; `Deferred` carries no failed attempt.
enum PostError {
    Failed(AlertSinkError),
    Deferred,
}

impl From<PostError> for AlertSinkError {
    fn from(e: PostError) -> Self {
        match e {
            PostError::Failed(e) => e,
            PostError::Deferred => Self::DiscordSinkError("rate limited".into()),
        }
    }
}

impl DiscordSink {
    /// Builds a sink from either a webhook URL or a bot token plus channel id.
    pub fn build(
        id: SinkId,
        webhook_url: Option<String>,
        bot_token: Option<String>,
        channel_id: Option<String>,
        username: Option<String>,
        timeout_seconds: u64,
    ) -> Result<Self, AlertSinkError> {
        let endpoint = match (webhook_url, bot_token, channel_id) {
            (Some(url), None, _) => {
                if !url.starts_with("https://") {
                    return Err(AlertSinkError::DiscordSinkError(format!(
                        "discord sink `{id}`: webhook_url must be an https URL"
                    )));
                }
                Endpoint::Webhook(url)
            }
            (None, Some(token), Some(channel)) => Endpoint::Bot {
                token,
                channel_id: channel,
            },
            (Some(_), Some(_), _) => {
                return Err(AlertSinkError::DiscordSinkError(format!(
                    "discord sink `{id}`: set either webhook_url or bot_token, not both"
                )));
            }
            (None, None, _) => {
                return Err(AlertSinkError::DiscordSinkError(format!(
                    "discord sink `{id}`: needs a webhook_url, or a bot_token and a channel_id"
                )));
            }
            (None, Some(_), None) => {
                return Err(AlertSinkError::DiscordSinkError(format!(
                    "discord sink `{id}`: bot_token requires a channel_id"
                )));
            }
        };

        let http_client = HttpClient::builder()
            .timeout(Duration::from_secs(timeout_seconds))
            .build()
            .map_err(|e| {
                AlertSinkError::DiscordSinkError(format!("failed to build HTTP client: {e:#}"))
            })?;

        Ok(Self {
            id,
            http_client,
            endpoint,
            username,
            retry_at: Mutex::new(None),
        })
    }

    async fn post(&self, alert: &Alert) -> Result<(), PostError> {
        const MAX_ATTEMPTS: u32 = 3;
        const BASE_BACKOFF: Duration = Duration::from_millis(100);
        // Longest wait slept in place; a longer rate-limit window defers the
        // alert to a later flush.
        const MAX_BACKOFF: Duration = Duration::from_secs(5);

        if self.retry_at.lock().is_some_and(|at| Instant::now() < at) {
            return Err(PostError::Deferred);
        }

        let payload = DiscordPayload::from_alert(self.username.as_deref(), alert);
        let mut attempt = 0;

        loop {
            attempt += 1;

            let result = async {
                let mut req = self.http_client.post(self.endpoint.url());
                if let Endpoint::Bot { token, .. } = &self.endpoint {
                    req = req.header("Authorization", format!("Bot {token}"));
                }

                let resp = req
                    .json(&payload)
                    .send()
                    .await
                    .map_err(|e| (discord_err("discord request failed", e), true, None))?;

                let status = resp.status();

                // Discord answers a webhook post with 204 No Content and a bot
                // post with 200; anything else is a failure.
                if status == StatusCode::TOO_MANY_REQUESTS {
                    let limit = parse_rate_limit(resp).await;
                    let err = AlertSinkError::DiscordSinkError(format!(
                        "discord rate limited (global={})",
                        limit.global
                    ));
                    return Err((err, true, Some(limit.retry_after)));
                }

                if status.is_server_error() {
                    return Err((
                        AlertSinkError::DiscordSinkError(format!("discord http {status}")),
                        true,
                        None,
                    ));
                }

                if !status.is_success() {
                    // The body carries Discord's reason ("Invalid Webhook Token",
                    // "Missing Access", ...), which is worth more than the status.
                    let body = resp.text().await.unwrap_or_default();
                    let err = AlertSinkError::DiscordSinkError(format!(
                        "discord http {status}: {}",
                        truncate(body.trim(), 300)
                    ));
                    return Err((err, false, None));
                }

                Ok(())
            }
            .await;

            match result {
                Ok(()) => return Ok(()),
                Err((e, retryable, retry_after)) => {
                    if let Some(wait) = retry_after {
                        if wait > MAX_BACKOFF || attempt >= MAX_ATTEMPTS {
                            warn!(error = %e, retry_after_ms = wait.as_millis(), "Discord rate limited; deferring");
                            *self.retry_at.lock() = Some(Instant::now() + wait);
                            return Err(PostError::Deferred);
                        }
                    }
                    if !retryable || attempt >= MAX_ATTEMPTS {
                        return Err(PostError::Failed(e));
                    }
                    let backoff =
                        retry_after.unwrap_or_else(|| BASE_BACKOFF * 2u32.pow(attempt - 1));
                    warn!(
                        attempt,
                        max_attempts = MAX_ATTEMPTS,
                        error = %e,
                        backoff_ms = backoff.as_millis(),
                        "Discord post failed, retrying"
                    );
                    tokio::time::sleep(backoff).await;
                }
            }
        }
    }
}

// Keeps the reqwest source chain (e.g. the connect/TLS cause), which its Display alone drops.
fn discord_err(what: &str, e: reqwest::Error) -> AlertSinkError {
    AlertSinkError::DiscordSinkError(format!("{what}: {:#}", anyhow::Error::from(e)))
}

/// Discord's rate-limit hint: how long to wait, and whether the limit is
/// account-wide rather than per-webhook.
struct RateLimit {
    retry_after: Duration,
    global: bool,
}

/// Reads the retry hint from a 429 response. Discord reports it in the body as
/// `retry_after` (seconds, fractional) and mirrors it in the `Retry-After` and
/// `X-RateLimit-Reset-After` headers.
async fn parse_rate_limit(resp: reqwest::Response) -> RateLimit {
    let header_retry_after = resp
        .headers()
        .get(RETRY_AFTER)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.trim().parse::<f64>().ok());
    let header_reset_after = resp
        .headers()
        .get("x-ratelimit-reset-after")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.trim().parse::<f64>().ok());
    let global = resp
        .headers()
        .get("x-ratelimit-global")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.eq_ignore_ascii_case("true"));

    let body = resp.text().await.unwrap_or_default();
    let body_retry_after = serde_json::from_str::<serde_json::Value>(&body)
        .ok()
        .and_then(|v| v.get("retry_after").and_then(serde_json::Value::as_f64));

    let secs = body_retry_after
        .or(header_retry_after)
        .or(header_reset_after)
        .unwrap_or(1.0)
        .max(0.0);

    RateLimit {
        retry_after: Duration::from_secs_f64(secs),
        global,
    }
}

#[async_trait]
impl AlertSink for DiscordSink {
    fn id(&self) -> &SinkId {
        &self.id
    }

    async fn send(&self, alert: &Alert) -> Result<(), AlertSinkError> {
        Ok(self.post(alert).await?)
    }

    async fn send_batch(&self, alerts: &[Alert]) -> Result<(), AlertSinkError> {
        for alert in alerts {
            self.post(alert).await?;
        }
        Ok(())
    }

    // One message per alert, so each succeeds or fails on its own.
    async fn send_each(&self, alerts: &[Alert], out: &mut [Delivery]) {
        for (alert, slot) in alerts.iter().zip(out.iter_mut()) {
            *slot = match self.post(alert).await {
                Ok(()) => Delivery::Delivered,
                Err(PostError::Deferred) => Delivery::Deferred,
                Err(PostError::Failed(e)) => {
                    warn!(sink = %self.id, rule = alert.rule.name, error = %e, "Discord alert not delivered");
                    Delivery::Failed
                }
            };
        }
    }
}

#[derive(Serialize)]
struct DiscordPayload<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    username: Option<&'a str>,
    /// Notification summary: Discord pushes this to the client's tray and
    /// shows it above the embed.
    content: String,
    embeds: Vec<DiscordEmbed>,
    /// Never ping `@everyone`/`@here`: an alerting system must not page the
    /// whole server by accident.
    allowed_mentions: AllowedMentions,
}

#[derive(Serialize)]
struct AllowedMentions {
    /// Always empty — no mention parsing.
    parse: Vec<&'static str>,
}

#[derive(Serialize)]
struct DiscordEmbed {
    title: String,
    description: String,
    color: u32,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    fields: Vec<DiscordField>,
    timestamp: String,
    footer: DiscordFooter,
}

#[derive(Serialize)]
struct DiscordField {
    name: String,
    value: String,
    inline: bool,
}

#[derive(Serialize)]
struct DiscordFooter {
    text: String,
}

impl<'a> DiscordPayload<'a> {
    fn from_alert(username: Option<&'a str>, a: &Alert) -> Self {
        let content = truncate(
            &format!("{} {}", severity_emoji(a.severity), a.rule.name),
            CONTENT_LIMIT,
        );

        Self {
            username,
            content,
            embeds: vec![build_embed(a)],
            allowed_mentions: AllowedMentions { parse: Vec::new() },
        }
    }
}

/// Build the alert's embed: rule as the title, the message as the body, and
/// the metadata as fields.
fn build_embed(a: &Alert) -> DiscordEmbed {
    let title = truncate(&format!("Rule : {}", a.rule.name), EMBED_TITLE_LIMIT);
    let description = truncate(a.message.trim(), EMBED_DESCRIPTION_LIMIT);

    // Discord caps the whole embed at 6000 chars, so fields are added only
    // while they fit in the remaining budget.
    let mut used = title.chars().count() + description.chars().count();
    let mut fields = Vec::new();

    for (k, v) in &a.metadata {
        if fields.len() >= EMBED_FIELDS_LIMIT {
            break;
        }
        let name = truncate(k.trim(), FIELD_NAME_LIMIT);
        let value = truncate(v.trim(), FIELD_VALUE_LIMIT);
        // Discord rejects a field with an empty name or value.
        if name.is_empty() || value.is_empty() {
            continue;
        }
        let cost = name.chars().count() + value.chars().count();
        if used + cost > EMBED_TOTAL_LIMIT {
            break;
        }
        used += cost;
        fields.push(DiscordField {
            name,
            value,
            inline: true,
        });
    }

    DiscordEmbed {
        title,
        description,
        color: severity_color(a.severity),
        fields,
        timestamp: a.fired_at.to_rfc3339_opts(SecondsFormat::Secs, true),
        footer: DiscordFooter {
            text: truncate(
                &format!("{} | {}", a.rule.program.as_str(), a.severity),
                EMBED_TITLE_LIMIT,
            ),
        },
    }
}

/// Clamp to a character count, appending an ellipsis if truncated. Discord
/// counts characters, not bytes, so this slices on chars rather than bytes.
fn truncate(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        return s.to_string();
    }
    let mut out: String = s.chars().take(max.saturating_sub(1)).collect();
    out.push('\u{2026}');
    out
}

fn severity_emoji(s: Severity) -> &'static str {
    match s {
        Severity::Info => "ℹ️",
        Severity::Warn => "⚠️",
        Severity::Critical => "🚨",
    }
}

fn severity_color(s: Severity) -> u32 {
    match s {
        Severity::Info => 0x36A64F,     // green
        Severity::Warn => 0xDAA038,     // amber
        Severity::Critical => 0xD50200, // red
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use domain::programs::Program;
    use domain::rules::RuleId;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio::sync::oneshot;

    fn alert(message: &str, metadata: &[(&str, &str)]) -> Alert {
        Alert::for_rule(RuleId::new(Program::new("token_program"), "balance_drop"))
            .severity(Severity::Critical)
            .message(message)
            .metas(metadata.iter().copied())
            .build()
    }

    /// A one-shot HTTP server: answers a single request with `status_line` and
    /// hands the raw request back over a channel.
    async fn mock_server(
        status_line: &'static str,
        body: &'static str,
    ) -> (String, oneshot::Receiver<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, rx) = oneshot::channel();

        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buf = Vec::new();
            let mut tmp = [0u8; 4096];

            // Read the head, then the body, so the whole request is captured.
            let head_end = loop {
                let n = socket.read(&mut tmp).await.unwrap();
                if n == 0 {
                    break buf.len();
                }
                buf.extend_from_slice(&tmp[..n]);
                if let Some(p) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                    break p + 4;
                }
            };

            let head = String::from_utf8_lossy(&buf[..head_end]).to_string();
            let content_length = head
                .lines()
                .find(|l| l.to_ascii_lowercase().starts_with("content-length:"))
                .and_then(|l| l.split(':').nth(1))
                .and_then(|v| v.trim().parse::<usize>().ok())
                .unwrap_or(0);

            while buf.len() < head_end + content_length {
                let n = socket.read(&mut tmp).await.unwrap();
                if n == 0 {
                    break;
                }
                buf.extend_from_slice(&tmp[..n]);
            }

            let response = if body.is_empty() {
                format!("{status_line}\r\n\r\n")
            } else {
                let head = format!("{status_line}\r\nContent-Type: application/json\r\n");
                let len = format!("Content-Length: {}\r\n\r\n", body.len());
                format!("{head}{len}{body}")
            };
            socket.write_all(response.as_bytes()).await.unwrap();
            socket.flush().await.unwrap();

            let _ = tx.send(String::from_utf8_lossy(&buf).to_string());
        });

        (format!("http://{addr}"), rx)
    }

    /// A sink pointed at the mock server. `build` insists on an https webhook
    /// URL, so the struct is built directly here.
    fn sink_for(url: &str) -> DiscordSink {
        DiscordSink {
            id: SinkId::new("test-discord"),
            http_client: HttpClient::new(),
            endpoint: Endpoint::Webhook(url.to_string()),
            username: None,
            retry_at: Mutex::new(None),
        }
    }

    #[tokio::test]
    async fn posts_an_embed_to_the_webhook() {
        let (url, request) = mock_server("HTTP/1.1 204 No Content", "").await;
        let sink = sink_for(&url);

        let a = alert(
            "balance dropped by 40%",
            &[(
                "wallet_address",
                "3emsAVdmGKERbHjmGfQ6oZ1e35dkf5iYcS6U4CPKFVaa",
            )],
        );
        sink.send(&a).await.unwrap();

        let raw = request.await.unwrap();
        let (head, body) = raw.split_once("\r\n\r\n").unwrap();
        assert!(
            head.starts_with("POST /"),
            "unexpected request head: {head}"
        );

        let json: serde_json::Value = serde_json::from_str(body).unwrap();

        // A notification summary plus one embed carrying the alert.
        assert!(json["content"].as_str().unwrap().contains("balance_drop"));
        let embed = &json["embeds"][0];
        assert_eq!(embed["title"], "Rule : balance_drop");
        assert_eq!(embed["description"], "balance dropped by 40%");
        assert_eq!(embed["color"], 0xD50200);
        assert_eq!(embed["fields"][0]["name"], "wallet_address");
        assert_eq!(
            embed["fields"][0]["value"],
            "3emsAVdmGKERbHjmGfQ6oZ1e35dkf5iYcS6U4CPKFVaa"
        );

        // An alerting system must never ping @everyone by accident.
        assert_eq!(json["allowed_mentions"]["parse"], serde_json::json!([]));
    }

    #[tokio::test]
    async fn defers_when_discord_rate_limits() {
        let (url, _request) = mock_server(
            "HTTP/1.1 429 Too Many Requests",
            r#"{"message":"rate limited","retry_after":30.0,"global":false}"#,
        )
        .await;
        let sink = sink_for(&url);

        let a = alert("balance dropped", &[]);
        let mut out = [Delivery::Pending];
        sink.send_each(std::slice::from_ref(&a), &mut out).await;

        assert_eq!(out[0], Delivery::Deferred);
        // The sink now holds the deadline, so the next alert defers too.
        assert!(sink.retry_at.lock().is_some());
    }

    #[tokio::test]
    async fn reports_a_bad_webhook_as_failed() {
        let (url, _request) = mock_server(
            "HTTP/1.1 401 Unauthorized",
            r#"{"message":"401: Unauthorized","code":0}"#,
        )
        .await;
        let sink = sink_for(&url);

        let a = alert("balance dropped", &[]);
        let mut out = [Delivery::Pending];
        sink.send_each(std::slice::from_ref(&a), &mut out).await;

        assert_eq!(out[0], Delivery::Failed);
    }

    #[test]
    fn clamps_the_message_and_metadata_to_discord_limits() {
        let long = "x".repeat(10_000);
        let metadata: Vec<(String, String)> = (0..40)
            .map(|i| (format!("key{i}"), format!("value{i}")))
            .collect();
        let a = Alert::for_rule(RuleId::new(Program::new("token_program"), "balance_drop"))
            .message(long)
            .metas(metadata.iter().map(|(k, v)| (k.clone(), v.clone())))
            .build();

        let embed = build_embed(&a);
        assert!(embed.description.chars().count() <= EMBED_DESCRIPTION_LIMIT);
        assert!(embed.fields.len() <= EMBED_FIELDS_LIMIT);
        for field in &embed.fields {
            assert!(field.name.chars().count() <= FIELD_NAME_LIMIT);
            assert!(field.value.chars().count() <= FIELD_VALUE_LIMIT);
            assert!(!field.name.is_empty());
            assert!(!field.value.is_empty());
        }
    }

    #[test]
    fn requires_a_credential() {
        let err = DiscordSink::build(SinkId::new("test-discord"), None, None, None, None, 10)
            .err()
            .expect("build should reject a sink without a credential");
        assert!(err.to_string().contains("needs a webhook_url"));
    }
}
