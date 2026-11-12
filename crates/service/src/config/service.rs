use anyhow::{Context, Result};
use config::{Config, File, FileFormat};
use domain::sinks::SinkId;
use observability::HeartbeatSettings;
use serde::Deserialize;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;
use url::Url;

#[derive(Debug, Clone, Deserialize)]
pub struct ServiceConfig {
    pub data_source: DataSourceConfig,
    pub rpc: RpcConfig,
    pub sinks: Vec<SinkConfig>,
    pub observability: ObservabilityConfig,
    pub redis: RedisConfig,
    /// Optional: cooldown gate layers. Both default to on.
    #[serde(default)]
    pub alerting: AlertingConfig,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct AlertingConfig {
    #[serde(default)]
    pub cooldown_gate: CooldownGateConfig,
}

/// Cooldown gate: the two alert dedup layers. A disabled layer is never consulted.
#[derive(Debug, Clone, Deserialize)]
pub struct CooldownGateConfig {
    /// Per-node: suppress a repeat of the same alert within its rule's `cooldown_period_seconds`.
    #[serde(default = "defaults::gate_enabled")]
    pub local_gate: bool,
    /// Fleet-wide: suppress an alert another node already claimed.
    #[serde(default = "defaults::gate_enabled")]
    pub fleet_gate: bool,
    /// Fleet gate claim TTL for rules without `cooldown_period_seconds`.
    #[serde(default = "defaults::fleet_gate_default_ttl_seconds")]
    pub fleet_gate_default_ttl_seconds: u64,
}

impl Default for CooldownGateConfig {
    fn default() -> Self {
        Self {
            local_gate: defaults::gate_enabled(),
            fleet_gate: defaults::gate_enabled(),
            fleet_gate_default_ttl_seconds: defaults::fleet_gate_default_ttl_seconds(),
        }
    }
}

/// Where the stream comes from. Every variant is a shipstern source running in its
/// runtime: `yellowstone_grpc` (production default), `websocket` (Solana PubSub, for
/// nodes without Geyser), `kafka` (janus raw topics).
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DataSourceConfig {
    YellowstoneGrpc(YellowstoneGrpcSourceConfig),
    Websocket(WebsocketSourceConfig),
    Kafka(KafkaSourceConfig),
}

/// Shipstern runtime buffer settings, shared by every source type.
#[derive(Debug, Clone, Deserialize)]
pub struct BufferSettings {
    /// Bounded channel between the source and the pipelines.
    #[serde(default = "defaults::sources_channel_size")]
    pub sources_channel_size: usize,
}

#[derive(Debug, Clone, Deserialize)]
pub struct YellowstoneGrpcSourceConfig {
    pub endpoint: String,
    pub token: Option<String>,
    #[serde(default = "defaults::grpc_timeout_seconds")]
    pub timeout_seconds: u64,
    /// Client-side reconnect with slot replay and dedup (default on).
    #[serde(default = "defaults::auto_reconnect")]
    pub auto_reconnect: bool,
    /// Largest gRPC message accepted. Unbounded, a backlog of large accounts (ProgramData)
    /// in the source channel can exhaust the pod's memory.
    #[serde(default = "defaults::max_decoding_message_size")]
    pub max_decoding_message_size: usize,
    #[serde(flatten)]
    pub buffer: BufferSettings,
}

#[derive(Debug, Clone, Deserialize)]
pub struct WebsocketSourceConfig {
    /// `wss://...` PubSub endpoint. Transactions are fetched over `[rpc]` on each log notification.
    pub url: String,
    /// `confirmed` (default) or `finalized`; `processed` cannot serve `getTransaction`.
    #[serde(default = "defaults::websocket_commitment")]
    pub commitment: String,
    #[serde(flatten)]
    pub buffer: BufferSettings,
}

#[derive(Debug, Clone, Deserialize)]
pub struct KafkaSourceConfig {
    /// Comma-separated `host:port` list (librdkafka `bootstrap.servers`).
    pub brokers: String,
    /// janus raw topics: `raw.<program>.accounts` / `raw.<program>.transactions`.
    pub topics: Vec<String>,
    /// Consumer group. Replicas that must each see every update need distinct groups.
    pub group_id: String,
    /// `latest` (default) or `earliest`.
    #[serde(default = "defaults::kafka_auto_offset_reset")]
    pub auto_offset_reset: String,
    /// Extra librdkafka properties, e.g. `security.protocol`, `sasl.mechanism`.
    #[serde(default)]
    pub properties: HashMap<String, String>,
    #[serde(flatten)]
    pub buffer: BufferSettings,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RpcConfig {
    pub url: Url,
    /// Extra headers on every RPC request, e.g. `x-identity` so a shared provider
    /// can attribute traffic, or an auth header.
    #[serde(default)]
    pub headers: HashMap<String, String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SinkConfig {
    Clickhouse(ClickhouseConfig),
    Slack(SlackConfig),
    Discord(DiscordConfig),
    Betterstack(BetterstackSinkConfig),
    /// JSON lines on stdout; for local runs.
    Stdout(StdoutSinkConfig),
}

impl SinkConfig {
    pub fn id(&self) -> &SinkId {
        match self {
            Self::Clickhouse(c) => &c.id,
            Self::Slack(s) => &s.id,
            Self::Discord(d) => &d.id,
            Self::Betterstack(b) => &b.id,
            Self::Stdout(s) => &s.id,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct StdoutSinkConfig {
    pub id: SinkId,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ClickhouseConfig {
    pub id: SinkId,
    pub url: Url,
    #[serde(default = "defaults::clickhouse_user")]
    pub user: String,
    #[serde(default)]
    pub password: String,
    #[serde(default = "defaults::clickhouse_database")]
    pub database: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SlackConfig {
    pub id: SinkId,
    pub bot_auth_token: String,
    pub channel: String,
    #[serde(default = "defaults::slack_timeout_seconds")]
    pub timeout_seconds: u64,
}

/// Discord: a channel that receives the alert.
///
/// Two ways to authenticate, mutually exclusive:
///   - `webhook_url`: a channel webhook (Server Settings → Integrations →
///     Webhooks). The token is part of the URL, so nothing else is needed.
///   - `bot_token` + `channel_id`: a bot token and the channel to post into.
#[derive(Clone, Deserialize)]
pub struct DiscordConfig {
    pub id: SinkId,
    #[serde(default)]
    pub webhook_url: Option<String>,
    #[serde(default)]
    pub bot_token: Option<String>,
    #[serde(default)]
    pub channel_id: Option<String>,
    #[serde(default = "defaults::discord_timeout_seconds")]
    pub timeout_seconds: u64,
}

// Hand-written so a Debug-printed config never shows the webhook URL (which
// embeds its token) or the bot token.
impl std::fmt::Debug for DiscordConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let redacted = |s: &Option<String>| s.as_ref().map(|_| "<redacted>");
        f.debug_struct("DiscordConfig")
            .field("id", &self.id)
            .field("webhook_url", &redacted(&self.webhook_url))
            .field("bot_token", &redacted(&self.bot_token))
            .field("channel_id", &self.channel_id)
            .field("timeout_seconds", &self.timeout_seconds)
            .finish()
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct ObservabilityConfig {
    pub server: ServerConfig,
    /// Optional Betterstack heartbeat; omit for local runs.
    pub betterstack: Option<BetterstackHeartbeatConfig>,
}

impl ObservabilityConfig {
    pub fn heartbeat(&self) -> Option<HeartbeatSettings<'_>> {
        self.betterstack.as_ref().map(|hb| HeartbeatSettings {
            url: hb.heartbeat_url.as_str(),
            interval: Duration::from_secs(hb.heartbeat_interval_seconds),
        })
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    pub bind_addr: SocketAddr,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BetterstackSinkConfig {
    pub id: SinkId,
    pub incident_api_url: String,
    pub incident_auth_token: String,
    pub escalation_policy_id: String,
    pub requester_email: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BetterstackHeartbeatConfig {
    pub heartbeat_url: String,
    pub heartbeat_interval_seconds: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RedisConfig {
    /// Per-pod sidecar Redis (slot checkpoint, local gate).
    pub redis_sidecar_url: String,
    /// Fleet-wide shared Redis cluster for the fleet gate. Optional: without it
    /// the fleet gate runs in-memory, i.e. dedups within this process only.
    pub redis_fleet_url: Option<String>,
}

impl ServiceConfig {
    pub fn load() -> Result<Self> {
        let path = std::env::var("CONFIG_FILE").unwrap_or_else(|_| "./config.toml".into());
        let cfg = Config::builder()
            .add_source(File::new(&path, FileFormat::Toml).required(true))
            .build()
            .and_then(Config::try_deserialize::<Self>)
            .with_context(|| format!("failed to load service config `{path}`"))?;
        Ok(cfg)
    }
}

mod defaults {
    pub fn grpc_timeout_seconds() -> u64 {
        120
    }
    pub fn auto_reconnect() -> bool {
        true
    }
    // The channel is bounded by count, not bytes: 10k updates of large accounts is
    // already gigabytes, so keep it small and let the stream apply backpressure.
    pub fn sources_channel_size() -> usize {
        10_000
    }
    // Above the 10 MiB account size limit plus framing.
    pub fn max_decoding_message_size() -> usize {
        16 * 1024 * 1024
    }

    pub fn clickhouse_user() -> String {
        "default".into()
    }
    pub fn clickhouse_database() -> String {
        "default".into()
    }

    pub fn slack_timeout_seconds() -> u64 {
        10
    }

    pub fn discord_timeout_seconds() -> u64 {
        10
    }

    pub fn websocket_commitment() -> String {
        "confirmed".into()
    }

    pub fn kafka_auto_offset_reset() -> String {
        "latest".into()
    }

    pub fn gate_enabled() -> bool {
        true
    }
    pub fn fleet_gate_default_ttl_seconds() -> u64 {
        10 * 60
    }
}
