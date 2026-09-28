//! Kafka source for janus raw topics (github.com/jup-ag/janus). Each
//! `raw.<program>.accounts` record is a `SubscribeUpdateAccount` and each
//! `raw.<program>.transactions` record a `SubscribeUpdateTransaction`, optionally
//! behind a Confluent schema-registry prefix. Records carry no filter names, so
//! each is matched locally; slot updates are synthesized from record slots.

use crate::matching::match_update;
use async_trait::async_trait;
use prost::Message;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message as _;
use serde::Deserialize;
use shipstern::sources::{FilterUpdateSource, SourceExitStatus, SourceTrait};
use shipstern_core::Filters;
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::mpsc::Sender;
use tokio::sync::{oneshot, watch};
use tracing::{debug, info, warn};
use yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof;
use yellowstone_grpc_proto::geyser::{
    SubscribeUpdate, SubscribeUpdateAccount, SubscribeUpdateSlot, SubscribeUpdateTransaction,
};
use yellowstone_grpc_proto::tonic::Status;

const RECV_ERROR_BACKOFF: Duration = Duration::from_secs(1);
const CONFLUENT_MAGIC: u8 = 0x00;
const CONFLUENT_PREFIX_LEN: usize = 5;

#[derive(Debug, Clone, Deserialize, clap::Args)]
pub struct KafkaConfig {
    /// Comma-separated `host:port` list (librdkafka `bootstrap.servers`).
    #[arg(long)]
    pub brokers: String,
    #[arg(long)]
    pub group_id: String,
    /// janus raw topics; the `.accounts` / `.transactions` suffix picks the decoder.
    #[arg(long)]
    pub topics: Vec<String>,
    /// `latest` or `earliest`.
    #[arg(long, default_value = "latest")]
    pub auto_offset_reset: String,
    /// Extra librdkafka properties (`security.protocol`, `sasl.*`, ...), applied last.
    #[arg(skip)]
    #[serde(default)]
    pub properties: HashMap<String, String>,
}

#[derive(Debug)]
pub struct KafkaSource {
    config: KafkaConfig,
    filters: Filters,
}

impl KafkaSource {
    fn consumer(&self) -> anyhow::Result<StreamConsumer> {
        let c = &self.config;
        anyhow::ensure!(
            !c.topics.is_empty(),
            "kafka source needs at least one topic"
        );
        let mut config = ClientConfig::new();
        config
            .set("bootstrap.servers", &c.brokers)
            .set("group.id", &c.group_id)
            .set("auto.offset.reset", &c.auto_offset_reset)
            .set("enable.auto.commit", "true")
            .set("enable.partition.eof", "false");
        for (k, v) in &c.properties {
            config.set(k, v);
        }
        let consumer: StreamConsumer = config.create()?;
        let topics: Vec<&str> = c.topics.iter().map(String::as_str).collect();
        consumer.subscribe(&topics)?;
        Ok(consumer)
    }

    async fn run(
        &self,
        tx: Sender<Result<SubscribeUpdate, Status>>,
        status_tx: oneshot::Sender<SourceExitStatus>,
        mut filter_updates: Option<watch::Receiver<Filters>>,
    ) -> Result<(), shipstern::Error> {
        let consumer = match self.consumer() {
            Ok(c) => c,
            Err(e) => {
                let _ = status_tx.send(SourceExitStatus::Error(e.to_string()));
                return Err(shipstern::Error::Other(e.into()));
            }
        };
        info!(
            brokers = %self.config.brokers,
            topics = ?self.config.topics,
            group = %self.config.group_id,
            filters = ?self.filters.parser_ids().collect::<Vec<_>>(),
            "Kafka consumer subscribed"
        );
        let mut current = self.filters.clone();
        let mut last_slot = 0u64;

        loop {
            tokio::select! {
                Some(next) = next_filters(&mut filter_updates) => {
                    current = next;
                    info!("Applied filter update to local Kafka matching");
                }
                msg = consumer.recv() => {
                    let msg = match msg {
                        Ok(m) => m,
                        Err(e) => {
                            // librdkafka reconnects on its own; just avoid a hot loop.
                            warn!(error = %e, "Kafka receive failed");
                            tokio::time::sleep(RECV_ERROR_BACKOFF).await;
                            continue;
                        }
                    };
                    let Some(payload) = msg.payload() else { continue };
                    let update = match decode_record(msg.topic(), payload) {
                        Ok(u) => u,
                        Err(e) => {
                            debug!(topic = msg.topic(), offset = msg.offset(), error = %e, "Kafka record skipped");
                            continue;
                        }
                    };
                    let slot = update_slot(&update);
                    if let Some(update) = match_update(&current, update) {
                        if tx.send(Ok(update)).await.is_err() {
                            break;
                        }
                    }
                    // No slot topic: advance the slot feed as records reveal new slots.
                    if slot > last_slot {
                        last_slot = slot;
                        let slot_update = SubscribeUpdate {
                            filters: Vec::new(),
                            update_oneof: Some(UpdateOneof::Slot(SubscribeUpdateSlot { slot, parent: None, status: 0, dead_error: None })),
                            created_at: None,
                        };
                        if let Some(update) = match_update(&current, slot_update) {
                            if tx.send(Ok(update)).await.is_err() {
                                break;
                            }
                        }
                    }
                }
            }
        }
        info!("Runtime gone; stopping Kafka source");
        let _ = status_tx.send(SourceExitStatus::ReceiverDropped);
        Ok(())
    }
}

/// Yield the newest filter set, or pend forever when the runtime never sends any.
pub(crate) async fn next_filters(rx: &mut Option<watch::Receiver<Filters>>) -> Option<Filters> {
    let Some(rx) = rx else {
        return std::future::pending().await;
    };
    if rx.changed().await.is_err() {
        return std::future::pending().await;
    }
    Some(rx.borrow_and_update().clone())
}

#[async_trait]
impl SourceTrait for KafkaSource {
    type Config = KafkaConfig;

    fn new(config: Self::Config, filters: Filters) -> Self {
        Self { config, filters }
    }

    async fn connect(
        &self,
        tx: Sender<Result<SubscribeUpdate, Status>>,
        status_tx: oneshot::Sender<SourceExitStatus>,
    ) -> Result<(), shipstern::Error> {
        self.run(tx, status_tx, None).await
    }

    async fn connect_with_filter_updates(
        &self,
        tx: Sender<Result<SubscribeUpdate, Status>>,
        status_tx: oneshot::Sender<SourceExitStatus>,
        filter_updates_rx: watch::Receiver<Filters>,
    ) -> Result<(), shipstern::Error> {
        self.run(tx, status_tx, Some(filter_updates_rx)).await
    }
}

impl FilterUpdateSource for KafkaSource {}

/// Decode by topic kind; the body is the bare protobuf janus publishes.
fn decode_record(topic: &str, payload: &[u8]) -> anyhow::Result<SubscribeUpdate> {
    let body = strip_confluent_prefix(payload)?;
    let update_oneof = if topic.ends_with(".accounts") {
        UpdateOneof::Account(SubscribeUpdateAccount::decode(body)?)
    } else if topic.ends_with(".transactions") {
        UpdateOneof::Transaction(SubscribeUpdateTransaction::decode(body)?)
    } else {
        anyhow::bail!("topic is neither a janus accounts nor transactions topic");
    };
    Ok(SubscribeUpdate {
        filters: Vec::new(),
        update_oneof: Some(update_oneof),
        created_at: None,
    })
}

/// Confluent protobuf framing: magic 0x00, u32 schema id, zigzag-varint message
/// index list (a single 0x00 means "first message"). Absent on plain records.
fn strip_confluent_prefix(payload: &[u8]) -> anyhow::Result<&[u8]> {
    if payload.first() != Some(&CONFLUENT_MAGIC) || payload.len() < CONFLUENT_PREFIX_LEN + 1 {
        return Ok(payload);
    }
    let mut rest = &payload[CONFLUENT_PREFIX_LEN..];
    let count = read_zigzag(&mut rest)?;
    anyhow::ensure!(count >= 0, "negative message-index count");
    for _ in 0..count {
        read_zigzag(&mut rest)?;
    }
    Ok(rest)
}

fn read_zigzag(buf: &mut &[u8]) -> anyhow::Result<i64> {
    let raw = prost::encoding::decode_varint(buf)?;
    Ok(((raw >> 1) as i64) ^ -((raw & 1) as i64))
}

fn update_slot(update: &SubscribeUpdate) -> u64 {
    match &update.update_oneof {
        Some(UpdateOneof::Account(a)) => a.slot,
        Some(UpdateOneof::Transaction(t)) => t.slot,
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_plain_and_confluent_framed_account_records() {
        let account = SubscribeUpdateAccount {
            account: None,
            slot: 42,
            is_startup: false,
        };
        let body = account.encode_to_vec();
        let plain = decode_record("raw.prog.accounts", &body).unwrap();
        assert_eq!(update_slot(&plain), 42);

        // magic, schema id 7, index list shortcut 0x00
        let mut framed = vec![0x00, 0, 0, 0, 7, 0x00];
        framed.extend_from_slice(&body);
        let unframed = decode_record("raw.prog.accounts", &framed).unwrap();
        assert_eq!(update_slot(&unframed), 42);

        // explicit index list [1]: count 1 (zigzag 0x02), index 1 (zigzag 0x02)
        let mut framed = vec![0x00, 0, 0, 0, 7, 0x02, 0x02];
        framed.extend_from_slice(&body);
        assert_eq!(
            update_slot(&decode_record("raw.prog.accounts", &framed).unwrap()),
            42
        );
    }
}
