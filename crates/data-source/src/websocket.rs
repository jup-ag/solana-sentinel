//! Solana PubSub (websocket) source for nodes without Geyser. Accounts come
//! from `accountSubscribe` / `programSubscribe`; transactions from
//! `logsSubscribe(mentions)` followed by `getTransaction`; slots from
//! `slotSubscribe`. Each update is tagged with the filter names it matches,
//! computed locally, so shipstern's runtime routes it exactly as gRPC would.

use crate::convert::{account_update, transaction_update};
use crate::kafka::next_filters;
use crate::matching::tx_matches;
use async_trait::async_trait;
use futures::stream::{BoxStream, SelectAll};
use futures::StreamExt;
use serde::Deserialize;
use shipstern::sources::{FilterUpdateSource, SourceExitStatus, SourceTrait};
use shipstern_core::{AccountFilter, Filters, MemcmpData};
use solana_account_decoder_client_types::UiAccountEncoding;
use solana_commitment_config::CommitmentConfig;
use solana_pubkey::Pubkey;
use solana_pubsub_client::nonblocking::pubsub_client::PubsubClient;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_rpc_client_api::config::{
    RpcAccountInfoConfig, RpcProgramAccountsConfig, RpcTransactionConfig, RpcTransactionLogsConfig,
    RpcTransactionLogsFilter,
};
use solana_rpc_client_api::filter::{Memcmp, RpcFilterType};
use solana_rpc_client_api::response::SlotInfo;
use solana_signature::Signature;
use solana_transaction_status_client_types::UiTransactionEncoding;
use std::collections::{HashSet, VecDeque};
use std::str::FromStr;
use std::time::Duration;
use tokio::sync::mpsc::Sender;
use tokio::sync::{oneshot, watch};
use tracing::{debug, info, warn};
use yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof;
use yellowstone_grpc_proto::geyser::{SubscribeUpdate, SubscribeUpdateSlot};
use yellowstone_grpc_proto::tonic::Status;

/// `getTransaction` right after a log notification can miss; retry briefly.
const TX_FETCH_ATTEMPTS: u32 = 5;
const TX_FETCH_BACKOFF: Duration = Duration::from_millis(400);
const SEEN_SIGNATURES: usize = 4096;

#[derive(Debug, Clone, Deserialize, clap::Args)]
pub struct WebsocketConfig {
    /// `wss://...` PubSub endpoint.
    #[arg(long)]
    pub url: String,
    /// JSON-RPC endpoint for `getTransaction` on each log notification.
    #[arg(long)]
    pub rpc_url: String,
    /// `confirmed` or `finalized`; `processed` cannot serve `getTransaction`.
    #[arg(long, default_value = "confirmed")]
    pub commitment: String,
}

pub struct WebsocketSource {
    config: WebsocketConfig,
    filters: Filters,
    rpc: RpcClient,
}

impl std::fmt::Debug for WebsocketSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WebsocketSource")
            .field("config", &self.config)
            .field("filters", &self.filters.parser_ids().collect::<Vec<_>>())
            .finish_non_exhaustive()
    }
}

impl WebsocketSource {
    fn commitment(&self) -> Result<CommitmentConfig, shipstern::Error> {
        match self.config.commitment.as_str() {
            "confirmed" => Ok(CommitmentConfig::confirmed()),
            "finalized" => Ok(CommitmentConfig::finalized()),
            other => Err(shipstern::Error::Other(
                format!("websocket commitment must be confirmed or finalized, got `{other}`")
                    .into(),
            )),
        }
    }

    async fn run(
        &self,
        tx: Sender<Result<SubscribeUpdate, Status>>,
        status_tx: oneshot::Sender<SourceExitStatus>,
        mut filter_updates: Option<watch::Receiver<Filters>>,
    ) -> Result<(), shipstern::Error> {
        let commitment = self.commitment()?;
        let client = match PubsubClient::new(&self.config.url).await {
            Ok(c) => c,
            Err(e) => {
                let _ = status_tx.send(SourceExitStatus::Error(e.to_string()));
                return Err(shipstern::Error::Other(Box::new(e)));
            }
        };
        info!(url = %self.config.url, "PubSub connected");

        let mut streams: SelectAll<BoxStream<'_, Event>> = SelectAll::new();
        let mut opened = Opened::default();
        let mut current = self.filters.clone();
        if let Err(e) = self
            .subscribe(&client, &current, commitment, &mut opened, &mut streams)
            .await
        {
            let _ = status_tx.send(SourceExitStatus::Error(e.to_string()));
            return Err(shipstern::Error::Other(e.into()));
        }
        let mut seen: VecDeque<String> = VecDeque::with_capacity(SEEN_SIGNATURES);

        loop {
            tokio::select! {
                Some(next) = next_filters(&mut filter_updates) => {
                    current = next;
                    if let Err(e) = self.subscribe(&client, &current, commitment, &mut opened, &mut streams).await {
                        warn!(error = %e, "Failed to apply filter update");
                    }
                }
                event = streams.next() => {
                    let Some(event) = event else {
                        // No subscriptions at all: nothing will ever arrive.
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        continue;
                    };
                    let Some(update) = self.convert(event, &current, commitment, &mut seen).await else {
                        continue;
                    };
                    if tx.send(Ok(update)).await.is_err() {
                        break;
                    }
                }
            }
        }
        drop(streams);
        client.shutdown().await.ok();
        let _ = status_tx.send(SourceExitStatus::ReceiverDropped);
        Ok(())
    }
}

#[async_trait]
impl SourceTrait for WebsocketSource {
    type Config = WebsocketConfig;

    fn new(config: Self::Config, filters: Filters) -> Self {
        let rpc = RpcClient::new(config.rpc_url.clone());
        Self {
            config,
            filters,
            rpc,
        }
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

impl FilterUpdateSource for WebsocketSource {}

/// What one PubSub stream yields, before conversion.
enum Event {
    Account {
        filter: String,
        pubkey: Pubkey,
        account: solana_account_decoder_client_types::UiAccount,
        slot: u64,
    },
    Logs {
        signature: String,
        failed: bool,
    },
    Slot(SlotInfo),
}

/// Subscriptions already opened, so a filter update only adds new ones.
#[derive(Default)]
struct Opened {
    accounts: HashSet<(String, Pubkey)>,
    owners: HashSet<(String, Pubkey)>,
    mentions: HashSet<Pubkey>,
    slots: bool,
}

impl WebsocketSource {
    async fn subscribe<'a>(
        &self,
        client: &'a PubsubClient,
        filters: &Filters,
        commitment: CommitmentConfig,
        opened: &mut Opened,
        streams: &mut SelectAll<BoxStream<'a, Event>>,
    ) -> anyhow::Result<()> {
        let account_config = RpcAccountInfoConfig {
            encoding: Some(UiAccountEncoding::Base64),
            commitment: Some(commitment),
            ..Default::default()
        };

        for name in filters.parser_ids() {
            let Some(prefilter) = filters.get(name) else {
                continue;
            };
            if let Some(acc) = &prefilter.account {
                for key in &acc.accounts {
                    let pubkey = Pubkey::new_from_array(key.0);
                    if !opened.accounts.insert((name.to_string(), pubkey)) {
                        continue;
                    }
                    let (stream, _unsub) = client
                        .account_subscribe(&pubkey, Some(account_config.clone()))
                        .await?;
                    let filter = name.to_string();
                    streams.push(
                        stream
                            .map(move |r| Event::Account {
                                filter: filter.clone(),
                                pubkey,
                                account: r.value,
                                slot: r.context.slot,
                            })
                            .boxed(),
                    );
                }
                for owner in &acc.owners {
                    let owner = Pubkey::new_from_array(owner.0);
                    if !opened.owners.insert((name.to_string(), owner)) {
                        continue;
                    }
                    let config = RpcProgramAccountsConfig {
                        filters: Some(acc.filters.iter().filter_map(rpc_filter).collect()),
                        account_config: account_config.clone(),
                        with_context: None,
                        sort_results: None,
                    };
                    let (stream, _unsub) = client.program_subscribe(&owner, Some(config)).await?;
                    let filter = name.to_string();
                    streams.push(
                        stream
                            .filter_map(move |r| {
                                let filter = filter.clone();
                                async move {
                                    let pubkey = Pubkey::from_str(&r.value.pubkey).ok()?;
                                    Some(Event::Account {
                                        filter,
                                        pubkey,
                                        account: r.value.account,
                                        slot: r.context.slot,
                                    })
                                }
                            })
                            .boxed(),
                    );
                }
            }
            if let Some(tx) = &prefilter.transaction {
                // `mentions` takes one address per subscription.
                for key in tx.accounts_include.iter().chain(&tx.accounts_required) {
                    let pubkey = Pubkey::new_from_array(key.0);
                    if !opened.mentions.insert(pubkey) {
                        continue;
                    }
                    let (stream, _unsub) = client
                        .logs_subscribe(
                            RpcTransactionLogsFilter::Mentions(vec![pubkey.to_string()]),
                            RpcTransactionLogsConfig {
                                commitment: Some(commitment),
                            },
                        )
                        .await?;
                    streams.push(
                        stream
                            .map(|r| Event::Logs {
                                signature: r.value.signature,
                                failed: r.value.err.is_some(),
                            })
                            .boxed(),
                    );
                }
            }
            if prefilter.slot.is_some() && !opened.slots {
                opened.slots = true;
                let (stream, _unsub) = client.slot_subscribe().await?;
                streams.push(stream.map(Event::Slot).boxed());
            }
        }
        info!(
            accounts = opened.accounts.len(),
            owners = opened.owners.len(),
            mentions = opened.mentions.len(),
            "PubSub subscriptions open"
        );
        Ok(())
    }

    async fn convert(
        &self,
        event: Event,
        filters: &Filters,
        commitment: CommitmentConfig,
        seen: &mut VecDeque<String>,
    ) -> Option<SubscribeUpdate> {
        let update = |filters: Vec<String>, update_oneof| SubscribeUpdate {
            filters,
            update_oneof: Some(update_oneof),
            created_at: None,
        };
        match event {
            Event::Account {
                filter,
                pubkey,
                account,
                slot,
            } => Some(update(
                vec![filter],
                UpdateOneof::Account(account_update(&pubkey, &account, slot)?),
            )),
            Event::Slot(info) => {
                let names: Vec<String> = filters
                    .parser_ids()
                    .filter(|name| filters.get(name).is_some_and(|p| p.slot.is_some()))
                    .map(str::to_string)
                    .collect();
                Some(update(
                    names,
                    UpdateOneof::Slot(SubscribeUpdateSlot {
                        slot: info.slot,
                        parent: Some(info.parent),
                        status: 0,
                        dead_error: None,
                    }),
                ))
            }
            Event::Logs { signature, failed } => {
                if seen.contains(&signature) {
                    return None;
                }
                if seen.len() == SEEN_SIGNATURES {
                    seen.pop_front();
                }
                seen.push_back(signature.clone());

                let converted = self.fetch_transaction(&signature, commitment).await?;
                let names: Vec<String> = filters
                    .parser_ids()
                    .filter(|name| {
                        filters
                            .get(name)
                            .and_then(|p| p.transaction.as_ref())
                            .is_some_and(|t| tx_matches(t, &converted.account_keys, failed, false))
                    })
                    .map(str::to_string)
                    .collect();
                if names.is_empty() {
                    return None;
                }
                debug!(%signature, filters = ?names, "PubSub transaction matched");
                Some(update(names, UpdateOneof::Transaction(converted.update)))
            }
        }
    }

    async fn fetch_transaction(
        &self,
        signature: &str,
        commitment: CommitmentConfig,
    ) -> Option<crate::convert::ConvertedTransaction> {
        let sig = Signature::from_str(signature).ok()?;
        let config = RpcTransactionConfig {
            encoding: Some(UiTransactionEncoding::Base64),
            commitment: Some(commitment),
            max_supported_transaction_version: Some(0),
        };
        for attempt in 1..=TX_FETCH_ATTEMPTS {
            match self.rpc.get_transaction_with_config(&sig, config).await {
                Ok(tx) => return transaction_update(tx.slot, sig.as_ref(), &tx.transaction),
                Err(e) if attempt < TX_FETCH_ATTEMPTS => {
                    debug!(%signature, attempt, error = %e, "getTransaction not ready; retrying");
                    tokio::time::sleep(TX_FETCH_BACKOFF * attempt).await;
                }
                Err(e) => {
                    warn!(%signature, error = %e, "getTransaction failed; transaction dropped");
                    return None;
                }
            }
        }
        None
    }
}

fn rpc_filter(f: &AccountFilter) -> Option<RpcFilterType> {
    Some(match f {
        AccountFilter::Memcmp { offset, data } => {
            let bytes = match data {
                MemcmpData::Bytes(b) => b.clone(),
                MemcmpData::Base58(s) => bs58::decode(s).into_vec().ok()?,
                MemcmpData::Base64(s) => crate::convert::decode_base64(s)?,
            };
            RpcFilterType::Memcmp(Memcmp::new_raw_bytes(*offset as usize, bytes))
        }
        AccountFilter::DataSize(n) => RpcFilterType::DataSize(*n),
        AccountFilter::TokenAccountState(_) => RpcFilterType::TokenAccountState,
        AccountFilter::Lamports(_) => return None,
    })
}
