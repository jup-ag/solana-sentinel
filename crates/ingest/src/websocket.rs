//! Solana PubSub (websocket) source for nodes without Geyser. Accounts come
//! from `accountSubscribe` / `programSubscribe`; transactions from
//! `logsSubscribe(mentions)` followed by `getTransaction`; slots from
//! `slotSubscribe`. Each update is tagged with the filter names it matches,
//! computed locally, so the dispatcher routes it exactly as the gRPC path would.

use crate::convert::{account_update, transaction_update};
use async_trait::async_trait;
use futures::stream::{BoxStream, SelectAll};
use futures::StreamExt;
use shipstern_core::{AccountFilter, Filters, MemcmpData, TransactionPrefilter};
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
use solana_sentinel::source::{RawUpdate, SourceUpdate, SLOTS_FILTER};
use solana_sentinel::StreamSource;
use solana_signature::Signature;
use solana_transaction_status_client_types::UiTransactionEncoding;
use std::collections::{HashSet, VecDeque};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};
use yellowstone_grpc_proto::geyser::SubscribeUpdateSlot;

/// `getTransaction` right after a log notification can miss; retry briefly.
const TX_FETCH_ATTEMPTS: u32 = 5;
const TX_FETCH_BACKOFF: Duration = Duration::from_millis(400);
const SEEN_SIGNATURES: usize = 4096;

pub struct WebsocketSource {
    url: String,
    rpc: Arc<RpcClient>,
    commitment: CommitmentConfig,
}

impl WebsocketSource {
    pub fn new(url: String, rpc: Arc<RpcClient>, commitment: &str) -> anyhow::Result<Self> {
        let commitment = match commitment {
            "confirmed" => CommitmentConfig::confirmed(),
            "finalized" => CommitmentConfig::finalized(),
            other => {
                anyhow::bail!("websocket commitment must be confirmed or finalized, got `{other}`")
            }
        };
        Ok(Self {
            url,
            rpc,
            commitment,
        })
    }
}

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

#[async_trait]
impl StreamSource for WebsocketSource {
    async fn run(
        self: Box<Self>,
        filters: Filters,
        mut filter_updates: watch::Receiver<Filters>,
        sink: mpsc::Sender<SourceUpdate>,
        cancel: CancellationToken,
    ) -> anyhow::Result<()> {
        let client = PubsubClient::new(&self.url).await?;
        info!(url = %self.url, "PubSub connected");

        let mut streams: SelectAll<BoxStream<'_, Event>> = SelectAll::new();
        let mut opened = Opened::default();
        let mut current = filters;
        self.subscribe(&client, &current, &mut opened, &mut streams)
            .await?;
        let mut seen: VecDeque<String> = VecDeque::with_capacity(SEEN_SIGNATURES);

        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                changed = filter_updates.changed() => {
                    if changed.is_err() {
                        continue;
                    }
                    current = filter_updates.borrow_and_update().clone();
                    if let Err(e) = self.subscribe(&client, &current, &mut opened, &mut streams).await {
                        warn!(error = %e, "Failed to apply filter update");
                    }
                }
                event = streams.next() => {
                    let Some(event) = event else {
                        // No subscriptions at all: nothing will ever arrive.
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        continue;
                    };
                    let Some(update) = self.convert(event, &current, &mut seen).await else {
                        continue;
                    };
                    if sink.send(update).await.is_err() {
                        break;
                    }
                }
            }
        }
        drop(streams);
        client.shutdown().await.ok();
        Ok(())
    }
}

impl WebsocketSource {
    async fn subscribe<'a>(
        &self,
        client: &'a PubsubClient,
        filters: &Filters,
        opened: &mut Opened,
        streams: &mut SelectAll<BoxStream<'a, Event>>,
    ) -> anyhow::Result<()> {
        let account_config = RpcAccountInfoConfig {
            encoding: Some(UiAccountEncoding::Base64),
            commitment: Some(self.commitment),
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
                                commitment: Some(self.commitment),
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
            if name == SLOTS_FILTER && !opened.slots {
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
        seen: &mut VecDeque<String>,
    ) -> Option<SourceUpdate> {
        match event {
            Event::Account {
                filter,
                pubkey,
                account,
                slot,
            } => Some(SourceUpdate {
                filters: vec![filter],
                update: RawUpdate::Account(account_update(&pubkey, &account, slot)?),
            }),
            Event::Slot(info) => Some(SourceUpdate {
                filters: vec![SLOTS_FILTER.to_string()],
                update: RawUpdate::Slot(SubscribeUpdateSlot {
                    slot: info.slot,
                    parent: Some(info.parent),
                    status: 0,
                    dead_error: None,
                }),
            }),
            Event::Logs { signature, failed } => {
                if seen.contains(&signature) {
                    return None;
                }
                if seen.len() == SEEN_SIGNATURES {
                    seen.pop_front();
                }
                seen.push_back(signature.clone());

                let converted = self.fetch_transaction(&signature).await?;
                let names: Vec<String> = filters
                    .parser_ids()
                    .filter(|name| {
                        filters
                            .get(name)
                            .and_then(|p| p.transaction.as_ref())
                            .is_some_and(|t| tx_matches(t, &converted.account_keys, failed))
                    })
                    .map(str::to_string)
                    .collect();
                if names.is_empty() {
                    return None;
                }
                debug!(%signature, filters = ?names, "PubSub transaction matched");
                Some(SourceUpdate {
                    filters: names,
                    update: RawUpdate::Transaction(Box::new(converted.update)),
                })
            }
        }
    }

    async fn fetch_transaction(
        &self,
        signature: &str,
    ) -> Option<crate::convert::ConvertedTransaction> {
        let sig = Signature::from_str(signature).ok()?;
        let config = RpcTransactionConfig {
            encoding: Some(UiTransactionEncoding::Base64),
            commitment: Some(self.commitment),
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

/// The same test yellowstone applies server-side, on the fetched transaction's keys.
fn tx_matches(filter: &TransactionPrefilter, keys: &[Pubkey], failed: bool) -> bool {
    if failed && filter.failed == Some(false) {
        return false;
    }
    let has = |k: &shipstern_core::Pubkey| keys.contains(&Pubkey::new_from_array(k.0));
    let included = filter.accounts_include.is_empty() || filter.accounts_include.iter().any(has);
    let required = filter.accounts_required.iter().all(has);
    let excluded = filter.accounts_exclude.iter().any(has);
    included && required && !excluded
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
