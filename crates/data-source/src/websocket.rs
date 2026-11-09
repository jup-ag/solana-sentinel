//! Solana PubSub (websocket) source for nodes without Geyser. Accounts come
//! from `accountSubscribe` / `programSubscribe`; transactions from
//! `logsSubscribe(mentions)` followed by `getTransaction`; slots from
//! `slotSubscribe`. Each update is tagged with the filter names it matches,
//! computed locally, so shipstern's runtime routes it exactly as gRPC would.

use crate::convert::{account_update, transaction_update};
use crate::kafka::next_filters;
use crate::matching::{match_update, tx_matches};
use crate::rpc::build_rpc_client;
use anyhow::Context;
use async_trait::async_trait;
use futures::future::BoxFuture;
use futures::stream::{BoxStream, FuturesOrdered, SelectAll};
use futures::{FutureExt, StreamExt};
use serde::Deserialize;
use shipstern::sources::{
    FilterUpdateSource, FromConfig, SourceContext, SourceExitStatus, SourceTrait,
};
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
use std::collections::{HashMap, HashSet, VecDeque};
use std::str::FromStr;
use std::time::Duration;
use tracing::{debug, error, info, warn};
use utility::redact::{redact_url, redact_urls};
use yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof;
use yellowstone_grpc_proto::geyser::{SubscribeUpdate, SubscribeUpdateSlot};

/// `getTransaction` right after a log notification can miss; retry briefly.
const TX_FETCH_ATTEMPTS: u32 = 5;
const TX_FETCH_BACKOFF: Duration = Duration::from_millis(400);
const SEEN_SIGNATURES: usize = 4096;
/// `getTransaction` calls in flight at once. Fetched one at a time, a single slow or
/// not-yet-available transaction (up to ~4 s of retries) stalls every account, slot and
/// transaction behind it, and the stream falls further behind the chain each second.
/// Results are still delivered in notification order.
const MAX_TX_IN_FLIGHT: usize = 64;
/// How often the per-filter / per-mention counters are logged.
const STATS_INTERVAL: Duration = Duration::from_secs(60);

/// Per-interval counters, logged at `info` every [`STATS_INTERVAL`].
#[derive(Default)]
struct Stats {
    accounts: HashMap<String, u64>,
    transactions: HashMap<String, u64>,
    mentions: HashMap<Pubkey, u64>,
    notifications: u64,
    duplicates: u64,
    fetched: u64,
    dropped: u64,
    unmatched: u64,
    slots: u64,
    shed: u64,
    duplicates_accounts: u64,
    skipped_failed: u64,
    unmatched_accounts: u64,
    shed_mentions: HashMap<Pubkey, u64>,
    fetch_ms: u64,
    fetch_retried: u64,
}

fn top(counts: &HashMap<Pubkey, u64>, n: usize) -> Vec<String> {
    let mut top: Vec<_> = counts.iter().map(|(k, v)| (*v, *k)).collect();
    top.sort_unstable_by(|a, b| b.cmp(a));
    top.iter()
        .take(n)
        .map(|(n, k)| format!("{k}={n}"))
        .collect()
}

impl Stats {
    fn flush(&mut self, in_flight: usize) {
        let mut accounts: Vec<_> = self.accounts.iter().collect();
        accounts.sort();
        let mut transactions: Vec<_> = self.transactions.iter().collect();
        transactions.sort();
        info!(
            interval_secs = STATS_INTERVAL.as_secs(),
            slots = self.slots,
            log_notifications = self.notifications,
            duplicate_signatures = self.duplicates,
            skipped_failed = self.skipped_failed,
            tx_fetched = self.fetched,
            tx_dropped = self.dropped,
            tx_unmatched = self.unmatched,
            tx_shed = self.shed,
            account_duplicates = self.duplicates_accounts,
            account_unmatched = self.unmatched_accounts,
            fetch_avg_ms = self.fetch_ms.checked_div(self.fetched).unwrap_or(0),
            fetch_retried = self.fetch_retried,
            in_flight,
            accounts = ?accounts,
            transactions = ?transactions,
            top_mentions = ?top(&self.mentions, 15),
            "PubSub stats"
        );
        if self.shed > 0 {
            warn!(
                shed = self.shed,
                top_shed = ?top(&self.shed_mentions, 15),
                "PubSub transactions shed: notifications arrive faster than getTransaction keeps up"
            );
        }
        *self = Self::default();
    }
}

#[derive(Debug, Clone, Deserialize, clap::Args)]
pub struct WebsocketConfig {
    /// `wss://...` PubSub endpoint.
    #[arg(long)]
    pub url: String,
    /// JSON-RPC endpoint for `getTransaction` on each log notification.
    #[arg(long)]
    pub rpc_url: String,
    /// Sent on every `rpc_url` request.
    #[serde(default)]
    #[arg(skip)]
    pub rpc_headers: HashMap<String, String>,
    /// `confirmed` or `finalized`; `processed` cannot serve `getTransaction`.
    #[arg(long, default_value = "confirmed")]
    pub commitment: String,
}

pub struct WebsocketSource {
    config: WebsocketConfig,
    rpc: RpcClient,
}

impl std::fmt::Debug for WebsocketSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WebsocketSource")
            .field("config", &self.config)
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

    async fn run(&self, ctx: SourceContext) -> Result<SourceExitStatus, shipstern::Error> {
        let SourceContext {
            filters,
            tx,
            mut filter_updates,
            ..
        } = ctx;
        let commitment = self.commitment()?;
        let client = PubsubClient::new(&self.config.url).await.map_err(|e| {
            let msg = format!(
                "Failed to connect to PubSub at {}: {}",
                redact_url(&self.config.url),
                redact_urls(&e.to_string())
            );
            shipstern::Error::Other(msg.into())
        })?;
        info!(url = %redact_url(&self.config.url), "PubSub connected");

        // Accounts and slots never wait behind a transaction fetch: they have their own set.
        let mut streams = Streams::default();
        let mut opened = Opened::default();
        let mut current = filters;
        self.subscribe(&client, &current, commitment, &mut opened, &mut streams)
            .await
            .map_err(|e| shipstern::Error::Other(redact_urls(&format!("{e:#}")).into()))?;
        let mut seen: VecDeque<String> = VecDeque::with_capacity(SEEN_SIGNATURES);
        let mut pending: FuturesOrdered<BoxFuture<'_, Option<FetchedTx>>> = FuturesOrdered::new();
        let mut stats = Stats::default();
        let mut stats_tick = tokio::time::interval(STATS_INTERVAL);
        stats_tick.tick().await;
        let mut lost: Option<&'static str> = None;
        let mut recent = RecentAccounts::default();

        loop {
            tokio::select! {
                Some(next) = next_filters(&mut filter_updates) => {
                    current = next;
                    if let Err(e) = self.subscribe(&client, &current, commitment, &mut opened, &mut streams).await {
                        warn!(error = redact_urls(&format!("{e:#}")), "Failed to apply filter update");
                    }
                }
                Some(fetched) = pending.next(), if !pending.is_empty() => {
                    let Some(fetched) = fetched else {
                        stats.dropped += 1;
                        continue;
                    };
                    stats.fetched += 1;
                    stats.fetch_ms += fetched.elapsed_ms;
                    stats.fetch_retried += u64::from(fetched.attempts > 1);
                    let Some(update) = match_transaction(fetched, &current, &mut stats) else {
                        continue;
                    };
                    if tx.send(Ok(update)).await.is_err() {
                        break;
                    }
                }
                Some(event) = streams.state.next(), if !streams.state.is_empty() => {
                    if let Event::Ended(kind) = event {
                        lost = Some(kind);
                        break;
                    }
                    let Some(update) = convert_now(event, &current, &mut recent, &mut stats) else {
                        continue;
                    };
                    if tx.send(Ok(update)).await.is_err() {
                        break;
                    }
                }
                Some(event) = streams.logs.next(), if !streams.logs.is_empty() => {
                    let Event::Logs { mention, signature, failed } = event else {
                        if let Event::Ended(kind) = event {
                            lost = Some(kind);
                            break;
                        }
                        continue;
                    };
                    stats.notifications += 1;
                    *stats.mentions.entry(mention).or_default() += 1;
                    if failed && !accepts_failed(&current) {
                        stats.skipped_failed += 1;
                        continue;
                    }
                    if !remember(&mut seen, &signature) {
                        stats.duplicates += 1;
                        continue;
                    }
                    if pending.len() >= MAX_TX_IN_FLIGHT {
                        // Shed rather than queue: unread notifications pile up unbounded in
                        // the PubSub client, and every later update arrives later still.
                        stats.shed += 1;
                        *stats.shed_mentions.entry(mention).or_default() += 1;
                        continue;
                    }
                    pending.push_back(self.fetch_transaction(signature, commitment, failed).boxed());
                }
                _ = stats_tick.tick() => stats.flush(pending.len()),
            }
        }
        drop(pending);
        drop(streams);
        let closed = client.shutdown().await;
        let Some(kind) = lost else {
            return Ok(SourceExitStatus::ReceiverDropped);
        };
        // Nothing here reconnects: fail the source so the service restarts on a fresh connection.
        let reason = match closed {
            Ok(()) => "closed by the server".to_string(),
            Err(e) => redact_urls(&e.to_string()),
        };
        error!(
            first_ended = kind,
            subscriptions = opened.len(),
            %reason,
            "PubSub connection lost"
        );
        Err(shipstern::Error::Other(
            format!("PubSub connection lost: {reason}").into(),
        ))
    }
}

#[async_trait]
impl SourceTrait for WebsocketSource {
    async fn connect(&self, ctx: SourceContext) -> Result<SourceExitStatus, shipstern::Error> {
        self.run(ctx).await
    }
}

impl FromConfig for WebsocketSource {
    type Config = WebsocketConfig;

    fn from_config(config: Self::Config) -> Self {
        // Headers are validated at service startup, before any source is built.
        let rpc = build_rpc_client(&config.rpc_url, &config.rpc_headers)
            .expect("rpc headers were validated at startup");
        Self { config, rpc }
    }
}

impl FilterUpdateSource for WebsocketSource {}

/// What one PubSub stream yields, before conversion.
enum Event {
    Account {
        pubkey: Pubkey,
        account: solana_account_decoder_client_types::UiAccount,
        slot: u64,
    },
    Logs {
        /// The `mentions` address whose subscription delivered it.
        mention: Pubkey,
        signature: String,
        failed: bool,
    },
    Slot(SlotInfo),
    /// The stream of this subscription kind ended: the shared connection is gone.
    Ended(&'static str),
}

/// A one-item stream marking the end of a subscription stream.
fn ended(kind: &'static str) -> impl futures::Stream<Item = Event> {
    futures::stream::once(async move { Event::Ended(kind) })
}

/// A transaction fetched after its log notification, awaiting filter matching.
struct FetchedTx {
    signature: String,
    failed: bool,
    converted: crate::convert::ConvertedTransaction,
    elapsed_ms: u64,
    attempts: u32,
}

/// Record `signature`; false when it was already seen (several `mentions`
/// subscriptions notify the same transaction).
fn remember(seen: &mut VecDeque<String>, signature: &str) -> bool {
    if seen.iter().any(|s| s == signature) {
        return false;
    }
    if seen.len() == SEEN_SIGNATURES {
        seen.pop_front();
    }
    seen.push_back(signature.to_string());
    true
}

fn wrap(filters: Vec<String>, update_oneof: UpdateOneof) -> SubscribeUpdate {
    SubscribeUpdate {
        filters,
        update_oneof: Some(update_oneof),
        created_at: None,
    }
}

/// Last (slot, data hash) delivered per account, to drop the copy a second subscription
/// delivers for the same write.
#[derive(Default)]
struct RecentAccounts(HashMap<Pubkey, (u64, u64)>);

impl RecentAccounts {
    fn is_new(
        &mut self,
        pubkey: &Pubkey,
        slot: u64,
        data: &solana_account_decoder_client_types::UiAccountData,
    ) -> bool {
        use std::hash::{Hash, Hasher};
        let mut h = std::collections::hash_map::DefaultHasher::new();
        format!("{data:?}").hash(&mut h);
        let seen = (slot, h.finish());
        self.0.insert(*pubkey, seen) != Some(seen)
    }
}

/// Account and slot notifications: no RPC round trip needed.
fn convert_now(
    event: Event,
    filters: &Filters,
    recent: &mut RecentAccounts,
    stats: &mut Stats,
) -> Option<SubscribeUpdate> {
    match event {
        Event::Account {
            pubkey,
            account,
            slot,
        } => {
            let update = account_update(&pubkey, &account, slot)?;
            if !recent.is_new(&pubkey, slot, &account.data) {
                // The same write via `accountSubscribe` and `programSubscribe`.
                stats.duplicates_accounts += 1;
                return None;
            }
            let Some(update) =
                match_update(filters, wrap(Vec::new(), UpdateOneof::Account(update)))
            else {
                stats.unmatched_accounts += 1;
                return None;
            };
            for name in &update.filters {
                *stats.accounts.entry(name.clone()).or_default() += 1;
            }
            Some(update)
        }
        Event::Slot(info) => {
            stats.slots += 1;
            let names: Vec<String> = filters
                .parser_ids()
                .filter(|name| filters.get(name).is_some_and(|p| p.slot.is_some()))
                .map(str::to_string)
                .collect();
            Some(wrap(
                names,
                UpdateOneof::Slot(SubscribeUpdateSlot {
                    slot: info.slot,
                    parent: Some(info.parent),
                    status: 0,
                    dead_error: None,
                }),
            ))
        }
        Event::Logs { .. } | Event::Ended(_) => None,
    }
}

/// Tag a fetched transaction with the transaction filters it matches, against the
/// filters current when it is delivered.
fn match_transaction(
    fetched: FetchedTx,
    filters: &Filters,
    stats: &mut Stats,
) -> Option<SubscribeUpdate> {
    let FetchedTx {
        signature,
        failed,
        converted,
        ..
    } = fetched;
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
        stats.unmatched += 1;
        return None;
    }
    for name in &names {
        *stats.transactions.entry(name.clone()).or_default() += 1;
    }
    debug!(%signature, filters = ?names, "PubSub transaction matched");
    Some(wrap(names, UpdateOneof::Transaction(converted.update)))
}

/// Subscriptions already opened, so a filter update only adds new ones.
///
/// Keyed by the subscription's own parameters, never by pipeline: a PubSub server answers an
/// identical subscription with the subscription id it already gave, and the client then keeps
/// only the newest stream for that id, so the first pipeline's stream ends at once and its
/// rules go deaf. One subscription per distinct request; [`match_update`] tags each update
/// with every pipeline it matches, as yellowstone does.
#[derive(Default)]
struct Opened {
    accounts: HashSet<Pubkey>,
    owners: HashSet<(Pubkey, Vec<AccountFilter>)>,
    mentions: HashSet<Pubkey>,
    slots: bool,
}

impl Opened {
    /// Streams these subscriptions opened.
    fn len(&self) -> usize {
        self.accounts.len() + self.owners.len() + self.mentions.len() + usize::from(self.slots)
    }
}

/// The subscriptions `filters` need that are not open yet, one per distinct request.
#[derive(Debug, Default, PartialEq)]
struct Plan {
    accounts: Vec<Pubkey>,
    owners: Vec<(Pubkey, Vec<AccountFilter>)>,
    mentions: Vec<Pubkey>,
    slots: bool,
}

/// An owner subscription's identity: the owner plus the filters the server applies. Lamports
/// filters never reach the server (see [`rpc_filter`]); [`match_update`] applies them locally.
fn owner_key(owner: Pubkey, filters: &[AccountFilter]) -> (Pubkey, Vec<AccountFilter>) {
    let server_side = filters
        .iter()
        .filter(|f| !matches!(f, AccountFilter::Lamports(_)))
        .cloned()
        .collect();
    (owner, server_side)
}

fn plan(filters: &Filters, opened: &Opened) -> Plan {
    let mut plan = Plan::default();
    let mut accounts = HashSet::new();
    let mut owners = HashSet::new();
    let mut mentions = HashSet::new();
    for name in filters.parser_ids() {
        let Some(prefilter) = filters.get(name) else {
            continue;
        };
        if let Some(acc) = &prefilter.account {
            for key in &acc.accounts {
                let pubkey = Pubkey::new_from_array(key.0);
                if !opened.accounts.contains(&pubkey) && accounts.insert(pubkey) {
                    plan.accounts.push(pubkey);
                }
            }
            for owner in &acc.owners {
                let key = owner_key(Pubkey::new_from_array(owner.0), &acc.filters);
                if !opened.owners.contains(&key) && owners.insert(key.clone()) {
                    plan.owners.push(key);
                }
            }
        }
        if let Some(tx) = &prefilter.transaction {
            // `mentions` takes one address per subscription.
            for key in tx.accounts_include.iter().chain(&tx.accounts_required) {
                let pubkey = Pubkey::new_from_array(key.0);
                if !opened.mentions.contains(&pubkey) && mentions.insert(pubkey) {
                    plan.mentions.push(pubkey);
                }
            }
        }
        plan.slots |= prefilter.slot.is_some() && !opened.slots;
    }
    plan
}

/// Whether any transaction filter takes failed transactions; when none does, a failed
/// transaction's log notification is not worth a `getTransaction`.
fn accepts_failed(filters: &Filters) -> bool {
    filters.parser_ids().any(|name| {
        filters
            .get(name)
            .and_then(|p| p.transaction.as_ref())
            .is_some_and(|t| t.failed != Some(false))
    })
}

/// Open PubSub streams: account / program / slot updates, and log notifications whose
/// transactions still have to be fetched.
#[derive(Default)]
struct Streams<'a> {
    state: SelectAll<BoxStream<'a, Event>>,
    logs: SelectAll<BoxStream<'a, Event>>,
}

impl WebsocketSource {
    async fn subscribe<'a>(
        &self,
        client: &'a PubsubClient,
        filters: &Filters,
        commitment: CommitmentConfig,
        opened: &mut Opened,
        streams: &mut Streams<'a>,
    ) -> anyhow::Result<()> {
        let account_config = RpcAccountInfoConfig {
            encoding: Some(UiAccountEncoding::Base64),
            commitment: Some(commitment),
            ..Default::default()
        };

        let plan = plan(filters, opened);
        for pubkey in plan.accounts {
            let (stream, _unsub) = client
                .account_subscribe(&pubkey, Some(account_config.clone()))
                .await
                .with_context(|| format!("accountSubscribe failed for {pubkey}"))?;
            opened.accounts.insert(pubkey);
            streams.state.push(
                stream
                    .map(move |r| Event::Account {
                        pubkey,
                        account: r.value,
                        slot: r.context.slot,
                    })
                    .chain(ended("accountSubscribe"))
                    .boxed(),
            );
        }
        for (owner, account_filters) in plan.owners {
            let config = RpcProgramAccountsConfig {
                filters: Some(account_filters.iter().filter_map(rpc_filter).collect()),
                account_config: account_config.clone(),
                with_context: None,
                sort_results: None,
            };
            let (stream, _unsub) = client
                .program_subscribe(&owner, Some(config))
                .await
                .with_context(|| format!("programSubscribe failed for {owner}"))?;
            opened.owners.insert((owner, account_filters));
            streams.state.push(
                stream
                    .filter_map(|r| async move {
                        let pubkey = Pubkey::from_str(&r.value.pubkey).ok()?;
                        Some(Event::Account {
                            pubkey,
                            account: r.value.account,
                            slot: r.context.slot,
                        })
                    })
                    .chain(ended("programSubscribe"))
                    .boxed(),
            );
        }
        for pubkey in plan.mentions {
            let (stream, _unsub) = client
                .logs_subscribe(
                    RpcTransactionLogsFilter::Mentions(vec![pubkey.to_string()]),
                    RpcTransactionLogsConfig {
                        commitment: Some(commitment),
                    },
                )
                .await
                .with_context(|| format!("logsSubscribe failed for {pubkey}"))?;
            opened.mentions.insert(pubkey);
            streams.logs.push(
                stream
                    .map(move |r| Event::Logs {
                        mention: pubkey,
                        signature: r.value.signature,
                        failed: r.value.err.is_some(),
                    })
                    .chain(ended("logsSubscribe"))
                    .boxed(),
            );
        }
        if plan.slots {
            let (stream, _unsub) = client
                .slot_subscribe()
                .await
                .context("slotSubscribe failed")?;
            opened.slots = true;
            streams.state.push(
                stream
                    .map(Event::Slot)
                    .chain(ended("slotSubscribe"))
                    .boxed(),
            );
        }
        info!(
            accounts = opened.accounts.len(),
            owners = opened.owners.len(),
            mentions = opened.mentions.len(),
            "PubSub subscriptions open"
        );
        Ok(())
    }

    async fn fetch_transaction(
        &self,
        signature: String,
        commitment: CommitmentConfig,
        failed: bool,
    ) -> Option<FetchedTx> {
        let sig = Signature::from_str(&signature).ok()?;
        let started = std::time::Instant::now();
        let config = RpcTransactionConfig {
            encoding: Some(UiTransactionEncoding::Base64),
            commitment: Some(commitment),
            max_supported_transaction_version: Some(1),
        };
        for attempt in 1..=TX_FETCH_ATTEMPTS {
            match self.rpc.get_transaction_with_config(&sig, config).await {
                Ok(tx) => {
                    let converted = transaction_update(tx.slot, sig.as_ref(), &tx.transaction);
                    let Some(converted) = converted else {
                        warn!(%signature, "transaction decode failed; transaction dropped");
                        return None;
                    };
                    return Some(FetchedTx {
                        signature,
                        failed,
                        converted,
                        elapsed_ms: started.elapsed().as_millis() as u64,
                        attempts: attempt,
                    });
                }
                Err(e) if attempt < TX_FETCH_ATTEMPTS => {
                    debug!(%signature, attempt, error = %redact_urls(&e.to_string()), "getTransaction not ready; retrying");
                    tokio::time::sleep(TX_FETCH_BACKOFF * attempt).await;
                }
                Err(e) => {
                    warn!(%signature, error = %redact_urls(&e.to_string()), "getTransaction failed; transaction dropped");
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

#[cfg(test)]
mod tests {
    use super::*;
    use shipstern_core::{LamportsCmp, PrefilterBuilder};
    use solana_account_decoder_client_types::{UiAccount, UiAccountData};

    fn filters(entries: Vec<(&str, PrefilterBuilder)>) -> Filters {
        Filters::new(
            entries
                .into_iter()
                .map(|(name, b)| (name.to_string(), b.build().unwrap()))
                .collect(),
        )
    }

    fn ui_account(owner: &Pubkey, data: &[u8]) -> UiAccount {
        use base64::Engine;
        UiAccount {
            lamports: 1,
            data: UiAccountData::Binary(
                base64::engine::general_purpose::STANDARD.encode(data),
                UiAccountEncoding::Base64,
            ),
            owner: owner.to_string(),
            executable: false,
            rent_epoch: 0,
            space: Some(data.len() as u64),
        }
    }

    #[test]
    fn plan_opens_one_subscription_per_distinct_request() {
        // Two pipelines watching the same account, owner + filter and mention: a PubSub
        // server answers the second identical request with the first one's id, and the
        // client then ends the first stream. One request each.
        let account = Pubkey::new_unique();
        let owner = Pubkey::new_unique();
        let memcmp = AccountFilter::Memcmp {
            offset: 0,
            data: MemcmpData::Bytes(vec![1]),
        };
        let by_key = || PrefilterBuilder::default().accounts([account.to_bytes()]);
        let by_owner = || {
            PrefilterBuilder::default()
                .account_owners([owner.to_bytes()])
                .account_filters([memcmp.clone()])
        };
        let tx = || PrefilterBuilder::default().transaction_accounts_include([account.to_bytes()]);
        let f = filters(vec![
            ("a:acct", by_key()),
            ("b:acct", by_key()),
            ("a:owned", by_owner()),
            ("b:owned", by_owner()),
            ("a:tx", tx()),
            ("b:tx", tx()),
        ]);
        let plan = plan(&f, &Opened::default());
        assert_eq!(plan.accounts, vec![account]);
        assert_eq!(plan.owners, vec![(owner, vec![memcmp])]);
        assert_eq!(plan.mentions, vec![account]);
        assert!(!plan.slots);
    }

    #[test]
    fn plan_skips_what_is_open_and_ignores_lamports_in_owner_identity() {
        let owner = Pubkey::new_unique();
        let other = Pubkey::new_unique();
        let f = filters(vec![
            (
                "a",
                PrefilterBuilder::default()
                    .account_owners([owner.to_bytes()])
                    .account_filters([AccountFilter::Lamports(LamportsCmp::Gt(5))]),
            ),
            (
                "b",
                PrefilterBuilder::default().account_owners([owner.to_bytes()]),
            ),
            (
                "c",
                PrefilterBuilder::default().accounts([other.to_bytes()]),
            ),
            ("d", PrefilterBuilder::default().slots()),
        ]);
        // `a` and `b` send the same programSubscribe: lamports never reach the server.
        let first = plan(&f, &Opened::default());
        assert_eq!(first.owners, vec![(owner, vec![])]);
        assert!(first.slots);

        let mut opened = Opened::default();
        opened.owners.insert((owner, vec![]));
        opened.accounts.insert(other);
        opened.slots = true;
        assert_eq!(plan(&f, &opened), Plan::default());
    }

    #[test]
    fn account_update_is_tagged_with_every_matching_pipeline_once() {
        let account = Pubkey::new_unique();
        let owner = Pubkey::new_unique();
        let f = filters(vec![
            (
                "a:acct",
                PrefilterBuilder::default().accounts([account.to_bytes()]),
            ),
            (
                "b:owned",
                PrefilterBuilder::default().account_owners([owner.to_bytes()]),
            ),
            (
                "c:other",
                PrefilterBuilder::default().accounts([Pubkey::new_unique().to_bytes()]),
            ),
        ]);
        let mut recent = RecentAccounts::default();
        let mut stats = Stats::default();
        let event = || Event::Account {
            pubkey: account,
            account: ui_account(&owner, &[7, 7]),
            slot: 10,
        };
        let update = convert_now(event(), &f, &mut recent, &mut stats).unwrap();
        let mut names = update.filters;
        names.sort();
        assert_eq!(names, vec!["a:acct".to_string(), "b:owned".to_string()]);

        // The same write again, as the programSubscribe copy of an accountSubscribe update.
        assert!(convert_now(event(), &f, &mut recent, &mut stats).is_none());
        assert_eq!(stats.duplicates_accounts, 1);

        // A later write goes through.
        let later = Event::Account {
            pubkey: account,
            account: ui_account(&owner, &[7, 8]),
            slot: 11,
        };
        assert!(convert_now(later, &f, &mut recent, &mut stats).is_some());
    }

    #[test]
    fn remember_dedupes_signatures_and_evicts_the_oldest() {
        let mut seen = VecDeque::new();
        assert!(remember(&mut seen, "a"));
        assert!(!remember(&mut seen, "a"));
        for i in 0..SEEN_SIGNATURES {
            remember(&mut seen, &i.to_string());
        }
        assert_eq!(seen.len(), SEEN_SIGNATURES);
        assert!(remember(&mut seen, "a"), "evicted, so new again");
    }

    #[test]
    fn failed_transactions_are_fetched_only_when_a_filter_takes_them() {
        let k = Pubkey::new_unique().to_bytes();
        let default_tx = filters(vec![(
            "a",
            PrefilterBuilder::default().transaction_accounts_include([k]),
        )]);
        assert!(
            !accepts_failed(&default_tx),
            "transaction prefilters default to failed = false"
        );
        let none = filters(vec![("s", PrefilterBuilder::default().slots())]);
        assert!(!accepts_failed(&none));
    }
}
