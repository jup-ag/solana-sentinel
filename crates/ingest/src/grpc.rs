//! Plain Yellowstone gRPC: one `subscribe` stream driven straight from the
//! framework's filter set, no shipstern runtime in between.

use async_trait::async_trait;
use futures::{SinkExt, StreamExt};
use shipstern_core::Filters;
use solana_sentinel::source::{RawUpdate, SourceUpdate};
use solana_sentinel::StreamSource;
use std::time::Duration;
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};
use yellowstone_grpc_client::GeyserGrpcClient;
use yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof;
use yellowstone_grpc_proto::geyser::{CommitmentLevel, SubscribeRequest, SubscribeRequestPing};
use yellowstone_grpc_proto::tonic::transport::ClientTlsConfig;

const RECONNECT_BACKOFF: Duration = Duration::from_secs(2);

pub struct GrpcSource {
    pub endpoint: String,
    pub x_token: Option<String>,
    pub timeout: Duration,
    /// Replay start for the first subscribe only; later requests must not repeat it.
    pub from_slot: Option<u64>,
}

impl GrpcSource {
    fn request(&self, filters: &Filters, first: bool) -> SubscribeRequest {
        let mut request = SubscribeRequest::from(filters.clone());
        request.commitment = Some(CommitmentLevel::Processed as i32);
        if first {
            request.from_slot = self.from_slot;
        }
        request
    }
}

#[async_trait]
impl StreamSource for GrpcSource {
    async fn run(
        self: Box<Self>,
        filters: Filters,
        mut filter_updates: watch::Receiver<Filters>,
        sink: mpsc::Sender<SourceUpdate>,
        cancel: CancellationToken,
    ) -> anyhow::Result<()> {
        let mut first = true;
        loop {
            if cancel.is_cancelled() {
                return Ok(());
            }
            let mut client = GeyserGrpcClient::build_from_shared(self.endpoint.clone())?
                .x_token(self.x_token.clone())?
                .connect_timeout(self.timeout)
                .timeout(self.timeout)
                .tls_config(ClientTlsConfig::new().with_native_roots())?
                .connect()
                .await?;

            let current = filter_updates.borrow_and_update().clone();
            let request = self.request(&current, first);
            first = false;
            let (mut req_sink, mut stream) = client.subscribe_with_request(Some(request)).await?;
            info!(endpoint = %self.endpoint, filters = ?filters.parser_ids().collect::<Vec<_>>(), "Yellowstone gRPC subscribed");

            loop {
                tokio::select! {
                    _ = cancel.cancelled() => return Ok(()),
                    changed = filter_updates.changed() => {
                        if changed.is_err() {
                            continue; // every handle dropped; keep streaming with the last set
                        }
                        let next = filter_updates.borrow_and_update().clone();
                        if let Err(e) = req_sink.send(self.request(&next, false)).await {
                            warn!(error = %e, "Failed to send filter update; reconnecting");
                            break;
                        }
                        info!("Sent filter update to the live subscription");
                    }
                    msg = stream.next() => match msg {
                        Some(Ok(update)) => {
                            let filters = update.filters;
                            let raw = match update.update_oneof {
                                Some(UpdateOneof::Account(a)) => RawUpdate::Account(a),
                                Some(UpdateOneof::Transaction(t)) => RawUpdate::Transaction(Box::new(t)),
                                Some(UpdateOneof::Slot(s)) => RawUpdate::Slot(s),
                                Some(UpdateOneof::Ping(_)) => {
                                    // Keep-alive: the server expects a ping back on the request stream.
                                    let _ = req_sink
                                        .send(SubscribeRequest {
                                            ping: Some(SubscribeRequestPing { id: 1 }),
                                            ..Default::default()
                                        })
                                        .await;
                                    continue;
                                }
                                _ => continue,
                            };
                            if sink.send(SourceUpdate { filters, update: raw }).await.is_err() {
                                info!("Dispatcher gone; stopping gRPC source");
                                return Ok(());
                            }
                        }
                        Some(Err(status)) => {
                            warn!(error = %status, "gRPC stream error; reconnecting");
                            break;
                        }
                        None => {
                            warn!("gRPC stream ended; reconnecting");
                            break;
                        }
                    }
                }
            }
            debug!("Reconnecting after {:?}", RECONNECT_BACKOFF);
            tokio::time::sleep(RECONNECT_BACKOFF).await;
        }
    }
}
