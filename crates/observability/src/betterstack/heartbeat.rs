use reqwest::Client;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{error, info};
use url::Url;

pub struct Heartbeat {
    pub url: Url,                   // betterstack heartbeat url to send heartbeats to
    pub interval_seconds: Duration, // interval at which to send heartbeats
    pub cancel_token: CancellationToken,
}

impl Heartbeat {
    pub fn new(
        heartbeat_url: &str,
        interval_seconds: Duration,
        cancel_token: CancellationToken,
    ) -> Self {
        let url = Url::parse(heartbeat_url).expect("Invalid BetterStack heartbeat URL");
        Self {
            url,
            interval_seconds,
            cancel_token,
        }
    }

    fn spawn_heartbeat_task(self, tracker: &TaskTracker) {
        let Self {
            url,
            interval_seconds,
            cancel_token,
        } = self;
        let mut tick = tokio::time::interval(interval_seconds);
        let http_client = Client::new();

        tracker.spawn(async move {
            info!(
                "Starting BetterStack heartbeat task with interval: {:?}",
                interval_seconds
            );
            loop {
                tokio::select! {
                    _ = cancel_token.cancelled() => {
                        info!("Cancellation requested, stopping heartbeat task");
                        break;
                    }
                    _ = tick.tick() => {
                        // Send heartbeat to BetterStack
                        if let Err(e) = Self::send_heartbeat(&url, &http_client).await {
                            error!("Error sending heartbeat to BetterStack: {}", e);
                        }
                    }

                }
            }
        });
    }

    pub fn start(self, tracker: &TaskTracker) {
        self.spawn_heartbeat_task(tracker);
    }

    async fn send_heartbeat(url: &Url, client: &Client) -> anyhow::Result<()> {
        let response = client.post(url.as_str()).send().await?;
        if !response.status().is_success() {
            anyhow::bail!(
                "Failed to send heartbeat to BetterStack: HTTP {}",
                response.status()
            );
        }
        info!("Sent heartbeat to BetterStack successfully");
        Ok(())
    }
}
