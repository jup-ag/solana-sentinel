use crate::liveness::Liveness;
use crate::prometheus::metrics::record_heartbeat_withheld;
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
    /// When set, a beat is withheld while any armed canary misses its sink.
    gate: Option<Liveness>,
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
            gate: None,
        }
    }

    /// Beat only while canaries reach their sinks: a broken alert path then stops the
    /// heartbeat and Betterstack pages, even when nothing in-process can deliver an alert.
    #[must_use]
    pub fn gated_on(mut self, liveness: Liveness) -> Self {
        self.gate = Some(liveness);
        self
    }

    fn spawn_heartbeat_task(self, tracker: &TaskTracker) {
        let Self {
            url,
            interval_seconds,
            cancel_token,
            gate,
        } = self;
        let mut tick = tokio::time::interval(interval_seconds);
        let http_client = Client::new();

        tracker.spawn(async move {
            info!(
                gated_on_canary = gate.is_some(),
                "Starting BetterStack heartbeat task with interval: {:?}", interval_seconds
            );
            loop {
                tokio::select! {
                    _ = cancel_token.cancelled() => {
                        info!("Cancellation requested, stopping heartbeat task");
                        break;
                    }
                    _ = tick.tick() => {
                        if !should_beat(gate.as_ref()) {
                            continue;
                        }
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

/// Whether the gate (if any) lets this beat through; logs and counts a withheld one.
fn should_beat(gate: Option<&Liveness>) -> bool {
    let Some(liveness) = gate else {
        return true;
    };
    let misses = liveness.canary().check(tokio::time::Instant::now());
    if misses.is_empty() {
        return true;
    }
    record_heartbeat_withheld();
    error!(
        missed = ?misses.iter().map(|m| format!("{} -> {}", m.route, m.sink)).collect::<Vec<_>>(),
        "Canary missed its sink; withholding the Betterstack heartbeat"
    );
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use domain::canary::{canary_routes, CanaryScope};
    use domain::programs::Program;
    use domain::rules::RuleId;
    use domain::sinks::SinkId;

    #[tokio::test(start_paused = true)]
    async fn heartbeat_withheld_while_a_canary_is_missing() {
        assert!(should_beat(None));
        let liveness = Liveness::new();
        assert!(should_beat(Some(&liveness)), "no canary armed: beat");
        let chat = SinkId::new("chat");
        liveness
            .canary()
            .set_routes(canary_routes([("lend", &[chat.clone()][..])]));
        let routes = liveness.canary().arm(
            RuleId::new(Program::new("health"), "canary"),
            Duration::from_secs(60),
            Duration::from_secs(60),
            CanaryScope::Route,
            &[],
            &[],
        );
        assert!(should_beat(Some(&liveness)), "grace after arming");
        tokio::time::advance(Duration::from_secs(121)).await;
        assert!(!should_beat(Some(&liveness)), "no canary delivered");
        let now = tokio::time::Instant::now();
        let seq = liveness.canary().emit(&routes[0].id, now);
        liveness.canary().delivered(seq, &chat, now);
        assert!(should_beat(Some(&liveness)));
    }
}
