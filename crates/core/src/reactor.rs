//! Reactors: rules fed by other rules' alerts instead of a parser or a poll.
//!
//! A reactor sees every alert the moment a rule raises it, before the cooldown and fleet
//! gates and before the alerting service batches it, so a response (simulating or sending a
//! pause, paging a different channel, enriching with an RPC read) starts within microseconds
//! of detection. What a reactor returns is routed like any alert of its own rule id.
//!
//! The tap ([`spawn_alert_tap`]) sits between the rule handlers and the alerting service:
//! it hands each alert to the reactors that `accept` it (each reaction runs in its own task)
//! and forwards the alert unchanged. Reactor output goes straight to the alerting service and
//! is never fed back to reactors, so a reactor cannot loop on its own alerts.

use crate::deps::Deps;
use crate::rule::RuleConfigError;
use crate::BootstrapCtx;
use async_trait::async_trait;
use config::Value;
use domain::alert::Alert;
use domain::programs::Program;
use domain::rules::RuleId;
use futures::future::BoxFuture;
use serde::de::DeserializeOwned;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{error, info, warn};

/// What the tap knew when it handed an alert to a reactor.
#[derive(Debug, Clone, Copy)]
pub struct ReactCtx {
    /// When the tap received the alert (rule raised it → channel → tap is microseconds).
    pub seen_at: Instant,
    /// Slot of the stream event that raised the alert; `None` for poll and sweep alerts.
    pub trigger_slot: Option<u64>,
}

/// A rule that reacts to other rules' alerts. Registered with `ProgramSpec::reactor`,
/// configured and routed in `rules.toml` like any rule.
#[async_trait]
pub trait ReactorRule: Sized + Send + Sync + 'static {
    const ID: RuleId;
    type Config: DeserializeOwned + Send + Sync + 'static;

    fn build(cfg: Self::Config, deps: &Deps) -> anyhow::Result<Self>;

    /// Seed state from RPC. Runs after the deps bootstrap, before any alert is delivered.
    async fn bootstrap(&mut self, _ctx: &BootstrapCtx) -> anyhow::Result<()> {
        Ok(())
    }

    /// How often to call [`Self::refresh`]; `None` never.
    fn refresh_interval(_cfg: &Self::Config) -> Option<Duration> {
        None
    }

    /// Re-read chain state (pre-built transactions, signer sets). Runs on its own loop.
    async fn refresh(&self, _ctx: &BootstrapCtx) -> anyhow::Result<()> {
        Ok(())
    }

    /// Cheap and synchronous: runs on the alert path for every alert of every program.
    fn accepts(&self, alert: &Alert) -> bool;

    /// Runs in its own task per accepted alert. The returned alerts are routed as this rule's.
    async fn react(&self, alert: Alert, ctx: ReactCtx) -> Vec<Alert>;
}

type AcceptsFn = Arc<dyn Fn(&Alert) -> bool + Send + Sync>;
type ReactFn = Arc<dyn Fn(Alert, ReactCtx) -> BoxFuture<'static, Vec<Alert>> + Send + Sync>;
type RefreshFn =
    Arc<dyn for<'a> Fn(&'a BootstrapCtx) -> BoxFuture<'a, anyhow::Result<()>> + Send + Sync>;

/// A bootstrapped reactor with its type erased.
#[derive(Clone)]
pub struct BuiltReactor {
    pub id: RuleId,
    accepts: AcceptsFn,
    react: ReactFn,
    refresh: Option<(Duration, RefreshFn)>,
}

impl BuiltReactor {
    pub fn new<R: ReactorRule>(rule: R, refresh_interval: Option<Duration>) -> Self {
        let rule = Arc::new(rule);
        let accepts = Arc::clone(&rule);
        let react = Arc::clone(&rule);
        Self {
            id: R::ID,
            accepts: Arc::new(move |alert: &Alert| accepts.accepts(alert)),
            react: Arc::new(move |alert: Alert, ctx: ReactCtx| {
                let rule = Arc::clone(&react);
                Box::pin(async move { rule.react(alert, ctx).await })
            }),
            refresh: refresh_interval.map(|interval| {
                let refresh: RefreshFn = Arc::new(move |ctx: &BootstrapCtx| {
                    let rule = Arc::clone(&rule);
                    Box::pin(async move { rule.refresh(ctx).await })
                });
                (interval, refresh)
            }),
        }
    }

    /// Whether this reactor wants `alert`. Never its own alerts, never a canary.
    pub fn accepts(&self, alert: &Alert) -> bool {
        alert.rule != self.id && !alert.is_canary() && (self.accepts)(alert)
    }

    pub async fn react(&self, alert: Alert, ctx: ReactCtx) -> Vec<Alert> {
        (self.react)(alert, ctx).await
    }
}

// ---- registration -----------------------------------------------------------

pub(crate) struct ReactorEntry {
    pub id: RuleId,
    pub validate: fn(Value) -> Result<ValidatedReactor, config::ConfigError>,
}

impl ReactorEntry {
    pub fn of<R: ReactorRule>() -> Self {
        Self {
            id: R::ID,
            validate: validate_reactor::<R>,
        }
    }
}

type BuildFn = Box<
    dyn for<'a> FnOnce(&'a Deps, &'a BootstrapCtx) -> BoxFuture<'a, anyhow::Result<BuiltReactor>>
        + Send,
>;

pub(crate) struct ValidatedReactor {
    pub id: RuleId,
    build: BuildFn,
}

impl ValidatedReactor {
    pub async fn build(
        self,
        program: Program,
        deps: &Deps,
        ctx: &BootstrapCtx,
    ) -> Result<BuiltReactor, RuleConfigError> {
        (self.build)(deps, ctx)
            .await
            .map_err(|source| RuleConfigError::Build {
                program,
                rule: self.id.name,
                source,
            })
    }
}

fn validate_reactor<R: ReactorRule>(value: Value) -> Result<ValidatedReactor, config::ConfigError> {
    let cfg: R::Config = value.try_deserialize()?;
    let refresh_interval = R::refresh_interval(&cfg);
    Ok(ValidatedReactor {
        id: R::ID,
        build: Box::new(move |deps, ctx| {
            Box::pin(async move {
                let mut rule = R::build(cfg, deps)?;
                rule.bootstrap(ctx).await?;
                Ok(BuiltReactor::new(rule, refresh_interval))
            })
        }),
    })
}

// ---- runtime ----------------------------------------------------------------

/// Run each reactor's refresh on its own cadence until cancelled.
pub fn spawn_reactor_refresh_loops(
    reactors: &[BuiltReactor],
    ctx: &BootstrapCtx,
    cancel_token: &CancellationToken,
    tracker: &TaskTracker,
) {
    for reactor in reactors {
        let Some((interval, refresh)) = reactor.refresh.clone() else {
            continue;
        };
        let id = reactor.id;
        let ctx = ctx.clone();
        let cancel_token = cancel_token.clone();
        info!(rule = %id, interval_secs = interval.as_secs(), "Starting reactor refresh loop");
        let mut tick = tokio::time::interval(interval);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        tick.reset(); // bootstrap just ran
        tracker.spawn(async move {
            loop {
                tokio::select! {
                    _ = cancel_token.cancelled() => break,
                    _ = tick.tick() => {
                        if let Err(e) = refresh(&ctx).await {
                            error!(rule = %id, error = format!("{e:#}"), "Reactor refresh failed");
                        }
                    }
                }
            }
        });
    }
}

/// Relay `rules_rx` → `alerts_tx`, handing each alert to every reactor that accepts it first.
/// Reactions run in their own tasks on `tracker`; their alerts go to `alerts_tx` directly.
/// Stops when `rules_rx` closes; reactions still running finish on the tracker.
pub fn spawn_alert_tap(
    reactors: Vec<BuiltReactor>,
    mut rules_rx: mpsc::Receiver<Alert>,
    alerts_tx: mpsc::Sender<Alert>,
    tracker: &TaskTracker,
) {
    if !reactors.is_empty() {
        info!(
            reactors = ?reactors.iter().map(|r| r.id.name).collect::<Vec<_>>(),
            "Alert tap feeding reactors"
        );
    }
    let reactors: Arc<[BuiltReactor]> = reactors.into();
    let spawner = tracker.clone();
    tracker.spawn(async move {
        while let Some(alert) = rules_rx.recv().await {
            dispatch(&reactors, &alert, &alerts_tx, &spawner);
            if alerts_tx.send(alert).await.is_err() {
                warn!("Alerting service gone; alert tap stopping");
                break;
            }
        }
    });
}

/// Hand `alert` to every accepting reactor, each in its own task.
fn dispatch(
    reactors: &[BuiltReactor],
    alert: &Alert,
    alerts_tx: &mpsc::Sender<Alert>,
    tracker: &TaskTracker,
) {
    let seen_at = Instant::now();
    for reactor in reactors {
        if !reactor.accepts(alert) {
            continue;
        }
        let ctx = ReactCtx {
            seen_at,
            trigger_slot: alert
                .slot_hold
                .as_ref()
                .map(domain::inflight::SlotHold::slot),
        };
        // The reaction must not hold the slot checkpoint back; the forwarded alert keeps it.
        let mut copy = alert.clone();
        copy.slot_hold = None;
        let reactor = reactor.clone();
        let alerts_tx = alerts_tx.clone();
        tracker.spawn(async move {
            for out in reactor.react(copy, ctx).await {
                if out.rule != reactor.id {
                    warn!(rule = %reactor.id, emitted = %out.rule, "Reactor emitted another rule's alert; dropped");
                    continue;
                }
                if alerts_tx.send(out).await.is_err() {
                    warn!(rule = %reactor.id, "Alerting service gone; reactor alert dropped");
                    return;
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rule::NoConfig;
    use domain::inflight::SlotHold;

    const PROGRAM: Program = Program::new("test_program");
    const SOURCE: RuleId = RuleId::new(PROGRAM, "source");

    struct Echo;

    #[async_trait]
    impl ReactorRule for Echo {
        const ID: RuleId = RuleId::new(PROGRAM, "echo");
        type Config = NoConfig;

        fn build(_: NoConfig, _: &Deps) -> anyhow::Result<Self> {
            Ok(Self)
        }

        fn accepts(&self, alert: &Alert) -> bool {
            alert.metadata.get("action").map(String::as_str) == Some("L1")
        }

        async fn react(&self, alert: Alert, ctx: ReactCtx) -> Vec<Alert> {
            assert!(
                alert.slot_hold.is_none(),
                "reaction must not hold the checkpoint"
            );
            vec![Alert::for_rule(Self::ID)
                .message(format!("echo {}", alert.message))
                .meta("trigger_slot", ctx.trigger_slot.unwrap_or_default())
                .build()]
        }
    }

    fn source_alert(action: &str, slot: Option<u64>) -> Alert {
        let mut alert = Alert::for_rule(SOURCE)
            .message(action)
            .meta("action", action)
            .build();
        alert.slot_hold = slot.map(SlotHold::new);
        alert
    }

    /// Every alert passes through; accepted ones also produce the reactor's alert, with the slot.
    #[tokio::test]
    async fn tap_forwards_and_reacts() {
        let tracker = TaskTracker::new();
        let (rules_tx, rules_rx) = mpsc::channel(8);
        let (alerts_tx, mut alerts_rx) = mpsc::channel(8);
        spawn_alert_tap(
            vec![BuiltReactor::new(Echo, None)],
            rules_rx,
            alerts_tx,
            &tracker,
        );
        rules_tx.send(source_alert("alert", None)).await.unwrap();
        rules_tx.send(source_alert("L1", Some(42))).await.unwrap();
        drop(rules_tx);
        tracker.close();
        tracker.wait().await;

        let mut got = Vec::new();
        while let Ok(a) = alerts_rx.try_recv() {
            got.push(a);
        }
        assert_eq!(got.len(), 3);
        assert_eq!(got.iter().filter(|a| a.rule == SOURCE).count(), 2);
        let echo = got.iter().find(|a| a.rule == Echo::ID).unwrap();
        assert_eq!(echo.message, "echo L1");
        assert_eq!(echo.metadata["trigger_slot"], "42");
        // The forwarded original still holds its slot.
        let original = got.iter().find(|a| a.message == "L1").unwrap();
        assert!(original.slot_hold.is_some());
    }

    /// Reactors are configured by name like any rule; unconfigured ones are not validated.
    #[test]
    fn spec_validates_reactors() {
        let spec = crate::ProgramSpec::new(PROGRAM).reactor::<Echo>();
        assert_eq!(spec.rule_ids(), vec![Echo::ID]);
        let empty = crate::RawRuleMap::new();
        assert!(spec.validate(&empty).unwrap().rule_ids().is_empty());
        let raw = crate::RawRuleMap::from([(
            "echo".to_string(),
            Value::from(config::Map::<String, Value>::new()),
        )]);
        let validated = spec.validate(&raw).unwrap();
        assert_eq!(validated.rule_ids(), vec![Echo::ID]);
        assert!(validated.report().to_string().contains("reactor echo"));
    }

    /// A reactor never sees its own alerts.
    #[test]
    fn reactor_ignores_itself() {
        let reactor = BuiltReactor::new(Echo, None);
        let own = Alert::for_rule(Echo::ID).meta("action", "L1").build();
        assert!(!reactor.accepts(&own));
        assert!(reactor.accepts(&source_alert("L1", None)));
    }

    /// Canaries pass through the tap but no reactor acts on them, whatever they carry.
    #[test]
    fn reactor_ignores_canaries() {
        let reactor = BuiltReactor::new(Echo, None);
        let mut canary = source_alert("L1", None);
        canary
            .metadata
            .insert(domain::canary::CANARY.to_string(), "true".to_string());
        assert!(!reactor.accepts(&canary));
    }
}
