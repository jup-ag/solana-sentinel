//! Rule traits: a rule implements exactly one of [`StreamRule`] (fed by a
//! parser pipeline) or [`PollRule`] (fed by an RPC poll loop).

use crate::deps::Deps;
use crate::subscription::Subscription;
use crate::BootstrapCtx;
use async_trait::async_trait;
use domain::alert::{Alert, DedupScope};
use domain::counter_measure::{CounterMeasure, Trigger};
use domain::programs::Program;
use domain::rules::RuleId;
use observability::prometheus::metrics::record_countermeasure;
use serde::de::DeserializeOwned;
use solana_pubkey::Pubkey;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::sync::mpsc;
use tracing::{error, warn};

const TX_SIG_UNAVAILABLE: &str = "NOT_AVAILABLE";

/// What the stream knows about the event a rule is evaluating.
#[derive(Debug, Clone, Default)]
pub struct StreamCtx {
    pub slot: u64,
    /// Signature of the transaction that produced the update, when the feed carries one.
    pub tx_sig: Option<String>,
    /// The account that changed; set on account pipelines only.
    pub account: Option<Pubkey>,
}

impl StreamCtx {
    /// The signature, or the same placeholder the old adapters used when it was missing.
    pub fn tx_sig(&self) -> &str {
        self.tx_sig.as_deref().unwrap_or(TX_SIG_UNAVAILABLE)
    }

    /// Dedup per transaction: repeats of the same condition in other transactions alert again.
    pub fn tx_scope(&self) -> DedupScope {
        DedupScope::Tx(self.tx_sig().to_string())
    }
}

/// What a poll tick knows: the RPC response slot and when the fetch ran.
#[derive(Debug, Clone, Copy)]
pub struct PollCtx {
    pub slot: u64,
    pub fetched_at: Instant,
}

/// A rule fed by an account, instruction or transaction pipeline.
#[async_trait]
pub trait StreamRule: Sized + Send + Sync + 'static {
    /// Program + name; the name is what `rules.toml` refers to.
    const ID: RuleId;
    /// Typed config, deserialized from the rule's `[rules.config]` table before any I/O.
    type Config: DeserializeOwned + Send + Sync + 'static;
    /// The pipeline parser's output.
    type Event: Send + Sync + 'static;

    /// Pure function of constants and config; shapes the subscription.
    fn subscription(cfg: &Self::Config) -> Subscription;

    /// `deps.get::<Cache>()` for the program's shared state; most rules ignore it.
    fn build(cfg: Self::Config, deps: &Deps) -> anyhow::Result<Self>;

    /// Seed caches from RPC. Runs after the deps bootstrap, before any event is delivered.
    async fn bootstrap(&mut self, _ctx: &BootstrapCtx) -> anyhow::Result<()> {
        Ok(())
    }

    /// Sees every event of the pipeline's union subscription, including other rules' and other
    /// instances', so return `None` for anything outside this rule's own config. Rules that keep
    /// last-seen state use interior mutability (`StateCache`); handler and sweep may call concurrently.
    fn evaluate(&self, ev: &Self::Event, ctx: &StreamCtx) -> Option<Alert>;

    /// Optional periodic re-evaluation with no incoming event (interest accrual, staleness).
    fn sweep_interval(_cfg: &Self::Config) -> Option<Duration> {
        None
    }

    fn sweep(&self) -> Vec<Alert> {
        Vec::new()
    }

    /// On-chain response to this rule's alert; called right after `evaluate` or `sweep` fires,
    /// only when Riposte is on and the rule has `riposte = true`. Riposte sends one per
    /// condition per cooldown, but a crash or failover can repeat one: ixs must be safe to
    /// land twice (rejected or a no-op on repeat, e.g. pause when already paused).
    fn counteract(
        &self,
        _alert: &Alert,
        _ctx: &CounteractCtx,
    ) -> anyhow::Result<Option<CounterMeasure>> {
        Ok(None)
    }
}

/// A rule fed by a poll loop: `accounts()` are fetched on a cadence and parsed
/// through the program's [`crate::PollSource`].
#[async_trait]
pub trait PollRule: Sized + Send + Sync + 'static {
    const ID: RuleId;
    type Config: DeserializeOwned + Send + Sync + 'static;
    /// The program's `PollSource::Item`.
    type Item: Send + Sync + 'static;

    fn accounts(cfg: &Self::Config) -> HashSet<Pubkey>;

    /// `None` means the program default cadence. Same-cadence rules share one fetch.
    fn poll_interval(_cfg: &Self::Config) -> Option<Duration> {
        None
    }

    fn build(cfg: Self::Config, deps: &Deps) -> anyhow::Result<Self>;

    async fn bootstrap(&mut self, _ctx: &BootstrapCtx) -> anyhow::Result<()> {
        Ok(())
    }

    fn evaluate(&self, item: &Self::Item, ctx: &PollCtx) -> Option<Alert>;

    /// Same as [`StreamRule::counteract`].
    fn counteract(
        &self,
        _alert: &Alert,
        _ctx: &CounteractCtx,
    ) -> anyhow::Result<Option<CounterMeasure>> {
        Ok(None)
    }
}

/// What the framework knows when it calls `counteract`, like [`StreamCtx`] for `evaluate`.
#[derive(Debug, Clone, Copy)]
pub struct CounteractCtx {
    /// This program's Riposte signer: the only signer the ixs may require; it also pays the
    /// fee. shortcut: one signer per tx (`compose` rejects more); a Vec once multi-signer lands.
    pub authority: Pubkey,
}

/// `BootstrapCtx::riposte`; `None` when Riposte is off.
#[derive(Debug, Clone)]
pub struct RiposteCtx {
    /// Per program: its Riposte signer, the rules' [`CounteractCtx::authority`].
    pub authorities: HashMap<Program, Pubkey>,
    /// Rules with `riposte = true`; `counteract` runs for these only.
    pub rules: HashSet<RuleId>,
    /// Riposte's queue; the alert rides along for its dedup key and the signer.
    pub tx: mpsc::Sender<Trigger>,
}

pub type StreamEvalFn<Ev> = Arc<dyn Fn(&Ev, &StreamCtx) -> Option<Alert> + Send + Sync>;
pub type SweepFn = Arc<dyn Fn() -> Vec<Alert> + Send + Sync>;
pub type PollEvalFn<It> = Arc<dyn Fn(&It, &PollCtx) -> Option<Alert> + Send + Sync>;
/// Runs the rule's `counteract` on a fired alert and queues the result to Riposte; when that
/// fails, marks the alert (`riposte` meta) before it's forwarded. Takes the stream event's slot.
pub type CounteractFn = Arc<dyn Fn(&mut Alert, Option<u64>) + Send + Sync>;

/// A bootstrapped stream rule with its type erased; the handler and the sweep loop share it.
pub struct BuiltStreamRule<Ev> {
    pub id: RuleId,
    pub evaluate: StreamEvalFn<Ev>,
    pub sweep: Option<(Duration, SweepFn)>,
    /// `Some` when Riposte runs and the rule has `riposte = true`; call it right after
    /// `evaluate` / `sweep` fires, before the alert is forwarded.
    pub counteract: Option<CounteractFn>,
}

// Derived `Clone` would demand `Ev: Clone`; only the `Arc`s are cloned.
impl<Ev> Clone for BuiltStreamRule<Ev> {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            evaluate: Arc::clone(&self.evaluate),
            sweep: self.sweep.clone(),
            counteract: self.counteract.clone(),
        }
    }
}

impl<Ev: Send + Sync + 'static> BuiltStreamRule<Ev> {
    /// `id` is `R::ID` plus the configured instance; every alert the rule raises carries it.
    pub fn new<R: StreamRule<Event = Ev>>(
        rule: R,
        id: RuleId,
        sweep_interval: Option<Duration>,
        riposte: Option<&RiposteCtx>,
    ) -> Self {
        let rule = Arc::new(rule);
        let eval = Arc::clone(&rule);
        let counteract = counteract_fn(riposte, id, &rule, StreamRule::counteract);
        Self {
            id,
            evaluate: Arc::new(move |ev: &Ev, ctx: &StreamCtx| {
                eval.evaluate(ev, ctx).map(|a| a.for_instance(id))
            }),
            sweep: sweep_interval.map(|interval| {
                let sweeper: SweepFn = Arc::new(move || {
                    rule.sweep()
                        .into_iter()
                        .map(|a| a.for_instance(id))
                        .collect()
                });
                (interval, sweeper)
            }),
            counteract,
        }
    }
}

/// A bootstrapped poll rule with its type erased.
pub struct BuiltPollRule<It> {
    pub id: RuleId,
    pub evaluate: PollEvalFn<It>,
    /// Same as [`BuiltStreamRule::counteract`].
    pub counteract: Option<CounteractFn>,
}

impl<It: Send + Sync + 'static> BuiltPollRule<It> {
    /// `id` is `R::ID` plus the configured instance; every alert the rule raises carries it.
    pub fn new<R: PollRule<Item = It>>(rule: R, id: RuleId, riposte: Option<&RiposteCtx>) -> Self {
        let rule = Arc::new(rule);
        let counteract = counteract_fn(riposte, id, &rule, PollRule::counteract);
        Self {
            id,
            evaluate: Arc::new(move |item: &It, ctx: &PollCtx| {
                rule.evaluate(item, ctx).map(|a| a.for_instance(id))
            }),
            counteract,
        }
    }
}

/// `rule`'s `counteract` wired to Riposte's queue, or `None` when Riposte doesn't cover `id`.
/// Never blocks, and the alert goes out regardless of what `counteract` does.
fn counteract_fn<R: Send + Sync + 'static>(
    riposte: Option<&RiposteCtx>,
    id: RuleId,
    rule: &Arc<R>,
    call: fn(&R, &Alert, &CounteractCtx) -> anyhow::Result<Option<CounterMeasure>>,
) -> Option<CounteractFn> {
    let r = riposte.filter(|r| r.rules.contains(&id))?;
    // Startup requires a signer for every responding rule's program.
    let ctx = CounteractCtx {
        authority: *r.authorities.get(&id.program)?,
    };
    let tx = r.tx.clone();
    let rule = Arc::clone(rule);
    Some(Arc::new(move |alert: &mut Alert, slot: Option<u64>| {
        let not_sent = match call(&rule, alert, &ctx) {
            Ok(Some(cm)) if !cm.ixs.is_empty() => match tx.try_send((alert.clone(), cm, slot)) {
                Ok(()) => return,
                Err(e) => {
                    warn!(rule = %alert.rule, error = %e, "riposte queue unavailable; countermeasure dropped");
                    record_countermeasure(&alert.rule.to_string(), "dropped");
                    format!("riposte queue unavailable: {e}")
                }
            },
            Ok(_) => return,
            Err(e) => {
                error!(rule = %alert.rule, error = ?e, "counteract failed; no countermeasure");
                record_countermeasure(&alert.rule.to_string(), "failed");
                format!("counteract failed: {e:#}")
            }
        };
        // On the alert that goes out next, so the rule's sinks see protection didn't run.
        alert
            .metadata
            .insert("riposte".into(), format!("not sent: {not_sent}"));
    }))
}

/// Config for rules that take none; accepts an empty or absent `[rules.config]`.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoConfig;

impl<'de> serde::Deserialize<'de> for NoConfig {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        serde::de::IgnoredAny::deserialize(d)?;
        Ok(Self)
    }
}

#[derive(Debug, Error)]
pub enum RuleConfigError {
    #[error("program `{program}` defines unknown rule `{rule}`")]
    UnknownRule { program: Program, rule: String },
    #[error("program `{program}` rule `{rule}`: invalid config: {source}")]
    Config {
        program: Program,
        rule: RuleId,
        #[source]
        source: Box<config::ConfigError>,
    },
    #[error("program `{program}` rule `{rule}`: {source}")]
    Build {
        program: Program,
        rule: RuleId,
        #[source]
        source: anyhow::Error,
    },
}
