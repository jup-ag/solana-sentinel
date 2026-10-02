//! Rule traits: a rule implements exactly one of [`StreamRule`] (fed by a
//! parser pipeline) or [`PollRule`] (fed by an RPC poll loop).

use crate::deps::Deps;
use crate::subscription::Subscription;
use crate::BootstrapCtx;
use async_trait::async_trait;
use domain::alert::{Alert, DedupScope};
use domain::programs::Program;
use domain::rules::RuleId;
use serde::de::DeserializeOwned;
use solana_pubkey::Pubkey;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};
use thiserror::Error;

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

    /// Rules that keep last-seen state use interior mutability (`StateCache`);
    /// the handler and the sweep loop may call concurrently.
    fn evaluate(&self, ev: &Self::Event, ctx: &StreamCtx) -> Option<Alert>;

    /// Optional periodic re-evaluation with no incoming event (interest accrual, staleness).
    fn sweep_interval(_cfg: &Self::Config) -> Option<Duration> {
        None
    }

    fn sweep(&self) -> Vec<Alert> {
        Vec::new()
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
}

pub type StreamEvalFn<Ev> = Arc<dyn Fn(&Ev, &StreamCtx) -> Option<Alert> + Send + Sync>;
pub type SweepFn = Arc<dyn Fn() -> Vec<Alert> + Send + Sync>;
pub type PollEvalFn<It> = Arc<dyn Fn(&It, &PollCtx) -> Option<Alert> + Send + Sync>;

/// A bootstrapped stream rule with its type erased; the handler and the sweep loop share it.
pub struct BuiltStreamRule<Ev> {
    pub id: RuleId,
    pub evaluate: StreamEvalFn<Ev>,
    pub sweep: Option<(Duration, SweepFn)>,
}

// Derived `Clone` would demand `Ev: Clone`; only the `Arc`s are cloned.
impl<Ev> Clone for BuiltStreamRule<Ev> {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            evaluate: Arc::clone(&self.evaluate),
            sweep: self.sweep.clone(),
        }
    }
}

impl<Ev: Send + Sync + 'static> BuiltStreamRule<Ev> {
    pub fn new<R: StreamRule<Event = Ev>>(rule: R, sweep_interval: Option<Duration>) -> Self {
        let rule = Arc::new(rule);
        let eval = Arc::clone(&rule);
        Self {
            id: R::ID,
            evaluate: Arc::new(move |ev: &Ev, ctx: &StreamCtx| eval.evaluate(ev, ctx)),
            sweep: sweep_interval.map(|interval| {
                let sweeper: SweepFn = Arc::new(move || rule.sweep());
                (interval, sweeper)
            }),
        }
    }
}

/// A bootstrapped poll rule with its type erased.
pub struct BuiltPollRule<It> {
    pub id: RuleId,
    pub evaluate: PollEvalFn<It>,
}

impl<It: Send + Sync + 'static> BuiltPollRule<It> {
    pub fn new<R: PollRule<Item = It>>(rule: R) -> Self {
        Self {
            id: R::ID,
            evaluate: Arc::new(move |item: &It, ctx: &PollCtx| rule.evaluate(item, ctx)),
        }
    }
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
        rule: &'static str,
        #[source]
        source: Box<config::ConfigError>,
    },
    #[error("program `{program}` rule `{rule}`: {source}")]
    Build {
        program: Program,
        rule: &'static str,
        #[source]
        source: anyhow::Error,
    },
}
