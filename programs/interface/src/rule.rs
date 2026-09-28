//! Rule traits. [`RuleDef`] is shared; a rule implements exactly one of
//! [`GrpcRule`] (stream-fed) or [`RpcRule`] (poll-fed).

use crate::deps::ProgramDeps;
use crate::prefilter::Prefilter;
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

/// Identity, config, construction and bootstrap. Shared by both feeds.
#[async_trait]
pub trait RuleDef: Sized + Send + Sync + 'static {
    /// Program + name; the name is what `rules.toml` refers to.
    const ID: RuleId;
    /// Typed config, deserialized from the rule's `[rules.config]` table before any I/O.
    type Config: DeserializeOwned + Send + Sync + 'static;
    /// Program shared state, `()` for most programs.
    type Deps: ProgramDeps;

    fn build(cfg: Self::Config, deps: &Self::Deps) -> anyhow::Result<Self>;

    /// Seed caches from RPC. Runs after `Deps::bootstrap`, before any event is delivered.
    async fn bootstrap(&mut self, _ctx: &BootstrapCtx) -> anyhow::Result<()> {
        Ok(())
    }

    fn name() -> &'static str {
        Self::ID.name
    }

    fn program() -> Program {
        Self::ID.program
    }
}

/// Fed by a shipstern account, instruction or transaction pipeline.
pub trait GrpcRule: RuleDef {
    /// The pipeline parser's output.
    type Event: Send + Sync + 'static;

    /// Pure function of constants and config; shapes the subscription.
    fn static_prefilter(cfg: &Self::Config) -> Prefilter;

    fn evaluate(&self, ev: &Self::Event, ctx: &StreamCtx) -> Option<Alert>;

    /// Optional periodic re-evaluation with no incoming event (interest accrual, staleness).
    fn sweep_interval(&self) -> Option<Duration> {
        None
    }

    fn sweep(&self) -> Vec<Alert> {
        Vec::new()
    }
}

/// Fed by a poll loop that fetches `accounts()` on a cadence and parses through the program's [`crate::PollSource`].
pub trait RpcRule: RuleDef {
    /// The program's `PollSource::Item`.
    type Item: Send + Sync + 'static;

    fn accounts(cfg: &Self::Config) -> HashSet<Pubkey>;

    /// `None` means the program default cadence. Same-cadence rules share one fetch.
    fn poll_interval(_cfg: &Self::Config) -> Option<Duration> {
        None
    }

    fn evaluate(&self, item: &Self::Item, ctx: &PollCtx) -> Option<Alert>;
}

/// Object-safe view of a [`GrpcRule`]; handlers hold `Arc<dyn DynGrpcRule<Ev>>`.
pub trait DynGrpcRule<Ev>: Send + Sync {
    fn id(&self) -> RuleId;
    fn evaluate(&self, ev: &Ev, ctx: &StreamCtx) -> Option<Alert>;
    fn sweep_interval(&self) -> Option<Duration>;
    fn sweep(&self) -> Vec<Alert>;
}

impl<R: GrpcRule> DynGrpcRule<R::Event> for R {
    fn id(&self) -> RuleId {
        R::ID
    }

    fn evaluate(&self, ev: &R::Event, ctx: &StreamCtx) -> Option<Alert> {
        GrpcRule::evaluate(self, ev, ctx)
    }

    fn sweep_interval(&self) -> Option<Duration> {
        GrpcRule::sweep_interval(self)
    }

    fn sweep(&self) -> Vec<Alert> {
        GrpcRule::sweep(self)
    }
}

/// Object-safe view of an [`RpcRule`].
pub trait DynRpcRule<It>: Send + Sync {
    fn id(&self) -> RuleId;
    fn evaluate(&self, item: &It, ctx: &PollCtx) -> Option<Alert>;
}

impl<R: RpcRule> DynRpcRule<R::Item> for R {
    fn id(&self) -> RuleId {
        R::ID
    }

    fn evaluate(&self, item: &R::Item, ctx: &PollCtx) -> Option<Alert> {
        RpcRule::evaluate(self, item, ctx)
    }
}

pub type SharedGrpcRule<Ev> = Arc<dyn DynGrpcRule<Ev>>;
pub type SharedRpcRule<It> = Arc<dyn DynRpcRule<It>>;

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
