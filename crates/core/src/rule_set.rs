//! Rule registries: one per feed. Both validate typed config with no I/O,
//! then build and bootstrap each configured rule.

use crate::deps::ProgramDeps;
use crate::rule::{
    BuiltStreamRule, DynGrpcRule, DynRpcRule, GrpcRule, RpcRule, RuleConfigError, SharedRpcRule,
};
use crate::subscription::Subscription;
use crate::{BootstrapCtx, RawRuleMap};
use async_trait::async_trait;
use config::Value;
use domain::programs::Program;
use domain::rules::RuleId;
use solana_pubkey::Pubkey;
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

// ---- stream -----------------------------------------------------------------

type StreamValidator<Ev, D> =
    fn(Value) -> Result<Box<dyn PendingStreamRule<Ev, D>>, config::ConfigError>;

struct StreamEntry<Ev, D> {
    id: RuleId,
    validate: StreamValidator<Ev, D>,
}

/// Rules of one stream pipeline. `register` only accepts rules whose event and deps types match.
pub struct StreamRules<Ev, D = ()> {
    entries: Vec<StreamEntry<Ev, D>>,
}

impl<Ev, D> Default for StreamRules<Ev, D> {
    fn default() -> Self {
        Self {
            entries: Vec::new(),
        }
    }
}

impl<Ev: Send + Sync + 'static, D: ProgramDeps> StreamRules<Ev, D> {
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn register<R: GrpcRule<Event = Ev, Deps = D>>(mut self) -> Self {
        self.entries.push(StreamEntry {
            id: R::ID,
            validate: validate_stream::<R>,
        });
        self
    }

    pub fn ids(&self) -> Vec<RuleId> {
        self.entries.iter().map(|e| e.id).collect()
    }

    /// Typed-config validation for the rules present in `raw`; no I/O.
    pub fn validate(
        &self,
        program: Program,
        raw: &RawRuleMap,
    ) -> Result<ValidatedStream<Ev, D>, RuleConfigError> {
        let mut rules = Vec::new();
        for entry in &self.entries {
            let Some(value) = raw.get(entry.id.name) else {
                continue;
            };
            let pending =
                (entry.validate)(value.clone()).map_err(|source| RuleConfigError::Config {
                    program,
                    rule: entry.id.name,
                    source: Box::new(source),
                })?;
            rules.push(pending);
        }
        Ok(ValidatedStream { program, rules })
    }
}

fn validate_stream<R: GrpcRule>(
    value: Value,
) -> Result<Box<dyn PendingStreamRule<R::Event, R::Deps>>, config::ConfigError> {
    let cfg: R::Config = value.try_deserialize()?;
    let subscription = R::static_subscription(&cfg);
    let sweep_interval = R::sweep_interval(&cfg);
    Ok(Box::new(PendingStream::<R> {
        cfg,
        subscription,
        sweep_interval,
    }))
}

#[async_trait]
pub trait PendingStreamRule<Ev, D>: Send {
    fn id(&self) -> RuleId;
    fn subscription(&self) -> &Subscription;
    async fn build(
        self: Box<Self>,
        deps: &D,
        ctx: &BootstrapCtx,
    ) -> anyhow::Result<BuiltStreamRule<Ev>>;
}

struct PendingStream<R: GrpcRule> {
    cfg: R::Config,
    subscription: Subscription,
    sweep_interval: Option<Duration>,
}

#[async_trait]
impl<R: GrpcRule> PendingStreamRule<R::Event, R::Deps> for PendingStream<R> {
    fn id(&self) -> RuleId {
        R::ID
    }

    fn subscription(&self) -> &Subscription {
        &self.subscription
    }

    async fn build(
        self: Box<Self>,
        deps: &R::Deps,
        ctx: &BootstrapCtx,
    ) -> anyhow::Result<BuiltStreamRule<R::Event>> {
        let mut rule = R::build(self.cfg, deps)?;
        rule.bootstrap(ctx).await?;
        Ok(BuiltStreamRule {
            rule: Arc::new(rule) as Arc<dyn DynGrpcRule<R::Event>>,
            sweep_interval: self.sweep_interval,
        })
    }
}

pub struct ValidatedStream<Ev, D> {
    program: Program,
    rules: Vec<Box<dyn PendingStreamRule<Ev, D>>>,
}

impl<Ev: Send + Sync + 'static, D: ProgramDeps> ValidatedStream<Ev, D> {
    pub fn is_empty(&self) -> bool {
        self.rules.is_empty()
    }

    pub fn ids(&self) -> Vec<RuleId> {
        self.rules.iter().map(|r| r.id()).collect()
    }

    /// Union over the configured rules' static subscriptions.
    pub fn static_subscription(&self) -> Subscription {
        self.rules.iter().fold(Subscription::default(), |acc, r| {
            acc.union(r.subscription().clone())
        })
    }

    /// Per-rule static subscriptions, for `check` output.
    pub fn subscriptions(&self) -> Vec<(RuleId, Subscription)> {
        self.rules
            .iter()
            .map(|r| (r.id(), r.subscription().clone()))
            .collect()
    }

    pub async fn build(
        self,
        deps: &D,
        ctx: &BootstrapCtx,
    ) -> Result<Vec<BuiltStreamRule<Ev>>, RuleConfigError> {
        let mut out = Vec::with_capacity(self.rules.len());
        for pending in self.rules {
            let id = pending.id();
            let rule = pending
                .build(deps, ctx)
                .await
                .map_err(|source| RuleConfigError::Build {
                    program: self.program,
                    rule: id.name,
                    source,
                })?;
            out.push(rule);
        }
        Ok(out)
    }
}

// ---- poll -------------------------------------------------------------------

type PollValidator<It, D> =
    fn(Value) -> Result<Box<dyn PendingPollRule<It, D>>, config::ConfigError>;

struct PollEntry<It, D> {
    id: RuleId,
    validate: PollValidator<It, D>,
}

/// Rules of one poll feed. `register` only accepts rules whose item and deps types match.
pub struct PollRules<It, D = ()> {
    entries: Vec<PollEntry<It, D>>,
}

impl<It, D> Default for PollRules<It, D> {
    fn default() -> Self {
        Self {
            entries: Vec::new(),
        }
    }
}

impl<It: Send + Sync + 'static, D: ProgramDeps> PollRules<It, D> {
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn register<R: RpcRule<Item = It, Deps = D>>(mut self) -> Self {
        self.entries.push(PollEntry {
            id: R::ID,
            validate: validate_poll::<R>,
        });
        self
    }

    pub fn ids(&self) -> Vec<RuleId> {
        self.entries.iter().map(|e| e.id).collect()
    }

    pub fn validate(
        &self,
        program: Program,
        raw: &RawRuleMap,
    ) -> Result<ValidatedPoll<It, D>, RuleConfigError> {
        let mut rules = Vec::new();
        for entry in &self.entries {
            let Some(value) = raw.get(entry.id.name) else {
                continue;
            };
            let pending =
                (entry.validate)(value.clone()).map_err(|source| RuleConfigError::Config {
                    program,
                    rule: entry.id.name,
                    source: Box::new(source),
                })?;
            rules.push(pending);
        }
        Ok(ValidatedPoll { program, rules })
    }
}

fn validate_poll<R: RpcRule>(
    value: Value,
) -> Result<Box<dyn PendingPollRule<R::Item, R::Deps>>, config::ConfigError> {
    let cfg: R::Config = value.try_deserialize()?;
    let accounts = R::accounts(&cfg);
    let interval = R::poll_interval(&cfg);
    Ok(Box::new(PendingPoll::<R> {
        cfg,
        accounts,
        interval,
    }))
}

#[async_trait]
pub trait PendingPollRule<It, D>: Send {
    fn id(&self) -> RuleId;
    fn accounts(&self) -> &HashSet<Pubkey>;
    fn poll_interval(&self) -> Option<Duration>;
    async fn build(
        self: Box<Self>,
        deps: &D,
        ctx: &BootstrapCtx,
    ) -> anyhow::Result<SharedRpcRule<It>>;
}

struct PendingPoll<R: RpcRule> {
    cfg: R::Config,
    accounts: HashSet<Pubkey>,
    interval: Option<Duration>,
}

#[async_trait]
impl<R: RpcRule> PendingPollRule<R::Item, R::Deps> for PendingPoll<R> {
    fn id(&self) -> RuleId {
        R::ID
    }

    fn accounts(&self) -> &HashSet<Pubkey> {
        &self.accounts
    }

    fn poll_interval(&self) -> Option<Duration> {
        self.interval
    }

    async fn build(
        self: Box<Self>,
        deps: &R::Deps,
        ctx: &BootstrapCtx,
    ) -> anyhow::Result<SharedRpcRule<R::Item>> {
        let mut rule = R::build(self.cfg, deps)?;
        rule.bootstrap(ctx).await?;
        Ok(Arc::new(rule) as Arc<dyn DynRpcRule<R::Item>>)
    }
}

/// Rules that share one fetch: same cadence, union of their accounts.
pub struct PollBucket<It> {
    pub interval: Duration,
    pub accounts: Vec<Pubkey>,
    pub rules: Vec<SharedRpcRule<It>>,
}

pub struct ValidatedPoll<It, D> {
    program: Program,
    rules: Vec<Box<dyn PendingPollRule<It, D>>>,
}

impl<It: Send + Sync + 'static, D: ProgramDeps> ValidatedPoll<It, D> {
    pub fn is_empty(&self) -> bool {
        self.rules.is_empty()
    }

    pub fn ids(&self) -> Vec<RuleId> {
        self.rules.iter().map(|r| r.id()).collect()
    }

    /// Accounts per cadence, for `check` output.
    pub fn schedule(
        &self,
        default_interval: Duration,
    ) -> BTreeMap<Duration, (Vec<RuleId>, Vec<Pubkey>)> {
        let mut out: BTreeMap<Duration, (Vec<RuleId>, HashSet<Pubkey>)> = BTreeMap::new();
        for rule in &self.rules {
            let slot = out
                .entry(rule.poll_interval().unwrap_or(default_interval))
                .or_default();
            slot.0.push(rule.id());
            slot.1.extend(rule.accounts().iter().copied());
        }
        out.into_iter()
            .map(|(k, (ids, accounts))| (k, (ids, sorted(accounts))))
            .collect()
    }

    /// Rules bucketed by cadence; each bucket becomes one poll loop with one fetch per tick.
    pub async fn build(
        self,
        default_interval: Duration,
        deps: &D,
        ctx: &BootstrapCtx,
    ) -> Result<Vec<PollBucket<It>>, RuleConfigError> {
        let mut buckets: BTreeMap<Duration, (HashSet<Pubkey>, Vec<SharedRpcRule<It>>)> =
            BTreeMap::new();
        for pending in self.rules {
            let id = pending.id();
            let interval = pending.poll_interval().unwrap_or(default_interval);
            let accounts = pending.accounts().clone();
            let rule = pending
                .build(deps, ctx)
                .await
                .map_err(|source| RuleConfigError::Build {
                    program: self.program,
                    rule: id.name,
                    source,
                })?;
            let bucket = buckets.entry(interval).or_default();
            bucket.0.extend(accounts);
            bucket.1.push(rule);
        }
        Ok(buckets
            .into_iter()
            .map(|(interval, (accounts, rules))| PollBucket {
                interval,
                accounts: sorted(accounts),
                rules,
            })
            .collect())
    }
}

fn sorted(set: HashSet<Pubkey>) -> Vec<Pubkey> {
    let mut v: Vec<Pubkey> = set.into_iter().collect();
    v.sort_unstable();
    v
}
