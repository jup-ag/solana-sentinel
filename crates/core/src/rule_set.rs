//! Rule registries, one per feed. `validate` deserializes typed config with no
//! I/O; `build` runs each rule's `build` + `bootstrap` later.

use crate::deps::Deps;
use crate::rule::{BuiltPollRule, BuiltStreamRule, PollRule, RuleConfigError, StreamRule};
use crate::subscription::Subscription;
use crate::{BootstrapCtx, RawRuleMap};
use config::Value;
use domain::programs::Program;
use domain::rules::RuleId;
use futures::future::BoxFuture;
use solana_pubkey::Pubkey;
use std::collections::{BTreeMap, HashSet};
use std::time::Duration;

/// Finishes a validated rule: `build` then `bootstrap`, with the typed config captured.
type BuildFn<Built> = Box<
    dyn for<'a> FnOnce(&'a Deps, &'a BootstrapCtx) -> BoxFuture<'a, anyhow::Result<Built>> + Send,
>;

struct Entry<V> {
    id: RuleId,
    validate: fn(Value) -> Result<V, config::ConfigError>,
}

/// Typed-config validation for the entries present in `raw`; no I/O.
fn validate_entries<V>(
    entries: &[Entry<V>],
    program: Program,
    raw: &RawRuleMap,
) -> Result<Vec<V>, RuleConfigError> {
    let mut out = Vec::new();
    for entry in entries {
        let Some(value) = raw.get(entry.id.name) else {
            continue;
        };
        let validated =
            (entry.validate)(value.clone()).map_err(|source| RuleConfigError::Config {
                program,
                rule: entry.id.name,
                source: Box::new(source),
            })?;
        out.push(validated);
    }
    Ok(out)
}

// ---- stream -----------------------------------------------------------------

/// Rules of one stream pipeline. `register` only accepts rules whose event type matches.
pub struct StreamRules<Ev> {
    entries: Vec<Entry<ValidatedStreamRule<Ev>>>,
}

impl<Ev> Default for StreamRules<Ev> {
    fn default() -> Self {
        Self {
            entries: Vec::new(),
        }
    }
}

impl<Ev: Send + Sync + 'static> StreamRules<Ev> {
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn register<R: StreamRule<Event = Ev>>(mut self) -> Self {
        self.entries.push(Entry {
            id: R::ID,
            validate: validate_stream::<R>,
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
    ) -> Result<ValidatedStream<Ev>, RuleConfigError> {
        let rules = validate_entries(&self.entries, program, raw)?;
        Ok(ValidatedStream { program, rules })
    }
}

fn validate_stream<R: StreamRule>(
    value: Value,
) -> Result<ValidatedStreamRule<R::Event>, config::ConfigError> {
    let cfg: R::Config = value.try_deserialize()?;
    let sweep_interval = R::sweep_interval(&cfg);
    Ok(ValidatedStreamRule {
        id: R::ID,
        subscription: R::subscription(&cfg),
        build: Box::new(move |deps, ctx| {
            Box::pin(async move {
                let mut rule = R::build(cfg, deps)?;
                rule.bootstrap(ctx).await?;
                Ok(BuiltStreamRule::new(rule, sweep_interval))
            })
        }),
    })
}

pub struct ValidatedStreamRule<Ev> {
    id: RuleId,
    subscription: Subscription,
    build: BuildFn<BuiltStreamRule<Ev>>,
}

pub struct ValidatedStream<Ev> {
    program: Program,
    rules: Vec<ValidatedStreamRule<Ev>>,
}

impl<Ev: Send + Sync + 'static> ValidatedStream<Ev> {
    pub fn is_empty(&self) -> bool {
        self.rules.is_empty()
    }

    pub fn ids(&self) -> Vec<RuleId> {
        self.rules.iter().map(|r| r.id).collect()
    }

    /// Union over the configured rules' static subscriptions.
    pub fn static_subscription(&self) -> Subscription {
        self.rules.iter().fold(Subscription::default(), |acc, r| {
            acc.union(r.subscription.clone())
        })
    }

    /// Per-rule static subscriptions, for `check` output.
    pub fn subscriptions(&self) -> Vec<(RuleId, Subscription)> {
        self.rules
            .iter()
            .map(|r| (r.id, r.subscription.clone()))
            .collect()
    }

    pub async fn build(
        self,
        deps: &Deps,
        ctx: &BootstrapCtx,
    ) -> Result<Vec<BuiltStreamRule<Ev>>, RuleConfigError> {
        let mut out = Vec::with_capacity(self.rules.len());
        for rule in self.rules {
            let built = (rule.build)(deps, ctx)
                .await
                .map_err(|source| RuleConfigError::Build {
                    program: self.program,
                    rule: rule.id.name,
                    source,
                })?;
            out.push(built);
        }
        Ok(out)
    }
}

// ---- poll -------------------------------------------------------------------

/// Rules of one poll feed. `register` only accepts rules whose item type matches.
pub struct PollRules<It> {
    entries: Vec<Entry<ValidatedPollRule<It>>>,
}

impl<It> Default for PollRules<It> {
    fn default() -> Self {
        Self {
            entries: Vec::new(),
        }
    }
}

impl<It: Send + Sync + 'static> PollRules<It> {
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn register<R: PollRule<Item = It>>(mut self) -> Self {
        self.entries.push(Entry {
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
    ) -> Result<ValidatedPoll<It>, RuleConfigError> {
        let rules = validate_entries(&self.entries, program, raw)?;
        Ok(ValidatedPoll { program, rules })
    }
}

fn validate_poll<R: PollRule>(
    value: Value,
) -> Result<ValidatedPollRule<R::Item>, config::ConfigError> {
    let cfg: R::Config = value.try_deserialize()?;
    Ok(ValidatedPollRule {
        id: R::ID,
        accounts: R::accounts(&cfg),
        interval: R::poll_interval(&cfg),
        build: Box::new(move |deps, ctx| {
            Box::pin(async move {
                let mut rule = R::build(cfg, deps)?;
                rule.bootstrap(ctx).await?;
                Ok(BuiltPollRule::new(rule))
            })
        }),
    })
}

pub struct ValidatedPollRule<It> {
    id: RuleId,
    accounts: HashSet<Pubkey>,
    interval: Option<Duration>,
    build: BuildFn<BuiltPollRule<It>>,
}

/// Rules that share one fetch: same cadence, union of their accounts.
pub struct PollBucket<It> {
    pub interval: Duration,
    pub accounts: Vec<Pubkey>,
    pub rules: Vec<BuiltPollRule<It>>,
}

pub struct ValidatedPoll<It> {
    program: Program,
    rules: Vec<ValidatedPollRule<It>>,
}

impl<It: Send + Sync + 'static> ValidatedPoll<It> {
    pub fn is_empty(&self) -> bool {
        self.rules.is_empty()
    }

    pub fn ids(&self) -> Vec<RuleId> {
        self.rules.iter().map(|r| r.id).collect()
    }

    /// Accounts per cadence, for `check` output.
    pub fn schedule(
        &self,
        default_interval: Duration,
    ) -> BTreeMap<Duration, (Vec<RuleId>, Vec<Pubkey>)> {
        let mut out: BTreeMap<Duration, (Vec<RuleId>, HashSet<Pubkey>)> = BTreeMap::new();
        for rule in &self.rules {
            let slot = out
                .entry(rule.interval.unwrap_or(default_interval))
                .or_default();
            slot.0.push(rule.id);
            slot.1.extend(rule.accounts.iter().copied());
        }
        out.into_iter()
            .map(|(k, (ids, accounts))| (k, (ids, sorted(accounts))))
            .collect()
    }

    /// Rules bucketed by cadence; each bucket becomes one poll loop with one fetch per tick.
    pub async fn build(
        self,
        default_interval: Duration,
        deps: &Deps,
        ctx: &BootstrapCtx,
    ) -> Result<Vec<PollBucket<It>>, RuleConfigError> {
        let mut buckets: BTreeMap<Duration, (HashSet<Pubkey>, Vec<BuiltPollRule<It>>)> =
            BTreeMap::new();
        for rule in self.rules {
            let interval = rule.interval.unwrap_or(default_interval);
            let built = (rule.build)(deps, ctx)
                .await
                .map_err(|source| RuleConfigError::Build {
                    program: self.program,
                    rule: rule.id.name,
                    source,
                })?;
            let bucket = buckets.entry(interval).or_default();
            bucket.0.extend(rule.accounts);
            bucket.1.push(built);
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
