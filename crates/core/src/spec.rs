//! What a program crate exports: a [`ProgramSpec`] listing a parser per feed
//! and the rules registered on it. The service drives it through
//! `validate` (typed config, no I/O) and `start` (bootstrap deps and rules,
//! build pipelines, spawn loops). Pipelines are type-erased behind two small
//! traits so every program fits one registry entry.

use crate::deps::{Deps, ProgramDeps};
use crate::handler::RuleHandler;
use crate::parser::{
    scoped_parser_id, FrameworkParser, PipelineKind, StreamInput, StreamPipeline, TxGate,
};
use crate::poll::{spawn_poll_loop, PollSource};
use crate::rule::RuleConfigError;
use crate::rule_set::{PollRules, StreamRules, ValidatedPoll, ValidatedStream};
use crate::subscription::Subscription;
use crate::sweep::spawn_sweep_loops;
use crate::{BootstrapCtx, RawRuleMap};
use async_trait::async_trait;
use domain::alert::Alert;
use domain::programs::Program;
use domain::rules::RuleId;
use futures::future::BoxFuture;
use shipstern::filter_pipeline::FilterPipeline;
use shipstern::handler::BoxPipeline;
use shipstern_core::Parser;
use solana_pubkey::Pubkey;
use std::collections::HashSet;
use std::fmt::{self, Debug, Display};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{info, warn};

/// Everything `start` needs besides the program itself.
pub struct StartCtx<'a> {
    pub bootstrap: BootstrapCtx,
    pub alerts_tx: mpsc::Sender<Alert>,
    pub cancel_token: CancellationToken,
    pub tracker: &'a TaskTracker,
}

/// `Deps::bootstrap` behind a function pointer so the spec stays non-generic.
type DepsBootstrap =
    for<'a> fn(&'a BootstrapCtx) -> BoxFuture<'a, anyhow::Result<Arc<dyn ProgramDeps>>>;

fn bootstrap_deps<D: ProgramDeps>(
    ctx: &BootstrapCtx,
) -> BoxFuture<'_, anyhow::Result<Arc<dyn ProgramDeps>>> {
    Box::pin(async move { Ok(Arc::new(D::bootstrap(ctx).await?) as Arc<dyn ProgramDeps>) })
}

// ---- pipelines --------------------------------------------------------------

trait Pipeline: Send + Sync {
    fn rule_ids(&self) -> Vec<RuleId>;
    /// `None` when none of the pipeline's rules are configured.
    fn validate(
        &self,
        program: Program,
        raw: &RawRuleMap,
    ) -> Result<Option<Box<dyn ValidatedPipeline>>, RuleConfigError>;
}

#[async_trait]
trait ValidatedPipeline: Send {
    fn rule_ids(&self) -> Vec<RuleId>;
    fn report(&self) -> PipelineReport;
    /// Build + bootstrap the rules, then hand back stream pipelines or spawn poll loops.
    async fn start(
        self: Box<Self>,
        program: Program,
        deps: &Deps,
        ctx: &StartCtx<'_>,
        out: &mut StartedProgram,
    ) -> Result<(), RuleConfigError>;
}

struct StreamSpec<P: Parser> {
    parser: P,
    rules: StreamRules<P::Output>,
    /// `StreamPipeline::Account` and friends; picks the builder slot for `P::Input`.
    wrap: fn(BoxPipeline<'static, P::Input>) -> StreamPipeline,
}

impl<P> Pipeline for StreamSpec<P>
where
    P: Parser + Clone + Debug + Send + Sync + 'static,
    P::Input: StreamInput,
    P::Output: Send + Sync + 'static,
{
    fn rule_ids(&self) -> Vec<RuleId> {
        self.rules.ids()
    }

    fn validate(
        &self,
        program: Program,
        raw: &RawRuleMap,
    ) -> Result<Option<Box<dyn ValidatedPipeline>>, RuleConfigError> {
        let rules = self.rules.validate(program, raw)?;
        if rules.is_empty() {
            return Ok(None);
        }
        Ok(Some(Box::new(StreamValidated {
            parser: self.parser.clone(),
            rules,
            wrap: self.wrap,
        })))
    }
}

struct StreamValidated<P: Parser> {
    parser: P,
    rules: ValidatedStream<P::Output>,
    wrap: fn(BoxPipeline<'static, P::Input>) -> StreamPipeline,
}

#[async_trait]
impl<P> ValidatedPipeline for StreamValidated<P>
where
    P: Parser + Clone + Debug + Send + Sync + 'static,
    P::Input: StreamInput,
    P::Output: Send + Sync + 'static,
{
    fn rule_ids(&self) -> Vec<RuleId> {
        self.rules.ids()
    }

    fn report(&self) -> PipelineReport {
        PipelineReport {
            kind: P::Input::KIND,
            parser: self.parser.id().into_owned(),
            rules: self.rules.ids(),
            stream: Some(StreamReport {
                per_rule: self.rules.subscriptions(),
                union: self.rules.static_subscription(),
            }),
            polls: Vec::new(),
        }
    }

    async fn start(
        self: Box<Self>,
        program: Program,
        deps: &Deps,
        ctx: &StartCtx<'_>,
        out: &mut StartedProgram,
    ) -> Result<(), RuleConfigError> {
        let kind = P::Input::KIND;
        let base_id = self.parser.id().into_owned();
        let static_subscription = self.rules.static_subscription();
        let rules = self.rules.build(deps, &ctx.bootstrap).await?;
        let rule_names: Vec<_> = rules.iter().map(|r| r.id.name).collect();

        let subscription = static_subscription
            .clone()
            .union(out.deps.dynamic_subscription());
        let subs = kind.prefilters(&subscription);
        if subs.is_empty() {
            warn!(
                program = %program,
                parser = %base_id,
                rules = ?rule_names,
                "Pipeline resolved to an empty subscription; its rules will never fire"
            );
            return Ok(());
        }

        let handler = RuleHandler::new(rules.clone(), ctx.alerts_tx.clone());
        let gate =
            (kind == PipelineKind::Instruction).then(|| TxGate::from_subscription(&subscription));
        let subscriptions = subs.len();
        for (i, sub) in subs.into_iter().enumerate() {
            let parser = FrameworkParser::new(
                self.parser.clone(),
                scoped_parser_id(program, &base_id, i),
                gate.clone(),
            );
            let pipeline = FilterPipeline::new(parser, [handler.clone()], sub);
            out.pipelines.push((self.wrap)(Box::new(pipeline)));
        }
        info!(
            program = %program,
            kind = kind.as_str(),
            parser = %base_id,
            subscriptions,
            rules = ?rule_names,
            "Pipeline built"
        );

        spawn_sweep_loops(&rules, &ctx.alerts_tx, &ctx.cancel_token, ctx.tracker);
        out.streams.push(InstalledStream {
            kind,
            base_id,
            parser_prefilter: self.parser.prefilter(),
            static_subscription,
            subscriptions,
        });
        Ok(())
    }
}

struct PollSpec<S: PollSource> {
    source: Arc<S>,
    default_interval: Duration,
    rules: PollRules<S::Item>,
}

impl<S: PollSource> Pipeline for PollSpec<S> {
    fn rule_ids(&self) -> Vec<RuleId> {
        self.rules.ids()
    }

    fn validate(
        &self,
        program: Program,
        raw: &RawRuleMap,
    ) -> Result<Option<Box<dyn ValidatedPipeline>>, RuleConfigError> {
        let rules = self.rules.validate(program, raw)?;
        if rules.is_empty() {
            return Ok(None);
        }
        Ok(Some(Box::new(PollValidated {
            source: Arc::clone(&self.source),
            default_interval: self.default_interval,
            rules,
        })))
    }
}

struct PollValidated<S: PollSource> {
    source: Arc<S>,
    default_interval: Duration,
    rules: ValidatedPoll<S::Item>,
}

#[async_trait]
impl<S: PollSource> ValidatedPipeline for PollValidated<S> {
    fn rule_ids(&self) -> Vec<RuleId> {
        self.rules.ids()
    }

    fn report(&self) -> PipelineReport {
        PipelineReport {
            kind: PipelineKind::Poll,
            parser: std::any::type_name::<S>()
                .rsplit("::")
                .next()
                .unwrap_or("poll")
                .to_string(),
            rules: self.rules.ids(),
            stream: None,
            polls: self
                .rules
                .schedule(self.default_interval)
                .into_iter()
                .map(|(interval, (rules, accounts))| PollReport {
                    interval,
                    rules,
                    accounts,
                })
                .collect(),
        }
    }

    async fn start(
        self: Box<Self>,
        program: Program,
        deps: &Deps,
        ctx: &StartCtx<'_>,
        _out: &mut StartedProgram,
    ) -> Result<(), RuleConfigError> {
        let buckets = self
            .rules
            .build(self.default_interval, deps, &ctx.bootstrap)
            .await?;
        for bucket in buckets {
            spawn_poll_loop(
                program,
                Arc::clone(&self.source),
                bucket,
                Arc::clone(&ctx.bootstrap.rpc),
                ctx.alerts_tx.clone(),
                ctx.cancel_token.clone(),
                ctx.tracker,
            );
        }
        Ok(())
    }
}

// ---- public surface ---------------------------------------------------------

/// One monitored program: optional shared deps and the pipelines that feed its rules.
pub struct ProgramSpec {
    program: Program,
    deps: DepsBootstrap,
    pipelines: Vec<Box<dyn Pipeline>>,
}

impl ProgramSpec {
    pub fn new(program: Program) -> Self {
        Self {
            program,
            deps: bootstrap_deps::<()>,
            pipelines: Vec::new(),
        }
    }

    pub fn program(&self) -> Program {
        self.program
    }

    /// Give the program shared state; rules reach it with `deps.get::<D>()` in `build`.
    #[must_use]
    pub fn deps<D: ProgramDeps>(mut self) -> Self {
        self.deps = bootstrap_deps::<D>;
        self
    }

    fn stream<P>(
        mut self,
        parser: P,
        rules: StreamRules<P::Output>,
        wrap: fn(BoxPipeline<'static, P::Input>) -> StreamPipeline,
    ) -> Self
    where
        P: Parser + Clone + Debug + Send + Sync + 'static,
        P::Input: StreamInput,
        P::Output: Send + Sync + 'static,
    {
        self.pipelines.push(Box::new(StreamSpec {
            parser,
            rules,
            wrap,
        }));
        self
    }

    /// An account-update pipeline. Rules must implement `StreamRule<Event = P::Output>`.
    #[must_use]
    pub fn account<P>(self, parser: P, rules: StreamRules<P::Output>) -> Self
    where
        P: Parser<Input = shipstern_core::AccountUpdate> + Clone + Debug + Send + Sync + 'static,
        P::Output: Send + Sync + 'static,
    {
        self.stream(parser, rules, StreamPipeline::Account)
    }

    /// An instruction pipeline, gated locally on the transaction accounts of its subscription.
    #[must_use]
    pub fn instruction<P>(self, parser: P, rules: StreamRules<P::Output>) -> Self
    where
        P: Parser<Input = shipstern_core::instruction::InstructionUpdate>
            + Clone
            + Debug
            + Send
            + Sync
            + 'static,
        P::Output: Send + Sync + 'static,
    {
        self.stream(parser, rules, StreamPipeline::Instruction)
    }

    /// A whole-transaction pipeline.
    #[must_use]
    pub fn transaction<P>(self, parser: P, rules: StreamRules<P::Output>) -> Self
    where
        P: Parser<Input = shipstern_core::TransactionUpdate>
            + Clone
            + Debug
            + Send
            + Sync
            + 'static,
        P::Output: Send + Sync + 'static,
    {
        self.stream(parser, rules, StreamPipeline::Transaction)
    }

    /// A poll feed: `getMultipleAccounts` on `default_interval` unless a rule asks for its own cadence.
    #[must_use]
    pub fn poll<S: PollSource>(
        mut self,
        source: S,
        default_interval: Duration,
        rules: PollRules<S::Item>,
    ) -> Self {
        self.pipelines.push(Box::new(PollSpec {
            source: Arc::new(source),
            default_interval,
            rules,
        }));
        self
    }

    /// Every rule this program can run, whether or not it is configured.
    pub fn rule_ids(&self) -> Vec<RuleId> {
        self.pipelines.iter().flat_map(|p| p.rule_ids()).collect()
    }

    /// Typed-config validation of the configured rules. No I/O; errors name program, rule and field.
    pub fn validate(&self, raw: &RawRuleMap) -> Result<ValidatedProgram, RuleConfigError> {
        let known: HashSet<&'static str> = self.rule_ids().into_iter().map(|id| id.name).collect();
        if let Some(unknown) = raw.keys().find(|name| !known.contains(name.as_str())) {
            return Err(RuleConfigError::UnknownRule {
                program: self.program,
                rule: unknown.clone(),
            });
        }
        let mut pipelines = Vec::with_capacity(self.pipelines.len());
        for spec in &self.pipelines {
            if let Some(validated) = spec.validate(self.program, raw)? {
                pipelines.push(validated);
            }
        }
        Ok(ValidatedProgram {
            program: self.program,
            deps: self.deps,
            pipelines,
        })
    }
}

/// A program whose configured rules passed typed validation; nothing has connected yet.
pub struct ValidatedProgram {
    program: Program,
    deps: DepsBootstrap,
    pipelines: Vec<Box<dyn ValidatedPipeline>>,
}

impl ValidatedProgram {
    pub fn program(&self) -> Program {
        self.program
    }

    /// The configured rules, in registration order.
    pub fn rule_ids(&self) -> Vec<RuleId> {
        self.pipelines.iter().flat_map(|p| p.rule_ids()).collect()
    }

    pub fn report(&self) -> ProgramReport {
        ProgramReport {
            program: self.program,
            pipelines: self.pipelines.iter().map(|p| p.report()).collect(),
        }
    }

    /// Bootstrap deps, then every rule; build the stream pipelines and spawn poll / sweep loops.
    pub async fn start(self, ctx: &StartCtx<'_>) -> anyhow::Result<StartedProgram> {
        let started = tokio::time::Instant::now();
        let deps = Deps((self.deps)(&ctx.bootstrap).await?);
        let mut out = StartedProgram {
            program: self.program,
            deps: Arc::clone(&deps.0),
            pipelines: Vec::new(),
            streams: Vec::new(),
        };
        for pipeline in self.pipelines {
            pipeline.start(self.program, &deps, ctx, &mut out).await?;
        }
        info!(
            program = %self.program,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "Program started"
        );
        Ok(out)
    }
}

/// A program after `start`: its stream pipelines for the runtime builder, plus
/// what the refresh loop needs.
pub struct StartedProgram {
    pub program: Program,
    pub deps: Arc<dyn ProgramDeps>,
    pub pipelines: Vec<StreamPipeline>,
    pub(crate) streams: Vec<InstalledStream>,
}

/// One stream pipeline as built, enough to recompute its filters on refresh.
pub(crate) struct InstalledStream {
    pub kind: PipelineKind,
    pub base_id: String,
    pub parser_prefilter: shipstern_core::Prefilter,
    pub static_subscription: Subscription,
    pub subscriptions: usize,
}

// ---- check report -----------------------------------------------------------

pub struct ProgramReport {
    pub program: Program,
    pub pipelines: Vec<PipelineReport>,
}

pub struct PipelineReport {
    pub kind: PipelineKind,
    pub parser: String,
    pub rules: Vec<RuleId>,
    pub stream: Option<StreamReport>,
    pub polls: Vec<PollReport>,
}

pub struct StreamReport {
    pub per_rule: Vec<(RuleId, Subscription)>,
    pub union: Subscription,
}

pub struct PollReport {
    pub interval: Duration,
    pub rules: Vec<RuleId>,
    pub accounts: Vec<Pubkey>,
}

impl Display for ProgramReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "program {}", self.program)?;
        for p in &self.pipelines {
            writeln!(f, "  {} pipeline `{}`", p.kind.as_str(), p.parser)?;
            for rule in &p.rules {
                writeln!(f, "    rule {}", rule.name)?;
            }
            if let Some(stream) = &p.stream {
                for (id, pf) in &stream.per_rule {
                    writeln!(f, "    {}: {}", id.name, describe(pf))?;
                }
                writeln!(f, "    subscription: {}", describe(&stream.union))?;
            }
            for poll in &p.polls {
                writeln!(
                    f,
                    "    every {}s: {} accounts for {:?}",
                    poll.interval.as_secs(),
                    poll.accounts.len(),
                    poll.rules.iter().map(|r| r.name).collect::<Vec<_>>()
                )?;
                for a in &poll.accounts {
                    writeln!(f, "      {a}")?;
                }
            }
        }
        Ok(())
    }
}

fn describe(sub: &Subscription) -> String {
    let mut parts = Vec::new();
    if !sub.accounts.is_empty() {
        parts.push(format!("accounts={}", sub.accounts.len()));
    }
    for owned in &sub.owned {
        parts.push(format!(
            "owners={:?} filters={}",
            owned
                .owners
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>(),
            owned.filters.len()
        ));
    }
    if !sub.tx_accounts_include.is_empty() {
        parts.push(format!("tx_include={}", sub.tx_accounts_include.len()));
    }
    if !sub.tx_accounts_required.is_empty() {
        parts.push(format!("tx_required={}", sub.tx_accounts_required.len()));
    }
    if parts.is_empty() {
        "(empty)".to_string()
    } else {
        parts.join(" ")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raw::{RawInstruction, RawInstructionParser};
    use crate::rule::{BuiltStreamRule, NoConfig, StreamCtx, StreamRule};
    use crate::testing::ix_update;
    use crate::Alert;
    use shipstern::Handler;
    use solana_pubkey::Pubkey;

    const PROGRAM: Program = Program::new("test_program");
    const WATCHED: Pubkey = Pubkey::new_from_array([7; 32]);

    #[derive(Debug)]
    struct AnyIx;

    impl StreamRule for AnyIx {
        const ID: RuleId = RuleId::new(PROGRAM, "any_ix");
        type Config = NoConfig;
        type Event = RawInstruction;

        fn subscription(_: &NoConfig) -> Subscription {
            Subscription::new().tx_accounts_include([WATCHED])
        }

        fn build(_: NoConfig, _: &Deps) -> anyhow::Result<Self> {
            Ok(Self)
        }

        fn evaluate(&self, ix: &RawInstruction, ctx: &StreamCtx) -> Option<Alert> {
            Some(
                Alert::for_rule(Self::ID)
                    .message(format!("{} at {}", ix.program_id, ctx.slot))
                    .key([ix.program_id])
                    .scope(ctx.tx_scope())
                    .build(),
            )
        }
    }

    /// validate → static subscription → build → handler evaluates and forwards, with `tx_sig` filled in.
    #[tokio::test]
    async fn validated_rule_fires_through_handler() {
        let spec = ProgramSpec::new(PROGRAM)
            .instruction(RawInstructionParser, StreamRules::new().register::<AnyIx>());

        let raw = RawRuleMap::from([(
            "any_ix".to_string(),
            config::Value::from(config::Map::<String, config::Value>::new()),
        )]);
        let validated = spec.validate(&raw).expect("config validates");
        assert_eq!(validated.rule_ids(), vec![AnyIx::ID]);
        let union = validated.report().pipelines[0]
            .stream
            .as_ref()
            .unwrap()
            .union
            .clone();
        assert!(union.tx_accounts_include.contains(&WATCHED));

        let unknown = RawRuleMap::from([("nope".to_string(), config::Value::from(1))]);
        assert!(matches!(
            spec.validate(&unknown),
            Err(RuleConfigError::UnknownRule { .. })
        ));

        let (tx, mut rx) = mpsc::channel(4);
        let handler = RuleHandler::new(vec![BuiltStreamRule::new(AnyIx, None)], tx);
        let update = ix_update(Pubkey::new_unique(), &[WATCHED], vec![1, 2, 3]);
        let event = RawInstructionParser.parse(&update).await.unwrap();
        handler.handle(&event, &update).await.unwrap();

        let alert = rx.try_recv().expect("alert forwarded");
        assert_eq!(alert.rule, AnyIx::ID);
        assert!(alert.metadata.contains_key("tx_sig"));
    }
}
