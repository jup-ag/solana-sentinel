//! What a program crate exports: a [`ProgramSpec`] built from a parser per
//! feed and the rules registered on it. The service drives it through
//! validate → bootstrap → install; the deps type is erased so every program
//! fits one registry entry, and the typed builder keeps rule/deps mismatches
//! a compile error.

use crate::deps::ProgramDeps;
use crate::handler::RuleHandler;
use crate::parser::{
    scoped_parser_id, FrameworkParser, PipelineKind, PipelineSink, StreamInput, TxGate,
};
use crate::poll::{spawn_poll_loop, PollSource};
use crate::rule::{BuiltStreamRule, RuleConfigError};
use crate::rule_set::{PollBucket, PollRules, StreamRules, ValidatedPoll, ValidatedStream};
use crate::subscription::Subscription;
use crate::sweep::spawn_sweep_loops;
use crate::{BootstrapCtx, RawRuleMap};
use async_trait::async_trait;
use domain::alert::Alert;
use domain::programs::Program;
use domain::rules::RuleId;
use shipstern_core::Parser;
use solana_pubkey::Pubkey;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use std::any::Any;
use std::collections::HashSet;
use std::fmt::{self, Debug, Display};
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{info, warn};

// ---- deps erasure -----------------------------------------------------------

#[async_trait]
pub(crate) trait DynDeps: Send + Sync {
    fn as_any(&self) -> &(dyn Any + Send + Sync);
    fn dynamic_subscription(&self) -> Subscription;
    fn refresh_interval(&self) -> Option<Duration>;
    async fn refresh(&self, ctx: &BootstrapCtx) -> anyhow::Result<()>;
}

struct DepsCell<D>(D);

#[async_trait]
impl<D: ProgramDeps> DynDeps for DepsCell<D> {
    fn as_any(&self) -> &(dyn Any + Send + Sync) {
        &self.0
    }

    fn dynamic_subscription(&self) -> Subscription {
        self.0.dynamic_subscription()
    }

    fn refresh_interval(&self) -> Option<Duration> {
        self.0.refresh_interval()
    }

    async fn refresh(&self, ctx: &BootstrapCtx) -> anyhow::Result<()> {
        self.0.refresh(ctx).await
    }
}

#[async_trait]
trait DepsFactory: Send + Sync {
    async fn bootstrap(&self, ctx: &BootstrapCtx) -> anyhow::Result<Arc<dyn DynDeps>>;
}

struct Factory<D>(PhantomData<fn() -> D>);

#[async_trait]
impl<D: ProgramDeps> DepsFactory for Factory<D> {
    async fn bootstrap(&self, ctx: &BootstrapCtx) -> anyhow::Result<Arc<dyn DynDeps>> {
        Ok(Arc::new(DepsCell(D::bootstrap(ctx).await?)))
    }
}

// ---- pipeline erasure -------------------------------------------------------

trait PipelineSpec: Send + Sync {
    fn rule_ids(&self) -> Vec<RuleId>;
    fn validate(
        &self,
        program: Program,
        raw: &RawRuleMap,
    ) -> Result<Box<dyn ValidatedPipeline>, RuleConfigError>;
}

#[async_trait]
trait ValidatedPipeline: Send {
    fn is_empty(&self) -> bool;
    fn rule_ids(&self) -> Vec<RuleId>;
    fn report(&self) -> PipelineReport;
    async fn build(
        self: Box<Self>,
        deps: &dyn DynDeps,
        ctx: &BootstrapCtx,
    ) -> Result<Box<dyn BuiltPipeline>, RuleConfigError>;
}

trait BuiltPipeline: Send {
    fn install(self: Box<Self>, target: &mut InstallTarget<'_>);
}

fn typed_deps<D: ProgramDeps>(deps: &dyn DynDeps) -> &D {
    deps.as_any()
        .downcast_ref::<D>()
        .expect("program deps type mismatch; ProgramBuilder ties rules to their deps type")
}

// stream

struct StreamSpec<P: Parser, D> {
    parser: P,
    rules: StreamRules<P::Output, D>,
}

impl<P, D> PipelineSpec for StreamSpec<P, D>
where
    P: Parser + Clone + Debug + Send + Sync + 'static,
    P::Input: StreamInput,
    P::Output: Send + Sync + 'static,
    D: ProgramDeps,
{
    fn rule_ids(&self) -> Vec<RuleId> {
        self.rules.ids()
    }

    fn validate(
        &self,
        program: Program,
        raw: &RawRuleMap,
    ) -> Result<Box<dyn ValidatedPipeline>, RuleConfigError> {
        Ok(Box::new(StreamValidated {
            parser: self.parser.clone(),
            rules: self.rules.validate(program, raw)?,
        }))
    }
}

struct StreamValidated<P: Parser, D> {
    parser: P,
    rules: ValidatedStream<P::Output, D>,
}

#[async_trait]
impl<P, D> ValidatedPipeline for StreamValidated<P, D>
where
    P: Parser + Clone + Debug + Send + Sync + 'static,
    P::Input: StreamInput,
    P::Output: Send + Sync + 'static,
    D: ProgramDeps,
{
    fn is_empty(&self) -> bool {
        self.rules.is_empty()
    }

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

    async fn build(
        self: Box<Self>,
        deps: &dyn DynDeps,
        ctx: &BootstrapCtx,
    ) -> Result<Box<dyn BuiltPipeline>, RuleConfigError> {
        let static_subscription = self.rules.static_subscription();
        let rules = self.rules.build(typed_deps::<D>(deps), ctx).await?;
        Ok(Box::new(StreamBuilt {
            parser: self.parser,
            static_subscription,
            rules,
        }))
    }
}

struct StreamBuilt<P: Parser> {
    parser: P,
    static_subscription: Subscription,
    rules: Vec<BuiltStreamRule<P::Output>>,
}

impl<P> BuiltPipeline for StreamBuilt<P>
where
    P: Parser + Clone + Debug + Send + Sync + 'static,
    P::Input: StreamInput,
    P::Output: Send + Sync + 'static,
{
    fn install(self: Box<Self>, target: &mut InstallTarget<'_>) {
        let Self {
            parser,
            static_subscription,
            rules: built,
        } = *self;
        let rules: Vec<_> = built.iter().map(|b| Arc::clone(&b.rule)).collect();
        let program = target.program;
        let base_id = parser.id().into_owned();
        let subscription = static_subscription
            .clone()
            .union(target.dynamic_subscription.clone());
        let subs = <P::Input as StreamInput>::prefilters(&subscription);

        if subs.is_empty() {
            warn!(
                program = %program,
                parser = %base_id,
                rules = ?rules.iter().map(|r| r.id().name).collect::<Vec<_>>(),
                "Pipeline resolved to an empty subscription; its rules will never fire"
            );
            return;
        }

        let handler = RuleHandler::new(rules.clone(), target.alerts_tx.clone());
        let gate = <P::Input as StreamInput>::needs_gate()
            .then(|| TxGate::from_subscription(&subscription));
        let subscriptions = subs.len();

        for (i, sub) in subs.into_iter().enumerate() {
            let scoped = FrameworkParser::new(
                parser.clone(),
                scoped_parser_id(program, &base_id, i),
                gate.clone(),
            );
            <P::Input as StreamInput>::add_pipeline(target.sink, scoped, handler.clone(), sub);
        }
        target.grpc_pipelines += subscriptions;

        info!(
            program = %program,
            kind = P::Input::KIND.as_str(),
            parser = %base_id,
            subscriptions,
            rules = ?rules.iter().map(|r| r.id().name).collect::<Vec<_>>(),
            "Pipeline installed"
        );

        spawn_sweep_loops(
            &built,
            &target.alerts_tx,
            &target.cancel_token,
            target.tracker,
        );

        target.installed.push(InstalledStream {
            kind: P::Input::KIND,
            base_id,
            parser_prefilter: parser.prefilter(),
            static_subscription,
            subscriptions,
        });
    }
}

// poll

struct PollSpec<S: PollSource, D> {
    source: Arc<S>,
    default_interval: Duration,
    rules: PollRules<S::Item, D>,
}

impl<S: PollSource, D: ProgramDeps> PipelineSpec for PollSpec<S, D> {
    fn rule_ids(&self) -> Vec<RuleId> {
        self.rules.ids()
    }

    fn validate(
        &self,
        program: Program,
        raw: &RawRuleMap,
    ) -> Result<Box<dyn ValidatedPipeline>, RuleConfigError> {
        Ok(Box::new(PollValidated {
            source: Arc::clone(&self.source),
            default_interval: self.default_interval,
            rules: self.rules.validate(program, raw)?,
        }))
    }
}

struct PollValidated<S: PollSource, D> {
    source: Arc<S>,
    default_interval: Duration,
    rules: ValidatedPoll<S::Item, D>,
}

#[async_trait]
impl<S: PollSource, D: ProgramDeps> ValidatedPipeline for PollValidated<S, D> {
    fn is_empty(&self) -> bool {
        self.rules.is_empty()
    }

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

    async fn build(
        self: Box<Self>,
        deps: &dyn DynDeps,
        ctx: &BootstrapCtx,
    ) -> Result<Box<dyn BuiltPipeline>, RuleConfigError> {
        let buckets = self
            .rules
            .build(self.default_interval, typed_deps::<D>(deps), ctx)
            .await?;
        Ok(Box::new(PollBuilt {
            source: self.source,
            buckets,
        }))
    }
}

struct PollBuilt<S: PollSource> {
    source: Arc<S>,
    buckets: Vec<PollBucket<S::Item>>,
}

impl<S: PollSource> BuiltPipeline for PollBuilt<S> {
    fn install(self: Box<Self>, target: &mut InstallTarget<'_>) {
        for bucket in self.buckets {
            spawn_poll_loop(
                target.program,
                Arc::clone(&self.source),
                bucket,
                Arc::clone(&target.rpc),
                target.alerts_tx.clone(),
                target.cancel_token.clone(),
                target.tracker,
            );
        }
    }
}

// ---- public surface ---------------------------------------------------------

/// One monitored program: its shared deps and the pipelines that feed its rules.
pub struct ProgramSpec {
    program: Program,
    deps: Arc<dyn DepsFactory>,
    pipelines: Vec<Box<dyn PipelineSpec>>,
}

/// Typed builder; `D` is the program's [`ProgramDeps`] and every registered rule must use it.
pub struct ProgramBuilder<D> {
    program: Program,
    pipelines: Vec<Box<dyn PipelineSpec>>,
    _deps: PhantomData<fn() -> D>,
}

impl ProgramSpec {
    pub fn builder(program: Program) -> ProgramBuilder<()> {
        ProgramBuilder {
            program,
            pipelines: Vec::new(),
            _deps: PhantomData,
        }
    }

    pub fn program(&self) -> Program {
        self.program
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
            let validated = spec.validate(self.program, raw)?;
            if !validated.is_empty() {
                pipelines.push(validated);
            }
        }
        Ok(ValidatedProgram {
            program: self.program,
            deps: Arc::clone(&self.deps),
            pipelines,
        })
    }
}

impl ProgramBuilder<()> {
    /// Give the program shared state. Call before registering any pipeline.
    pub fn deps<D: ProgramDeps>(self) -> ProgramBuilder<D> {
        assert!(
            self.pipelines.is_empty(),
            "ProgramSpec::deps must be called before any pipeline is registered"
        );
        ProgramBuilder {
            program: self.program,
            pipelines: Vec::new(),
            _deps: PhantomData,
        }
    }
}

impl<D: ProgramDeps> ProgramBuilder<D> {
    fn stream<P>(mut self, parser: P, rules: StreamRules<P::Output, D>) -> Self
    where
        P: Parser + Clone + Debug + Send + Sync + 'static,
        P::Input: StreamInput,
        P::Output: Send + Sync + 'static,
    {
        self.pipelines.push(Box::new(StreamSpec { parser, rules }));
        self
    }

    /// An account-update pipeline. Rules must implement `GrpcRule<Event = P::Output>`.
    #[must_use]
    pub fn account<P>(self, parser: P, rules: StreamRules<P::Output, D>) -> Self
    where
        P: Parser<Input = shipstern_core::AccountUpdate> + Clone + Debug + Send + Sync + 'static,
        P::Output: Send + Sync + 'static,
    {
        self.stream(parser, rules)
    }

    /// An instruction pipeline, gated locally on the transaction accounts of its subscription.
    #[must_use]
    pub fn instruction<P>(self, parser: P, rules: StreamRules<P::Output, D>) -> Self
    where
        P: Parser<Input = shipstern_core::instruction::InstructionUpdate>
            + Clone
            + Debug
            + Send
            + Sync
            + 'static,
        P::Output: Send + Sync + 'static,
    {
        self.stream(parser, rules)
    }

    /// A whole-transaction pipeline.
    #[must_use]
    pub fn transaction<P>(self, parser: P, rules: StreamRules<P::Output, D>) -> Self
    where
        P: Parser<Input = shipstern_core::TransactionUpdate>
            + Clone
            + Debug
            + Send
            + Sync
            + 'static,
        P::Output: Send + Sync + 'static,
    {
        self.stream(parser, rules)
    }

    /// A poll feed: `getMultipleAccounts` on `default_interval` unless a rule asks for its own cadence.
    #[must_use]
    pub fn poll<S: PollSource>(
        mut self,
        source: S,
        default_interval: Duration,
        rules: PollRules<S::Item, D>,
    ) -> Self {
        self.pipelines.push(Box::new(PollSpec {
            source: Arc::new(source),
            default_interval,
            rules,
        }));
        self
    }

    pub fn finish(self) -> ProgramSpec {
        ProgramSpec {
            program: self.program,
            deps: Arc::new(Factory::<D>(PhantomData)),
            pipelines: self.pipelines,
        }
    }
}

/// A program whose configured rules passed typed validation; nothing has connected yet.
pub struct ValidatedProgram {
    program: Program,
    deps: Arc<dyn DepsFactory>,
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

    /// `Deps::bootstrap`, then build + bootstrap every configured rule.
    pub async fn bootstrap(self, ctx: &BootstrapCtx) -> anyhow::Result<BootstrappedProgram> {
        let started = tokio::time::Instant::now();
        let deps = self.deps.bootstrap(ctx).await?;
        let mut pipelines = Vec::with_capacity(self.pipelines.len());
        for pipeline in self.pipelines {
            pipelines.push(pipeline.build(deps.as_ref(), ctx).await?);
        }
        info!(
            program = %self.program,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "Program bootstrapped"
        );
        Ok(BootstrappedProgram {
            program: self.program,
            deps,
            pipelines,
        })
    }
}

pub struct BootstrappedProgram {
    program: Program,
    deps: Arc<dyn DynDeps>,
    pipelines: Vec<Box<dyn BuiltPipeline>>,
}

impl BootstrappedProgram {
    pub fn program(&self) -> Program {
        self.program
    }

    /// Add the stream pipelines to the dispatcher and spawn poll / sweep loops.
    pub fn install(self, target: &mut InstallTarget<'_>) -> InstalledProgram {
        target.program = self.program;
        target.dynamic_subscription = self.deps.dynamic_subscription();
        target.installed = Vec::new();
        for pipeline in self.pipelines {
            pipeline.install(target);
        }
        InstalledProgram {
            program: self.program,
            deps: self.deps,
            streams: std::mem::take(&mut target.installed),
        }
    }
}

/// Everything installing needs, plus the runtime builder the stream pipelines go into.
pub struct InstallTarget<'a> {
    pub(crate) sink: &'a mut dyn PipelineSink,
    pub(crate) rpc: Arc<RpcClient>,
    pub(crate) alerts_tx: mpsc::Sender<Alert>,
    pub(crate) cancel_token: CancellationToken,
    pub(crate) tracker: &'a TaskTracker,
    grpc_pipelines: usize,
    program: Program,
    dynamic_subscription: Subscription,
    installed: Vec<InstalledStream>,
}

impl<'a> InstallTarget<'a> {
    pub fn new(
        sink: &'a mut dyn PipelineSink,
        rpc: Arc<RpcClient>,
        alerts_tx: mpsc::Sender<Alert>,
        cancel_token: CancellationToken,
        tracker: &'a TaskTracker,
    ) -> Self {
        Self {
            sink,
            rpc,
            alerts_tx,
            cancel_token,
            tracker,
            grpc_pipelines: 0,
            program: Program::new(""),
            dynamic_subscription: Subscription::default(),
            installed: Vec::new(),
        }
    }

    /// How many stream pipelines were added to the sink.
    pub fn finish(self) -> usize {
        self.grpc_pipelines
    }
}

/// One stream pipeline as installed, enough to recompute its filters on refresh.
pub(crate) struct InstalledStream {
    pub kind: PipelineKind,
    pub base_id: String,
    pub parser_prefilter: shipstern_core::Prefilter,
    pub static_subscription: Subscription,
    pub subscriptions: usize,
}

/// A program after install; carries what the refresh loop needs.
pub struct InstalledProgram {
    pub(crate) program: Program,
    pub(crate) deps: Arc<dyn DynDeps>,
    pub(crate) streams: Vec<InstalledStream>,
}

impl InstalledProgram {
    pub fn program(&self) -> Program {
        self.program
    }

    pub fn refresh_interval(&self) -> Option<Duration> {
        self.deps.refresh_interval()
    }
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
    use crate::handler::RuleHandler;
    use crate::raw::{RawInstruction, RawInstructionParser};
    use crate::rule::{DynGrpcRule, GrpcRule, NoConfig, RuleDef, StreamCtx};
    use crate::rule_set::StreamRules;
    use crate::testing::ix_update;
    use crate::Alert;
    use shipstern::Handler;
    use solana_pubkey::Pubkey;

    const PROGRAM: Program = Program::new("test_program");
    const WATCHED: Pubkey = Pubkey::new_from_array([7; 32]);

    #[derive(Debug)]
    struct AnyIx;

    impl RuleDef for AnyIx {
        const ID: RuleId = RuleId::new(PROGRAM, "any_ix");
        type Config = NoConfig;
        type Deps = ();

        fn build(_: NoConfig, _: &()) -> anyhow::Result<Self> {
            Ok(Self)
        }
    }

    impl GrpcRule for AnyIx {
        type Event = RawInstruction;

        fn static_subscription(_: &NoConfig) -> Subscription {
            Subscription::new().tx_accounts_include([WATCHED])
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
        let spec = ProgramSpec::builder(PROGRAM)
            .instruction(RawInstructionParser, StreamRules::new().register::<AnyIx>())
            .finish();

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

        let rule: Arc<dyn DynGrpcRule<RawInstruction>> = Arc::new(AnyIx);
        let (tx, mut rx) = mpsc::channel(4);
        let handler = RuleHandler::<RawInstruction>::new(vec![rule], tx);
        let update = ix_update(Pubkey::new_unique(), &[WATCHED], vec![1, 2, 3]);
        let event = RawInstructionParser.parse(&update).await.unwrap();
        handler.handle(&event, &update).await.unwrap();

        let alert = rx.try_recv().expect("alert forwarded");
        assert_eq!(alert.rule, AnyIx::ID);
        assert!(alert.metadata.contains_key("tx_sig"));
    }
}
