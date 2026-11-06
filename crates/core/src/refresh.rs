//! Push a changed dynamic subscription to the live stream without a restart.
//!
//! Each tick refreshes the program's deps, recomputes every refreshable stream's
//! subscription (static plus the dynamic parts its [`DynamicScope`](crate::DynamicScope)
//! takes) and sends only the streams whose subscription changed, all in one filter
//! update. Updates are at least [`ProgramDeps::min_push_interval`] apart: a change
//! seen sooner is pushed once the gap has passed.

use crate::dynamic::DynamicSubscription;
use crate::parser::{scoped_parser_id, PipelineKind};
use crate::spec::{InstalledStream, StartedProgram};
use crate::subscription::Subscription;
use crate::BootstrapCtx;
#[cfg(doc)]
use crate::ProgramDeps;
use domain::programs::Program;
use shipstern::{FilterUpdateError, RuntimeHandle};
use shipstern_core::Prefilter;
use std::time::Duration;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{error, info, warn};

pub fn spawn_refresh_loops(
    programs: Vec<StartedProgram>,
    handle: &RuntimeHandle,
    ctx: &BootstrapCtx,
    cancel_token: &CancellationToken,
    tracker: &TaskTracker,
) {
    for program in programs {
        let Some(interval) = program.deps.refresh_interval() else {
            continue;
        };
        let min_gap = program.deps.min_push_interval();
        info!(
            program = %program.program,
            interval_secs = interval.as_secs(),
            min_push_secs = min_gap.as_secs(),
            "Starting subscription refresh loop"
        );
        let handle = handle.clone();
        let ctx = ctx.clone();
        let cancel_token = cancel_token.clone();
        let mut tick = tokio::time::interval(interval);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        tick.reset(); // the first tick fires immediately otherwise; bootstrap just ran

        tracker.spawn(async move {
            let mut state = RefreshState::new(&program.streams, min_gap);
            loop {
                let deferred = state.deferred_until();
                tokio::select! {
                    _ = cancel_token.cancelled() => break,
                    _ = tick.tick() => {
                        if let Err(e) = program.deps.refresh(&ctx).await {
                            error!(program = %program.program, error = format!("{e:#}"), "Deps refresh failed");
                            continue;
                        }
                    }
                    // A change held back by the push gap; nothing to re-read, just send it.
                    _ = sleep_until_some(deferred) => {}
                }
                let dynamic = program.deps.tagged_dynamic_subscription();
                state.step(
                    program.program,
                    &program.streams,
                    &dynamic,
                    Instant::now(),
                    &handle,
                );
            }
        });
    }
}

async fn sleep_until_some(at: Option<Instant>) {
    match at {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending().await,
    }
}

/// Where a filter update goes; the runtime handle in production.
pub(crate) trait FilterSink {
    /// Replace the prefilters under these parser ids, in one update.
    fn replace(&self, set: Vec<(String, Prefilter)>) -> Result<(), FilterUpdateError>;
}

impl FilterSink for RuntimeHandle {
    fn replace(&self, set: Vec<(String, Prefilter)>) -> Result<(), FilterUpdateError> {
        self.update_filters(|filters| {
            for (id, prefilter) in set {
                filters.insert(id, prefilter);
            }
        })
    }
}

/// What the refresh loop of one program remembers between ticks.
pub(crate) struct RefreshState {
    /// Per stream (same order as `StartedProgram::streams`): the subscription last on the wire.
    last: Vec<Subscription>,
    min_gap: Duration,
    last_push: Option<Instant>,
    /// A change is waiting for the push gap.
    pending: bool,
}

/// What one tick did; for tests.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum StepOutcome {
    Unchanged,
    /// Changed, but within the push gap; retried when it passes.
    Deferred,
    /// One update sent, naming these parser bases.
    Pushed(Vec<String>),
    /// Changed only in ways that need a restart (prefilter count), or the update failed.
    NotPushed,
}

impl RefreshState {
    pub(crate) fn new(streams: &[InstalledStream], min_gap: Duration) -> Self {
        Self {
            last: streams.iter().map(|s| s.subscription.clone()).collect(),
            min_gap,
            last_push: None,
            pending: false,
        }
    }

    /// When to wake up for a change the push gap held back.
    pub(crate) fn deferred_until(&self) -> Option<Instant> {
        match (self.pending, self.last_push) {
            (true, Some(at)) => Some(at + self.min_gap),
            _ => None,
        }
    }

    /// Streams whose subscription `dynamic` changes, with their new subscription.
    fn changes(
        &self,
        streams: &[InstalledStream],
        dynamic: &DynamicSubscription,
    ) -> Vec<(usize, Subscription)> {
        streams
            .iter()
            .enumerate()
            // Instruction parsers share one bundled filter across programs; not refreshable per program.
            .filter(|(_, s)| s.kind != PipelineKind::Instruction)
            .filter_map(|(i, s)| {
                let next = s
                    .static_subscription
                    .clone()
                    .union(dynamic.select(&s.dynamic));
                (next != self.last[i]).then_some((i, next))
            })
            .collect()
    }

    pub(crate) fn step(
        &mut self,
        program: Program,
        streams: &[InstalledStream],
        dynamic: &DynamicSubscription,
        now: Instant,
        sink: &impl FilterSink,
    ) -> StepOutcome {
        let changes = self.changes(streams, dynamic);
        if changes.is_empty() {
            self.pending = false;
            return StepOutcome::Unchanged;
        }
        if self
            .last_push
            .is_some_and(|at| now.duration_since(at) < self.min_gap)
        {
            self.pending = true;
            return StepOutcome::Deferred;
        }
        self.pending = false;

        let mut set = Vec::new();
        let mut sent = Vec::new();
        for (i, next) in changes {
            let stream = &streams[i];
            let subs = stream.kind.prefilters(&next);
            if subs.len() != stream.subscriptions {
                warn!(
                    program = %program,
                    parser = %stream.base_id,
                    "Dynamic subscription changed the prefilter count; restart to apply"
                );
                // Warn once per change, not on every tick.
                self.last[i] = next;
                continue;
            }
            let mut built = Vec::with_capacity(subs.len());
            for (n, sub) in subs.into_iter().enumerate() {
                match sub.build() {
                    Ok(extra) => {
                        let mut merged = stream.parser_prefilter.clone();
                        merged.merge(extra);
                        built.push((scoped_parser_id(program, &stream.base_id, n), merged));
                    }
                    Err(e) => {
                        warn!(error = %e, parser = %stream.base_id, "Invalid refreshed prefilter; skipping")
                    }
                }
            }
            if built.is_empty() {
                self.last[i] = next;
                continue;
            }
            set.extend(built);
            sent.push((i, next));
        }
        if sent.is_empty() {
            return StepOutcome::NotPushed;
        }

        let bases: Vec<String> = sent
            .iter()
            .map(|(i, _)| streams[*i].base_id.clone())
            .collect();
        match sink.replace(set) {
            Ok(()) => {
                info!(program = %program, parsers = ?bases, "Pushed refreshed subscription");
                for (i, next) in sent {
                    self.last[i] = next;
                }
                self.last_push = Some(now);
                StepOutcome::Pushed(bases)
            }
            Err(e) => {
                // `last` keeps the old sets, so the next tick retries.
                error!(program = %program, error = %e, "Failed to push refreshed subscription");
                StepOutcome::NotPushed
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dynamic::{DynamicScope, DynamicTag};
    use solana_pubkey::Pubkey;
    use std::cell::RefCell;

    const PROGRAM: Program = Program::new("refresh_test");
    const ORACLES: DynamicTag = DynamicTag::new("oracles");
    const FOLLOW: DynamicTag = DynamicTag::new("follow");
    const GAP: Duration = Duration::from_secs(10);

    fn key(n: u8) -> Pubkey {
        Pubkey::new_from_array([n; 32])
    }

    #[derive(Default)]
    struct Recorder(RefCell<Vec<Vec<String>>>);

    impl FilterSink for Recorder {
        fn replace(&self, set: Vec<(String, Prefilter)>) -> Result<(), FilterUpdateError> {
            self.0
                .borrow_mut()
                .push(set.into_iter().map(|(id, _)| id).collect());
            Ok(())
        }
    }

    fn stream(
        kind: PipelineKind,
        base_id: &str,
        static_subscription: Subscription,
        dynamic: DynamicScope,
        built_with: &DynamicSubscription,
    ) -> InstalledStream {
        let subscription = static_subscription
            .clone()
            .union(built_with.select(&dynamic));
        InstalledStream {
            kind,
            base_id: base_id.to_string(),
            parser_prefilter: Prefilter::default(),
            subscriptions: kind.prefilters(&subscription).len(),
            static_subscription,
            dynamic,
            subscription,
        }
    }

    fn dynamic(oracles: &[u8], follow: &[u8]) -> DynamicSubscription {
        DynamicSubscription::new()
            .tag(
                ORACLES,
                Subscription::new().accounts(oracles.iter().map(|n| key(*n))),
            )
            .tag(
                FOLLOW,
                Subscription::new().tx_accounts_include(follow.iter().map(|n| key(*n))),
            )
    }

    /// Three pipelines: oracle accounts, followed-wallet transactions, and one with no dynamic part.
    fn streams(built_with: &DynamicSubscription) -> Vec<InstalledStream> {
        vec![
            stream(
                PipelineKind::Account,
                "oracle_accounts",
                Subscription::new().accounts([key(100)]),
                DynamicScope::only([ORACLES]),
                built_with,
            ),
            stream(
                PipelineKind::Transaction,
                "follow_txs",
                Subscription::new().tx_accounts_include([key(101)]),
                DynamicScope::only([FOLLOW]),
                built_with,
            ),
            stream(
                PipelineKind::Account,
                "static_only",
                Subscription::new().accounts([key(102)]),
                DynamicScope::none(),
                built_with,
            ),
        ]
    }

    #[test]
    fn no_push_when_unchanged() {
        let built = dynamic(&[1, 2], &[3]);
        let streams = streams(&built);
        let mut state = RefreshState::new(&streams, GAP);
        let sink = Recorder::default();
        let t0 = Instant::now();
        assert_eq!(
            state.step(PROGRAM, &streams, &built, t0, &sink),
            StepOutcome::Unchanged
        );
        // Same sets, rebuilt in another order: still equal.
        let again = DynamicSubscription::new()
            .tag(FOLLOW, Subscription::new().tx_accounts_include([key(3)]))
            .tag(ORACLES, Subscription::new().accounts([key(2), key(1)]));
        assert_eq!(
            state.step(PROGRAM, &streams, &again, t0 + GAP * 5, &sink),
            StepOutcome::Unchanged
        );
        assert!(sink.0.borrow().is_empty());
    }

    #[test]
    fn pushes_only_the_pipeline_whose_tag_changed() {
        let built = dynamic(&[1], &[3]);
        let streams = streams(&built);
        let mut state = RefreshState::new(&streams, GAP);
        let sink = Recorder::default();
        let t0 = Instant::now();

        // A new followed wallet reaches the transaction pipeline only.
        let followed = dynamic(&[1], &[3, 4]);
        assert_eq!(
            state.step(PROGRAM, &streams, &followed, t0, &sink),
            StepOutcome::Pushed(vec!["follow_txs".into()])
        );
        assert_eq!(
            *sink.0.borrow(),
            vec![vec!["refresh_test:follow_txs".to_string()]]
        );

        // Within the gap: held back, and the loop is told when to retry.
        let oracle = dynamic(&[1, 2], &[3, 4]);
        assert_eq!(
            state.step(PROGRAM, &streams, &oracle, t0 + GAP / 2, &sink),
            StepOutcome::Deferred
        );
        assert_eq!(state.deferred_until(), Some(t0 + GAP));
        assert_eq!(sink.0.borrow().len(), 1);

        // Once the gap passed: the oracle pipeline alone, in one update.
        assert_eq!(
            state.step(PROGRAM, &streams, &oracle, t0 + GAP, &sink),
            StepOutcome::Pushed(vec!["oracle_accounts".into()])
        );
        assert_eq!(state.deferred_until(), None);
        assert_eq!(sink.0.borrow().len(), 2);

        // Both changed: one update carrying both, the static-only pipeline never.
        let both = dynamic(&[5], &[6]);
        assert_eq!(
            state.step(PROGRAM, &streams, &both, t0 + GAP * 2, &sink),
            StepOutcome::Pushed(vec!["oracle_accounts".into(), "follow_txs".into()])
        );
        let pushes = sink.0.borrow();
        assert_eq!(pushes.len(), 3);
        assert_eq!(pushes[2].len(), 2);
        assert!(pushes
            .iter()
            .flatten()
            .all(|id| !id.contains("static_only")));
    }

    #[test]
    fn deferred_change_reverted_is_dropped() {
        let built = dynamic(&[1], &[3]);
        let streams = streams(&built);
        let mut state = RefreshState::new(&streams, GAP);
        let sink = Recorder::default();
        let t0 = Instant::now();
        state.step(PROGRAM, &streams, &dynamic(&[1], &[4]), t0, &sink);
        assert_eq!(
            state.step(PROGRAM, &streams, &dynamic(&[1], &[3]), t0 + GAP / 2, &sink),
            StepOutcome::Deferred
        );
        // Back to what is on the wire before the gap passed: nothing left to send.
        assert_eq!(
            state.step(PROGRAM, &streams, &dynamic(&[1], &[4]), t0 + GAP, &sink),
            StepOutcome::Unchanged
        );
        assert_eq!(state.deferred_until(), None);
        assert_eq!(sink.0.borrow().len(), 1);
    }

    /// Programs that never tag: every non-instruction pipeline takes the whole dynamic
    /// subscription, as before; instruction pipelines are never refreshed.
    #[test]
    fn untagged_reaches_every_default_pipeline() {
        let built = DynamicSubscription::untagged(
            Subscription::new()
                .accounts([key(1)])
                .tx_accounts_include([key(2)]),
        );
        let streams = vec![
            stream(
                PipelineKind::Account,
                "accounts",
                Subscription::new().accounts([key(100)]),
                DynamicScope::All,
                &built,
            ),
            stream(
                PipelineKind::Transaction,
                "txs",
                Subscription::new().tx_accounts_include([key(101)]),
                DynamicScope::All,
                &built,
            ),
            stream(
                PipelineKind::Instruction,
                "ixs",
                Subscription::new().tx_accounts_include([key(102)]),
                DynamicScope::All,
                &built,
            ),
        ];
        assert!(streams[0].subscription.accounts.contains(&key(1)));
        assert!(streams[1]
            .subscription
            .tx_accounts_include
            .contains(&key(2)));

        let mut state = RefreshState::new(&streams, GAP);
        let sink = Recorder::default();
        let grown = DynamicSubscription::untagged(
            Subscription::new()
                .accounts([key(1), key(3)])
                .tx_accounts_include([key(2), key(4)]),
        );
        assert_eq!(
            state.step(PROGRAM, &streams, &grown, Instant::now(), &sink),
            StepOutcome::Pushed(vec!["accounts".into(), "txs".into()])
        );
    }

    /// The default `tagged_dynamic_subscription` is the old `dynamic_subscription`, untagged.
    #[test]
    fn old_api_defaults_to_untagged() {
        use crate::{BootstrapCtx, ProgramDeps};
        struct Old;
        #[async_trait::async_trait]
        impl ProgramDeps for Old {
            async fn bootstrap(_: &BootstrapCtx) -> anyhow::Result<Self> {
                Ok(Self)
            }
            fn dynamic_subscription(&self) -> Subscription {
                Subscription::new().accounts([key(9)])
            }
        }
        let tagged = Old.tagged_dynamic_subscription();
        assert_eq!(
            tagged.select(&DynamicScope::All),
            Old.dynamic_subscription()
        );
        assert!(tagged.select(&DynamicScope::only([ORACLES])).is_empty());
        assert_eq!(Old.min_push_interval(), crate::DEFAULT_MIN_PUSH_INTERVAL);
    }

    #[test]
    fn failed_push_is_retried() {
        struct Closed;
        impl FilterSink for Closed {
            fn replace(&self, _: Vec<(String, Prefilter)>) -> Result<(), FilterUpdateError> {
                Err(FilterUpdateError::Closed)
            }
        }
        let built = dynamic(&[1], &[3]);
        let streams = streams(&built);
        let mut state = RefreshState::new(&streams, GAP);
        let t0 = Instant::now();
        let next = dynamic(&[2], &[3]);
        assert_eq!(
            state.step(PROGRAM, &streams, &next, t0, &Closed),
            StepOutcome::NotPushed
        );
        let sink = Recorder::default();
        assert_eq!(
            state.step(PROGRAM, &streams, &next, t0, &sink),
            StepOutcome::Pushed(vec!["oracle_accounts".into()])
        );
    }
}
