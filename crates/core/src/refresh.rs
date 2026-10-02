//! Push a changed dynamic subscription to the live stream without a restart.

use crate::parser::{scoped_parser_id, PipelineKind};
use crate::spec::StartedProgram;
use crate::BootstrapCtx;
use shipstern::RuntimeHandle;
use shipstern_core::PrefilterBuilder;
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
        info!(program = %program.program, interval_secs = interval.as_secs(), "Starting subscription refresh loop");
        let handle = handle.clone();
        let ctx = ctx.clone();
        let cancel_token = cancel_token.clone();
        let mut tick = tokio::time::interval(interval);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        tick.reset(); // the first tick fires immediately otherwise; bootstrap just ran

        tracker.spawn(async move {
            let mut last = program.deps.dynamic_subscription();
            loop {
                tokio::select! {
                    _ = cancel_token.cancelled() => break,
                    _ = tick.tick() => {
                        if let Err(e) = program.deps.refresh(&ctx).await {
                            error!(program = %program.program, error = %e, "Deps refresh failed");
                            continue;
                        }
                        let dynamic = program.deps.dynamic_subscription();
                        if dynamic == last {
                            continue;
                        }
                        push(&program, &dynamic, &handle);
                        last = dynamic;
                    }
                }
            }
        });
    }
}

fn push(program: &StartedProgram, dynamic: &crate::Subscription, handle: &RuntimeHandle) {
    for stream in &program.streams {
        // Instruction parsers share one bundled filter across programs; not refreshable per program.
        if stream.kind == PipelineKind::Instruction {
            continue;
        }
        let subscription = stream.static_subscription.clone().union(dynamic.clone());
        let subs: Vec<PrefilterBuilder> = stream.kind.prefilters(&subscription);
        if subs.len() != stream.subscriptions {
            warn!(
                program = %program.program,
                parser = %stream.base_id,
                "Dynamic subscription changed the prefilter count; restart to apply"
            );
            continue;
        }
        let result = handle.update_filters(|filters| {
            for (i, sub) in subs.into_iter().enumerate() {
                match sub.build() {
                    Ok(extra) => {
                        let mut merged = stream.parser_prefilter.clone();
                        merged.merge(extra);
                        filters.insert(
                            scoped_parser_id(program.program, &stream.base_id, i),
                            merged,
                        );
                    }
                    Err(e) => warn!(error = %e, "Invalid refreshed prefilter; skipping"),
                }
            }
        });
        match result {
            Ok(()) => {
                info!(program = %program.program, parser = %stream.base_id, "Pushed refreshed subscription")
            }
            Err(e) => {
                error!(program = %program.program, error = %e, "Failed to push refreshed subscription")
            }
        }
    }
}
