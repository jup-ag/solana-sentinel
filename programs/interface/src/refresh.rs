//! Push a changed dynamic prefilter to the live subscription without a restart.

use crate::parser::{scoped_parser_id, PipelineKind};
use crate::spec::InstalledProgram;
use crate::BootstrapCtx;
use shipstern::RuntimeHandle;
use shipstern_core::{Filters, PrefilterBuilder};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{error, info, warn};

/// How a refreshed filter set reaches the live subscription.
#[derive(Clone)]
pub enum FilterHandle {
    /// Shipstern's runtime handle.
    Shipstern(RuntimeHandle),
    /// The watch channel a [`crate::source::StreamSource`] listens on.
    Watch(watch::Sender<Filters>),
}

impl FilterHandle {
    fn update(&self, edit: impl FnOnce(&mut Filters)) -> anyhow::Result<()> {
        match self {
            Self::Shipstern(h) => h.update_filters(edit).map_err(Into::into),
            Self::Watch(tx) => {
                tx.send_modify(edit);
                Ok(())
            }
        }
    }
}

pub fn spawn_refresh_loops(
    programs: Vec<InstalledProgram>,
    handle: &FilterHandle,
    ctx: &BootstrapCtx,
    cancel_token: &CancellationToken,
    tracker: &TaskTracker,
) {
    for program in programs {
        let Some(interval) = program.refresh_interval() else {
            continue;
        };
        info!(program = %program.program, interval_secs = interval.as_secs(), "Starting prefilter refresh loop");
        let handle = handle.clone();
        let ctx = ctx.clone();
        let cancel_token = cancel_token.clone();
        let mut tick = tokio::time::interval(interval);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        tick.reset(); // the first tick fires immediately otherwise; bootstrap just ran

        tracker.spawn(async move {
            let mut last = program.deps.dynamic_prefilter();
            loop {
                tokio::select! {
                    _ = cancel_token.cancelled() => break,
                    _ = tick.tick() => {
                        if let Err(e) = program.deps.refresh(&ctx).await {
                            error!(program = %program.program, error = %e, "Deps refresh failed");
                            continue;
                        }
                        let dynamic = program.deps.dynamic_prefilter();
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

fn push(program: &InstalledProgram, dynamic: &crate::Prefilter, handle: &FilterHandle) {
    for stream in &program.streams {
        let prefilter = stream.static_prefilter.clone().union(dynamic.clone());
        let subs: Vec<PrefilterBuilder> = match stream.kind {
            PipelineKind::Account => prefilter.account_subscriptions(),
            PipelineKind::Transaction => vec![prefilter.tx_subscription()],
            // Instruction parsers share one bundled filter across programs; not refreshable per program.
            PipelineKind::Instruction | PipelineKind::Poll => continue,
        };
        if subs.len() != stream.subscriptions {
            warn!(
                program = %program.program,
                parser = %stream.base_id,
                "Dynamic prefilter changed the subscription count; restart to apply"
            );
            continue;
        }
        let result = handle.update(|filters| {
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
                info!(program = %program.program, parser = %stream.base_id, "Pushed refreshed prefilter")
            }
            Err(e) => {
                error!(program = %program.program, error = %e, "Failed to push refreshed prefilter")
            }
        }
    }
}
