//! Kill switch: `SET {service}:riposte:halt 1` in the fleet Redis stops every countermeasure
//! within a second, fleet-wide; `DEL` resumes. Read into a cached flag, never per countermeasure.

use redis::cluster_async::ClusterConnection;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{error, warn};

const REFRESH: Duration = Duration::from_secs(1);

/// `Default` never halts: for dry run without a fleet Redis, and tests.
#[derive(Clone, Default)]
pub struct HaltSwitch(Arc<AtomicBool>);

impl HaltSwitch {
    /// Watches `key`. Starts halted until the first successful read, so a restart while
    /// halted can't act before it has seen the key; a later read error keeps the last value.
    pub fn spawn(
        mut conn: ClusterConnection,
        key: String,
        cancel: CancellationToken,
        tracker: &TaskTracker,
    ) -> Self {
        let switch = Self(Arc::new(AtomicBool::new(true)));
        let flag = Arc::clone(&switch.0);
        tracker.spawn(async move {
            let mut tick = tokio::time::interval(REFRESH);
            loop {
                tokio::select! {
                    () = cancel.cancelled() => return,
                    _ = tick.tick() => {}
                }
                match redis::cmd("EXISTS").arg(&key).query_async::<bool>(&mut conn).await {
                    Ok(halted) => {
                        if flag.swap(halted, Ordering::Relaxed) != halted {
                            error!(halted, key, "riposte kill switch changed");
                        }
                    }
                    Err(e) => warn!(error = %e, key, "riposte kill switch read failed"),
                }
            }
        });
        switch
    }

    pub fn is_halted(&self) -> bool {
        self.0.load(Ordering::Relaxed)
    }
}
