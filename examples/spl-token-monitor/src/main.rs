//! The deployment binary: name the service, list the programs, hand off to the
//! framework. `spl-token-monitor check` validates config without connecting.

use solana_sentinel::ProgramSpec;
use solana_sentinel_service::{run, RunOptions};

const PROGRAMS: &[fn() -> ProgramSpec] = &[spl_token_monitor_example::spec];

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    run(RunOptions {
        service_name: "spl_token_monitor",
        programs: PROGRAMS,
    })
    .await
}
