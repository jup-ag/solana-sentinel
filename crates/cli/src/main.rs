//! Reference binary: every bundled program, service name `solana_sentinel`.
//! A deployment with its own programs copies this file and lists them.

use solana_sentinel::ProgramSpec;
use solana_sentinel_service::{run, RunOptions};

pub const PROGRAMS: &[fn() -> ProgramSpec] = &[
    solana_sentinel_squads_v4::spec,
    solana_sentinel_spl_token::spec,
    solana_sentinel_watchlist::spec,
    solana_sentinel_pyth::spec,
    solana_sentinel_bpf_loader::spec,
    solana_sentinel_system::spec,
];

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    run(RunOptions {
        service_name: "solana_sentinel",
        programs: PROGRAMS,
    })
    .await
}
