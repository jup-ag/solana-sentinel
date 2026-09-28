//! Every rule in the shipped rules files resolves to a registered rule with a
//! config that deserializes. Catches a typo before it reaches a deploy.

use solana_sentinel::ProgramSpec;
use std::path::Path;

// The bin target is not linkable from an integration test; mirror its list.
fn solana_sentinel_cli_programs() -> &'static [fn() -> ProgramSpec] {
    &[
        solana_sentinel_squads_v4::spec,
        solana_sentinel_spl_token::spec,
        solana_sentinel_watchlist::spec,
        solana_sentinel_pyth::spec,
        solana_sentinel_bpf_loader::spec,
        solana_sentinel_system::spec,
    ]
}

fn validate(path: &str) {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap();
    std::env::set_var("RULES_FILE", root.join(path));
    std::env::set_var("CONFIG_FILE", root.join("config.example.toml"));
    let config = solana_sentinel_service::config::SentinelConfig::load().expect("config loads");
    let programs = solana_sentinel_service::validate::validate_programs(
        &config,
        solana_sentinel_cli_programs(),
    )
    .unwrap_or_else(|e| panic!("{path}: {e}"));
    assert!(!programs.is_empty(), "{path}: no programs configured");
    // Routes resolve too: every sink a rule names exists in the example config.
    solana_sentinel_service::wiring::build_alerting_router(&config, &programs)
        .unwrap_or_else(|e| panic!("{path}: {e}"));
}

#[test]
fn shipped_rules_validate() {
    validate("rules.toml");
}
