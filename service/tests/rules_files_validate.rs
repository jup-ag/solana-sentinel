//! Every rule in the shipped rules files resolves to a registered rule with a
//! config that deserializes. Catches a typo before it reaches a deploy.

use std::path::Path;

fn validate(path: &str) {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
    std::env::set_var("RULES_FILE", root.join(path));
    std::env::set_var("CONFIG_FILE", root.join("config.example.toml"));
    let config = service::config::SentinelConfig::load().expect("config loads");
    let programs =
        service::validate::validate_programs(&config).unwrap_or_else(|e| panic!("{path}: {e}"));
    assert!(!programs.is_empty(), "{path}: no programs configured");
    // Routes resolve too: every sink a rule names exists in the example config.
    service::wiring::build_alerting_router(&config, &programs)
        .unwrap_or_else(|e| panic!("{path}: {e}"));
}

#[test]
fn shipped_rules_validate() {
    validate("rules.toml");
}
