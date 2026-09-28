//! Fixture helpers for rule tests. Enable with the `testing` feature.

use crate::rule::{PollCtx, RuleDef, StreamCtx};
use config::{Config, File, FileFormat};
use shipstern_core::instruction::{InstructionShared, InstructionUpdate, Path};
use shipstern_core::{AccountUpdate, AccountUpdateInfo, KeyBytes, Parser};
use solana_pubkey::Pubkey;
use std::sync::Arc;
use std::time::Instant;

/// Deserialize a `[rules.config]` body from TOML and build the rule with no deps and no bootstrap.
pub fn rule_from_toml<R: RuleDef<Deps = ()>>(toml: &str) -> R {
    rule_with_deps::<R>(toml, &())
}

pub fn rule_with_deps<R: RuleDef>(toml: &str, deps: &R::Deps) -> R {
    let cfg: R::Config = Config::builder()
        .add_source(File::from_str(toml, FileFormat::Toml))
        .build()
        .expect("valid toml")
        .try_deserialize()
        .expect("config deserializes");
    R::build(cfg, deps).expect("rule builds")
}

pub fn stream_ctx(slot: u64, tx_sig: Option<&str>) -> StreamCtx {
    StreamCtx {
        slot,
        tx_sig: tx_sig.map(str::to_string),
        account: None,
    }
}

pub fn poll_ctx(slot: u64) -> PollCtx {
    PollCtx {
        slot,
        fetched_at: Instant::now(),
    }
}

pub fn account_update(pubkey: Pubkey, owner: Pubkey, data: Vec<u8>) -> AccountUpdate {
    AccountUpdate {
        account: Some(AccountUpdateInfo {
            pubkey: pubkey.to_bytes().to_vec(),
            owner: owner.to_bytes().to_vec(),
            data,
            txn_signature: Some(vec![7; 64]),
            ..Default::default()
        }),
        slot: 1,
        is_startup: false,
    }
}

pub fn ix_update(program: Pubkey, accounts: &[Pubkey], data: Vec<u8>) -> InstructionUpdate {
    InstructionUpdate {
        program: KeyBytes(program.to_bytes()),
        accounts: accounts.iter().map(|a| KeyBytes(a.to_bytes())).collect(),
        data,
        shared: Arc::new(InstructionShared::default()),
        inner: Vec::new(),
        path: Path::new_single(0),
        log_range: 0..0,
    }
}

/// Run a parser on a fixture and unwrap; the event a `GrpcRule` evaluates.
pub async fn parse<P: Parser>(parser: &P, input: &P::Input) -> P::Output {
    parser.parse(input).await.expect("parser accepts fixture")
}

/// Evaluate and unwrap the alert, panicking with the rule name when it stayed silent.
#[macro_export]
macro_rules! assert_fires {
    ($rule:expr, $ev:expr, $ctx:expr) => {
        $crate::GrpcRule::evaluate(&$rule, &$ev, &$ctx).expect("rule did not fire")
    };
}
