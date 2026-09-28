//! The rule framework every program crate builds on.
//!
//! A program exports one [`ProgramSpec`]: a parser per feed plus the rules
//! registered on it. The framework validates typed rule config with no I/O,
//! bootstraps shared [`ProgramDeps`] before any rule, unions prefilters,
//! installs shipstern pipelines and poll loops, and forwards alerts.
//!
//! Adding a rule is one file implementing [`RuleDef`] plus [`GrpcRule`] or
//! [`RpcRule`], and one `.register::<Rule>()` line in the program's `spec()`.

pub mod cache;
pub mod deps;
pub mod handler;
pub mod parser;
pub mod poll;
pub mod prefilter;
pub mod raw;
pub mod refresh;
pub mod rule;
pub mod rule_set;
pub mod source;
pub mod spec;
pub mod sweep;
#[cfg(feature = "testing")]
pub mod testing;

/// Alert, RuleId, Program and the other value types rules build on.
pub use ::domain;
/// Metrics and the slot tracker.
pub use ::observability;
/// Pubkey / RPC / math helpers for rule authors.
pub use ::utility;
pub use cache::{StateCache, UnknownKey};
pub use deps::ProgramDeps;
pub use domain::alert::{Alert, DedupScope, Severity};
pub use domain::programs::Program;
pub use domain::rules::RuleId;
pub use observability::slot_tracker::GrpcSlotReader;
pub use parser::PipelineSink;
pub use poll::PollSource;
pub use prefilter::Prefilter;
pub use refresh::FilterHandle;
pub use rule::{GrpcRule, NoConfig, PollCtx, RpcRule, RuleConfigError, RuleDef, StreamCtx};
pub use rule_set::{PollRules, StreamRules};
pub use shipstern_core::{AccountFilter, MemcmpData};
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
pub use source::{Dispatcher, RawUpdate, SourceUpdate, StreamSource};
pub use spec::{
    BootstrappedProgram, InstallTarget, InstalledProgram, ProgramReport, ProgramSpec,
    ValidatedProgram,
};
use std::collections::HashMap;
use std::sync::Arc;

/// Shared dependencies handed to every program and rule at bootstrap.
///
/// New cross-cutting deps (http clients, caches, …) go here rather than as
/// extra parameters on the rule traits.
#[derive(Clone)]
pub struct BootstrapCtx {
    pub rpc: Arc<RpcClient>,
    /// Last slot seen on the gRPC stream; `None` until the stream is live.
    pub grpc_slot: GrpcSlotReader,
}

/// Raw per-rule config payloads keyed by rule name, straight from `rules.toml`.
pub type RawRuleMap = HashMap<String, config::Value>;
