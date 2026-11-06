//! The rule framework every program crate builds on.
//!
//! A program exports one [`ProgramSpec`]: a parser per feed plus the rules
//! registered on it. The framework validates typed rule config with no I/O,
//! bootstraps shared [`ProgramDeps`] before any rule, unions subscriptions,
//! builds shipstern pipelines and poll loops, and forwards alerts.
//!
//! Adding a rule is one file implementing [`StreamRule`] or [`PollRule`], and
//! one `.register::<Rule>()` line in the program's `spec()`. A [`ReactorRule`] is fed
//! by other rules' alerts instead, registered with `.reactor::<Rule>()`.

pub mod cache;
pub mod deps;
pub mod dynamic;
pub mod handler;
pub mod parser;
pub mod poll;
pub mod raw;
pub mod reactor;
pub mod refresh;
pub mod rule;
pub mod rule_set;
pub mod spec;
pub mod subscription;
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
pub use deps::{Deps, ProgramDeps, DEFAULT_MIN_PUSH_INTERVAL};
pub use domain::alert::{Alert, DedupScope, Severity};
pub use domain::programs::Program;
pub use domain::rules::RuleId;
pub use dynamic::{DynamicScope, DynamicSubscription, DynamicTag};
pub use observability::liveness::{FeedHandle, FeedKind, Liveness};
pub use observability::slot_tracker::GrpcSlotReader;
pub use observability::source_health::SourceHealth;
pub use parser::StreamPipeline;
pub use poll::{PollSource, Tick, TickSource};
pub use reactor::{BuiltReactor, ReactCtx, ReactorRule};
pub use rule::{NoConfig, PollCtx, PollRule, RuleConfigError, StreamCtx, StreamRule};
pub use rule_set::{PollRules, StreamRules};
pub use shipstern_core::{AccountFilter, MemcmpData};
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
pub use spec::{ProgramReport, ProgramSpec, StartCtx, StartedProgram, ValidatedProgram};
use std::collections::HashMap;
use std::sync::Arc;
pub use subscription::Subscription;

/// Shared dependencies handed to every program and rule at bootstrap.
///
/// New cross-cutting deps (http clients, caches, …) go here rather than as
/// extra parameters on the rule traits.
#[derive(Clone)]
pub struct BootstrapCtx {
    pub rpc: Arc<RpcClient>,
    /// Last slot seen on the gRPC stream; `None` until the stream is live.
    pub grpc_slot: GrpcSlotReader,
    /// This node's identity (the pod hostname), for rules whose condition is per-node.
    pub pod_id: String,
    /// Per-feed liveness and canary state, shared by every pipeline and the health rules.
    pub liveness: Liveness,
    /// Per-child health of a `multi` data source; empty with a single source.
    pub source_health: SourceHealth,
}

/// Raw per-rule config payloads keyed by rule name, straight from `rules.toml`.
pub type RawRuleMap = HashMap<String, config::Value>;
