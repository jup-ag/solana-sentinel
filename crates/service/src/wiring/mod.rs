//! Turns a parsed [`SentinelConfig`](crate::config::SentinelConfig) into the running pieces:
//! alerting (router, cooldown gate) and Riposte (gate, signer, broadcasters).

pub mod alerting;
pub mod riposte;

use ::alerting::sinks::AlertSinkError;
use anyhow::Context;
use domain::programs::Program;
use domain::rules::RuleId;
use domain::sinks::SinkId;
use redis::cluster::ClusterClient;
use redis::cluster_async::ClusterConnection;
use solana_sentinel::{RuleConfigError, ValidatedProgram};
use std::collections::HashMap;
use std::time::Duration;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum WiringError {
    #[error("sink id must not be empty")]
    EmptySinkId,
    #[error("sink id `{0}` is defined more than once")]
    DuplicateSink(SinkId),
    #[error("unknown program `{0}` (no adapter is registered for that name)")]
    UnknownProgram(String),
    #[error("program `{0}` is defined more than once")]
    DuplicateProgram(Program),
    #[error("program `{program}` defines rule `{rule}` more than once")]
    DuplicateRule { program: Program, rule: String },
    #[error(
        "program `{program}` rule `{rule}`: `instance` must be 1 to {} characters",
        crate::validate::MAX_INSTANCE_LEN
    )]
    InvalidInstance { program: Program, rule: String },
    #[error(transparent)]
    Rule(#[from] RuleConfigError),
    #[error("program `{program}` rule `{rule}` was configured but not validated")]
    RuleNotValidated { program: String, rule: String },
    #[error("program `{program}` rule `{rule}` references sink `{sink}` which is not configured")]
    UnconfiguredSink {
        program: Program,
        rule: String,
        sink: SinkId,
    },
    #[error("failed to build sink `{id}`: {source}")]
    SinkBuild {
        id: SinkId,
        #[source]
        source: AlertSinkError,
    },
    #[error("failed to build betterstack sink `{id}`: {source}")]
    BetterstackBuild {
        id: SinkId,
        #[source]
        source: AlertSinkError,
    },
}

pub type Cooldowns = HashMap<RuleId, Duration>;

// Connects to a cluster-mode Redis (ElastiCache CME) with timeouts and retries.
async fn connect_to_redis_cluster(redis_url: &str) -> anyhow::Result<ClusterConnection> {
    let client = ClusterClient::builder(vec![redis_url])
        .connection_timeout(Duration::from_secs(2))
        .response_timeout(Duration::from_secs(1)) // per-attempt
        .retries(1)
        .build()
        .context("Invalid Redis cluster URL")?;
    client
        .get_async_connection()
        .await
        .context("Failed to connect to Redis cluster")
}

type RuleKey = (&'static str, &'static str, Option<&'static str>);

/// Every rule that will run, keyed by `(program, rule name, instance)` as written in config.
fn known_rules(validated: &[ValidatedProgram]) -> HashMap<RuleKey, RuleId> {
    validated
        .iter()
        .flat_map(ValidatedProgram::rule_ids)
        .map(|id| ((id.program.as_str(), id.name, id.instance), id))
        .collect()
}
