//! Translates a parsed [`SentinelConfig`] into a fully wired alerting
//! [`Router`] and [`CooldownGate`], validating along the way.

use crate::config::service::{BetterstackSinkConfig, ClickhouseConfig, SinkConfig, SlackConfig};
use crate::config::{ProgramRules, SentinelConfig};
use alerting::cooldown_gate::CooldownGate;
use alerting::fleet_gate::FleetGate;
use alerting::local_gate::LocalGate;
use alerting::router::Router;
use alerting::sinks::betterstack::BetterstackSink;
use alerting::sinks::clickhouse::ClickhouseSink;
use alerting::sinks::slack::SlackSink;
use alerting::sinks::stdout::StdoutSink;
use alerting::sinks::{AlertSink, AlertSinkError};
use domain::programs::Program;
use domain::rules::RuleId;
use domain::sinks::SinkId;
use redis::aio::ConnectionManager;
use redis::cluster::ClusterClient;
use redis::cluster_async::ClusterConnection;
use solana_sentinel::{RuleConfigError, ValidatedProgram};
use std::collections::HashMap;
use std::time::Duration;
use thiserror::Error;
use tracing::info;

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

pub fn build_alerting_router(
    config: &SentinelConfig,
    validated: &[ValidatedProgram],
) -> Result<(Router, Cooldowns), WiringError> {
    let sinks = build_sinks(config.sinks())?;
    let (routes, cooldowns) = build_routes(config.programs_with_rules(), validated, &sinks)?;
    Ok((Router::new(routes, sinks), cooldowns))
}

fn build_sinks(cfgs: &[SinkConfig]) -> Result<HashMap<SinkId, Box<dyn AlertSink>>, WiringError> {
    let mut out: HashMap<SinkId, Box<dyn AlertSink>> = HashMap::with_capacity(cfgs.len());
    for cfg in cfgs {
        let id = cfg.id().clone();
        if id.is_empty() {
            return Err(WiringError::EmptySinkId);
        }
        if out.contains_key(&id) {
            return Err(WiringError::DuplicateSink(id));
        }
        let sink: Box<dyn AlertSink> = match cfg {
            SinkConfig::Clickhouse(c) => Box::new(build_clickhouse(c)),
            SinkConfig::Slack(s) => Box::new(build_slack(s)?),
            SinkConfig::Betterstack(b) => Box::new(build_betterstack(b)?),
            SinkConfig::Stdout(s) => Box::new(StdoutSink::new(s.id.clone())),
        };
        out.insert(id, sink);
    }
    Ok(out)
}

fn build_clickhouse(cfg: &ClickhouseConfig) -> ClickhouseSink {
    ClickhouseSink::build(
        cfg.id.clone(),
        &cfg.url,
        cfg.user.clone(),
        cfg.password.clone(),
        cfg.database.clone(),
    )
}

fn build_slack(cfg: &SlackConfig) -> Result<SlackSink, WiringError> {
    SlackSink::build(
        cfg.id.clone(),
        cfg.bot_auth_token.clone(),
        cfg.channel.clone(),
        cfg.timeout_seconds,
    )
    .map_err(|source| WiringError::SinkBuild {
        id: cfg.id.clone(),
        source,
    })
}

fn build_betterstack(cfg: &BetterstackSinkConfig) -> Result<BetterstackSink, WiringError> {
    BetterstackSink::build(
        cfg.id.clone(),
        &cfg.incident_api_url,
        cfg.incident_auth_token.clone(),
        cfg.escalation_policy_id.clone(),
        cfg.requester_email.clone(),
    )
    .map_err(|source| WiringError::BetterstackBuild {
        id: cfg.id.clone(),
        source,
    })
}

/// Builds the local and fleet cooldown gates from `[alerting.cooldown_gate]`. The local
/// gate uses the sidecar Redis; the fleet gate uses `redis_fleet_url` when set and
/// process memory otherwise.
pub async fn build_cooldown_gate(
    config: &SentinelConfig,
    cooldowns: Cooldowns,
    sidecar: &ConnectionManager,
    pod_id: &str,
    service_name: &str,
) -> anyhow::Result<CooldownGate> {
    let gates = &config.alerting().cooldown_gate;

    let local_gate = gates
        .local_gate
        .then(|| LocalGate::new(sidecar.clone(), cooldowns.clone(), pod_id, service_name));

    let fleet_url = config.redis().redis_fleet_url.as_deref();
    let fleet_gate = match (gates.fleet_gate, fleet_url) {
        (false, _) => None,
        (true, Some(url)) => {
            let fleet = connect_to_redis_cluster(url).await?;
            Some(FleetGate::new(fleet, cooldowns, service_name))
        }
        (true, None) => {
            info!("No redis_fleet_url configured; fleet gate dedups within this process only");
            Some(FleetGate::in_memory(cooldowns))
        }
    }
    .map(|g| g.with_default_ttl(Duration::from_secs(gates.fleet_gate_default_ttl_seconds)));

    Ok(CooldownGate::new(local_gate, fleet_gate))
}

// Connects to a cluster-mode Redis (ElastiCache CME) with timeouts and retries.
async fn connect_to_redis_cluster(redis_url: &str) -> anyhow::Result<ClusterConnection> {
    let client = ClusterClient::builder(vec![redis_url])
        .connection_timeout(Duration::from_secs(2))
        .response_timeout(Duration::from_secs(1)) // per-attempt
        .retries(1)
        .build()?;
    Ok(client.get_async_connection().await?)
}

type Routes = HashMap<RuleId, Vec<SinkId>>;
pub type Cooldowns = HashMap<RuleId, Duration>;

/// Routes and cooldowns for every configured rule. Rule identity comes from the
/// validated programs, so every route here is a rule that will actually run.
fn build_routes(
    programs: &[ProgramRules],
    validated: &[ValidatedProgram],
    sinks: &HashMap<SinkId, Box<dyn AlertSink>>,
) -> Result<(Routes, Cooldowns), WiringError> {
    let known: HashMap<(&'static str, &'static str), RuleId> = validated
        .iter()
        .flat_map(ValidatedProgram::rule_ids)
        .map(|id| ((id.program.as_str(), id.name), id))
        .collect();

    let mut routes: Routes = HashMap::with_capacity(known.len());
    let mut cooldowns: Cooldowns = HashMap::new();

    for p in programs {
        for r in &p.rules {
            let rule_id = known
                .get(&(p.program.as_str(), r.name.as_str()))
                .copied()
                .ok_or_else(|| WiringError::RuleNotValidated {
                    program: p.program.clone(),
                    rule: r.name.clone(),
                })?;

            for sink in &r.sinks {
                if !sinks.contains_key(sink) {
                    return Err(WiringError::UnconfiguredSink {
                        program: rule_id.program,
                        rule: r.name.clone(),
                        sink: sink.clone(),
                    });
                }
            }

            // 0 means disabled, same as leaving cooldown_period_seconds out.
            if let Some(secs) = r.cooldown_period_seconds.filter(|secs| *secs > 0) {
                cooldowns.insert(rule_id, Duration::from_secs(secs));
            }
            routes.insert(rule_id, r.sinks.clone());
        }
    }

    Ok((routes, cooldowns))
}
