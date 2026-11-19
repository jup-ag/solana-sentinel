//! Builds the alerting [`Router`] and [`CooldownGate`] from sinks and the rules' routes.

use super::{connect_to_redis_cluster, known_rules, Cooldowns, WiringError};
use crate::config::service::{
    BetterstackSinkConfig, ClickhouseConfig, DiscordConfig, SinkConfig, SlackConfig,
};
use crate::config::{ProgramRules, SentinelConfig};
use dedup_gate::cooldown_gate::CooldownGate;
use dedup_gate::fleet_gate::FleetGate;
use dedup_gate::local_gate::LocalGate;
use alerting::router::Router;
use alerting::sinks::betterstack::BetterstackSink;
use alerting::sinks::clickhouse::ClickhouseSink;
use alerting::sinks::discord::DiscordSink;
use alerting::sinks::slack::SlackSink;
use alerting::sinks::stdout::StdoutSink;
use alerting::sinks::AlertSink;
use domain::rules::RuleId;
use domain::sinks::SinkId;
use redis::aio::ConnectionManager;
use solana_sentinel::ValidatedProgram;
use std::collections::HashMap;
use std::time::Duration;
use tracing::info;

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
            SinkConfig::Discord(d) => Box::new(build_discord(d)?),
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

fn build_discord(cfg: &DiscordConfig) -> Result<DiscordSink, WiringError> {
    DiscordSink::build(
        cfg.id.clone(),
        cfg.webhook_url.clone(),
        cfg.bot_token.clone(),
        cfg.channel_id.clone(),
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
            Some(FleetGate::new(fleet, cooldowns, service_name, pod_id))
        }
        (true, None) => {
            info!("No redis_fleet_url configured; fleet gate dedups within this process only");
            Some(FleetGate::in_memory(cooldowns))
        }
    }
    .map(|g| g.with_default_ttl(Duration::from_secs(gates.fleet_gate_default_ttl_seconds)));

    Ok(CooldownGate::new(local_gate, fleet_gate))
}

type Routes = HashMap<RuleId, Vec<SinkId>>;

/// Routes and cooldowns for every configured rule. Rule identity comes from the
/// validated programs, so every route here is a rule that will actually run.
fn build_routes(
    programs: &[ProgramRules],
    validated: &[ValidatedProgram],
    sinks: &HashMap<SinkId, Box<dyn AlertSink>>,
) -> Result<(Routes, Cooldowns), WiringError> {
    let known = known_rules(validated);

    let mut routes: Routes = HashMap::with_capacity(known.len());
    let mut cooldowns: Cooldowns = HashMap::new();

    for p in programs {
        for r in &p.rules {
            let rule_id = known
                .get(&(p.program.as_str(), r.name.as_str(), r.instance.as_deref()))
                .copied()
                .ok_or_else(|| WiringError::RuleNotValidated {
                    program: p.program.clone(),
                    rule: r.label(),
                })?;

            for sink in &r.sinks {
                if !sinks.contains_key(sink) {
                    return Err(WiringError::UnconfiguredSink {
                        program: rule_id.program,
                        rule: r.label(),
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
