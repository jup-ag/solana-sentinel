use crate::config::rules::{RuleConfig, RulesConfig};
use crate::config::service::{
    AlertingConfig, DataSourceConfig, ObservabilityConfig, RedisConfig, RpcConfig, ServiceConfig,
    SinkConfig,
};

pub mod rules;
pub mod service;

pub struct SentinelConfig {
    service: ServiceConfig,
    programs_with_rules: Vec<ProgramRules>, // keeps the rules grouped by program for easier wiring later
}

#[derive(Debug, Clone)]
pub struct ProgramRules {
    pub program: String,
    pub rules: Vec<RuleConfig>,
}

impl SentinelConfig {
    pub fn load() -> anyhow::Result<Self> {
        let service = ServiceConfig::load()?;
        let rules = RulesConfig::load()?;
        let programs_with_rules = map_programs_with_rules(rules.0);

        Ok(Self {
            service,
            programs_with_rules,
        })
    }

    pub fn rpc(&self) -> &RpcConfig {
        &self.service.rpc
    }

    pub fn sinks(&self) -> &[SinkConfig] {
        &self.service.sinks
    }

    pub fn data_source(&self) -> &DataSourceConfig {
        &self.service.data_source
    }

    pub fn observability(&self) -> &ObservabilityConfig {
        &self.service.observability
    }

    pub fn programs_with_rules(&self) -> &[ProgramRules] {
        &self.programs_with_rules
    }

    /// `None` means no Redis: in-memory gates and no slot checkpoint (local runs).
    pub fn redis(&self) -> Option<&RedisConfig> {
        self.service.redis.as_ref()
    }

    pub fn alerting(&self) -> &AlertingConfig {
        &self.service.alerting
    }
}

/// Group rules by program, keeping the order programs first appear in `rules.toml`.
fn map_programs_with_rules(rules: Vec<RuleConfig>) -> Vec<ProgramRules> {
    let mut out: Vec<ProgramRules> = Vec::new();
    for r in rules {
        match out.iter_mut().find(|p| p.program == r.program) {
            Some(p) => p.rules.push(r),
            None => out.push(ProgramRules {
                program: r.program.clone(),
                rules: vec![r],
            }),
        }
    }
    out
}
