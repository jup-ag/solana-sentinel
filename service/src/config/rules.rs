use anyhow::Context;
use config::{Config, File, FileFormat, Value};
use domain::sinks::SinkId;
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct RuleConfig {
    pub program: String,
    pub name: String,
    pub sinks: Vec<SinkId>,
    pub cooldown_period_seconds: Option<u64>,
    /// The rule's typed config table; may be omitted for rules that take none.
    #[serde(default)]
    pub config: Value,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct RulesLayout {
    #[serde(default)]
    rules: Vec<RuleConfig>,
}

pub struct RulesConfig(pub Vec<RuleConfig>);

impl RulesConfig {
    pub fn load() -> anyhow::Result<Self> {
        let path = std::env::var("RULES_FILE").unwrap_or_else(|_| "./rules.toml".into());
        let cfg = Config::builder()
            .add_source(File::new(&path, FileFormat::Toml).required(false))
            .build()?;
        let parsed = cfg
            .try_deserialize::<RulesLayout>()
            .with_context(|| format!("failed to parse rules file `{path}`"))?;
        Ok(Self(parsed.rules))
    }
}
