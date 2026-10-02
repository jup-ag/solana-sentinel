use crate::programs::Program;
use crate::rules::RuleId;
use chrono::{DateTime, Utc};
use sha2::Digest;
use std::collections::BTreeMap;
use std::fmt::{self, Display};
use uuid::Uuid;

// A HashKey used for deduplication of alerts
pub struct HashKey(Vec<String>);

impl HashKey {
    pub fn new(parts: &[impl ToString]) -> Self {
        Self(parts.iter().map(std::string::ToString::to_string).collect())
    }

    pub fn parts(&self) -> &[String] {
        &self.0
    }
}

// A hex-encoded SHA-256 digest used to identify an alert for deduplication.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DedupKey {
    pub rule: RuleId,
    pub key: String,
}

impl Display for DedupKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.rule.to_string())?;
        f.write_str(&self.key)
    }
}

impl DedupKey {
    pub fn new(rule: RuleId, key: String) -> Self {
        Self { rule, key }
    }

    pub fn rule(&self) -> RuleId {
        self.rule
    }

    pub fn key(&self) -> &str {
        &self.key
    }
}

// A scope for deduplicating alerts.
// Alerts with the same scope and the same underlying condition (rule, message, metadata) will collapse to a single alert.
#[derive(Debug, Clone)]
pub enum DedupScope {
    Tx(String),
    Slot(u64),
    // A monitor-level alert is one that is not tied to a specific transaction or slot
    // but rather to a broader condition which involves monitoring the account state over time.
    Monitor,
}

impl DedupScope {
    fn token(&self) -> String {
        match self {
            Self::Tx(sig) => sig.clone(),
            Self::Slot(slot) => slot.to_string(),
            Self::Monitor => "monitor".to_string(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Severity {
    Info,
    Warn,
    Critical,
}

impl From<Severity> for String {
    fn from(val: Severity) -> Self {
        match val {
            Severity::Info => "info".to_string(),
            Severity::Warn => "warn".to_string(),
            Severity::Critical => "critical".to_string(),
        }
    }
}

impl Display for Severity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Self::Info => "Info",
            Self::Warn => "Warn",
            Self::Critical => "Critical",
        };
        f.write_str(s)
    }
}

impl Severity {
    pub fn is_critical(&self) -> bool {
        matches!(self, Self::Critical)
    }
}

#[derive(Debug, Clone)]
pub struct Alert {
    pub id: Uuid,
    pub rule: RuleId,
    pub severity: Severity,
    pub message: String,
    pub metadata: BTreeMap<String, String>,
    pub local_gate_key: DedupKey,
    pub fleet_gate_key: DedupKey,
    pub scope: DedupScope,
    pub fired_at: DateTime<Utc>,
}

impl Alert {
    pub fn new(
        rule: RuleId,
        severity: Severity,
        message: impl Into<String> + Clone,
        metadata: BTreeMap<impl Into<String>, String>,
        hash_key: &HashKey,
        scope: DedupScope,
    ) -> Self {
        let metadata = metadata
            .into_iter()
            .map(|(k, v)| (k.into(), v))
            .collect::<BTreeMap<String, String>>();

        let message = message.into();

        let local_gate_key = derive_local_gate_dedup_key(rule, hash_key);

        let fleet_gate_key = derive_fleet_gate_dedup_key(rule, hash_key, &scope);

        Self {
            id: Uuid::new_v4(),
            rule,
            severity,
            message,
            metadata,
            local_gate_key,
            fleet_gate_key,
            scope,
            fired_at: Utc::now(),
        }
    }

    // Program is implied by the rule.
    pub const fn program(&self) -> Program {
        self.rule.program
    }

    pub fn local_gate_key(&self) -> &DedupKey {
        &self.local_gate_key
    }

    pub fn fleet_gate_key(&self) -> &DedupKey {
        &self.fleet_gate_key
    }
}

// Layer 1 — intra-node dedup. Identifies a single alert occurrence as seen
// by one node and is used to gate repeated alerts from the same node within a cooldown period.
pub fn derive_local_gate_dedup_key(rule: RuleId, hash_key: &HashKey) -> DedupKey {
    let mut hasher = sha2::Sha256::new();
    hash_condition(&mut hasher, rule, hash_key);
    let key = hex::encode(hasher.finalize());
    DedupKey::new(rule, key)
}

// Layer 2 — inter-node dedup. Identifies the underlying rule, independent
// of the observing node and per-occurrence scope,
// it uses the scope to distinguish between different occurrences of the same underlying rules.
pub fn derive_fleet_gate_dedup_key(
    rule: RuleId,
    hash_key: &HashKey,
    scope: &DedupScope,
) -> DedupKey {
    let mut hasher = sha2::Sha256::new();
    hash_condition(&mut hasher, rule, hash_key);
    hasher.update(scope.token().as_bytes());
    let key = hex::encode(hasher.finalize());
    DedupKey::new(rule, key)
}

fn hash_condition(hasher: &mut sha2::Sha256, rule: RuleId, hash_key: &HashKey) {
    hasher.update(rule.to_string().as_bytes());
    for part in hash_key.parts() {
        // NUL separator so part boundaries are unambiguous, e.g. ["ab","c"] != ["a","bc"].
        hasher.update([0u8]);
        hasher.update(part.as_bytes());
    }
}

/// Builder for [`Alert`]. Severity defaults to `Warn`, scope to `Monitor`;
/// an empty key collapses every alert of the rule onto one dedup key.
pub struct AlertBuilder {
    rule: RuleId,
    severity: Severity,
    message: String,
    metadata: BTreeMap<String, String>,
    key: Vec<String>,
    scope: DedupScope,
}

impl Alert {
    pub fn for_rule(rule: RuleId) -> AlertBuilder {
        AlertBuilder {
            rule,
            severity: Severity::Warn,
            message: String::new(),
            metadata: BTreeMap::new(),
            key: Vec::new(),
            scope: DedupScope::Monitor,
        }
    }
}

impl AlertBuilder {
    #[must_use]
    pub fn severity(mut self, severity: Severity) -> Self {
        self.severity = severity;
        self
    }

    #[must_use]
    pub fn message(mut self, message: impl Into<String>) -> Self {
        self.message = message.into();
        self
    }

    #[must_use]
    #[allow(clippy::needless_pass_by_value)]
    pub fn meta(mut self, key: impl Into<String>, value: impl ToString) -> Self {
        self.metadata.insert(key.into(), value.to_string());
        self
    }

    /// Insert `key` only when `value` is `Some`.
    #[must_use]
    pub fn meta_opt(self, key: impl Into<String>, value: Option<impl ToString>) -> Self {
        match value {
            Some(v) => self.meta(key, v),
            None => self,
        }
    }

    #[must_use]
    pub fn metas<K: Into<String>, V: ToString>(
        mut self,
        entries: impl IntoIterator<Item = (K, V)>,
    ) -> Self {
        for (k, v) in entries {
            self.metadata.insert(k.into(), v.to_string());
        }
        self
    }

    /// Dedup key parts; alerts with the same rule and parts collapse in both gates.
    #[must_use]
    pub fn key<I: IntoIterator>(mut self, parts: I) -> Self
    where
        I::Item: ToString,
    {
        self.key = parts.into_iter().map(|p| p.to_string()).collect();
        self
    }

    #[must_use]
    pub fn scope(mut self, scope: DedupScope) -> Self {
        self.scope = scope;
        self
    }

    pub fn build(self) -> Alert {
        Alert::new(
            self.rule,
            self.severity,
            self.message,
            self.metadata,
            &HashKey(self.key),
            self.scope,
        )
    }
}
