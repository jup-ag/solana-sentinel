//! Admitted alerts not yet settled, kept in the pod's sidecar Redis. Replaying the stream can't
//! reliably re-raise a stream alert (rules re-seed their baseline), so a restart re-admits these.

use crate::router::Router;
use crate::service::Pending;
use chrono::{DateTime, Utc};
use domain::alert::{Alert, DedupKey, DedupScope, Severity};
use redis::aio::ConnectionManager;
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use tracing::warn;
use uuid::Uuid;

#[derive(Clone)]
pub struct Outbox {
    conn: ConnectionManager,
    key: String,
}

/// `Alert` holds `&'static` rule names, so it is stored as this and resolved via the router.
#[derive(Serialize, Deserialize)]
struct Record {
    program: String,
    rule: String,
    severity: u8,
    message: String,
    metadata: BTreeMap<String, String>,
    local_gate_key: String,
    fleet_gate_key: String,
    scope: Scope,
    fired_at: DateTime<Utc>,
    sinks: Vec<String>,
    attempts: u32,
    delivered: bool,
}

#[derive(Serialize, Deserialize)]
enum Scope {
    Tx(String),
    Slot(u64),
    Monitor,
}

impl Outbox {
    /// One hash per pod and deployment; fields are alert ids.
    pub fn new(conn: ConnectionManager, pod_id: &str, namespace: &str) -> Self {
        Self {
            conn,
            key: format!("{pod_id}:{namespace}:alerts:outbox"),
        }
    }

    pub(crate) async fn put(&mut self, pending: &Pending) -> anyhow::Result<()> {
        let json = serde_json::to_string(&Record::from(pending))?;
        let id = pending.alert.id.to_string();
        self.conn.hset::<_, _, _, ()>(&self.key, id, json).await?;
        Ok(())
    }

    pub(crate) async fn remove(&mut self, id: Uuid) -> anyhow::Result<()> {
        self.conn
            .hdel::<_, _, ()>(&self.key, id.to_string())
            .await?;
        Ok(())
    }

    /// Everything a previous run left unsettled. Unreadable records, or ones for a rule no
    /// longer routed, are dropped.
    pub(crate) async fn load(&mut self, router: &Router) -> anyhow::Result<Vec<Pending>> {
        let all: HashMap<String, String> = self.conn.hgetall(&self.key).await?;
        let mut out = Vec::with_capacity(all.len());
        for (id, json) in all {
            let restored = Uuid::parse_str(&id)
                .ok()
                .zip(serde_json::from_str::<Record>(&json).ok())
                .and_then(|(id, record)| record.restore(id, router));
            match restored {
                Some(pending) => out.push(pending),
                None => {
                    warn!(%id, "Dropping unrestorable outbox record");
                    self.conn.hdel::<_, _, ()>(&self.key, &id).await?;
                }
            }
        }
        out.sort_by_key(|p| p.alert.fired_at);
        Ok(out)
    }
}

impl From<&Pending> for Record {
    fn from(p: &Pending) -> Self {
        let a = &p.alert;
        Self {
            program: a.rule.program.as_str().to_string(),
            rule: a.rule.name.to_string(),
            severity: match a.severity {
                Severity::Info => 0,
                Severity::Warn => 1,
                Severity::Critical => 2,
            },
            message: a.message.clone(),
            metadata: a.metadata.clone(),
            local_gate_key: a.local_gate_key.key.clone(),
            fleet_gate_key: a.fleet_gate_key.key.clone(),
            scope: match &a.scope {
                DedupScope::Tx(sig) => Scope::Tx(sig.clone()),
                DedupScope::Slot(slot) => Scope::Slot(*slot),
                DedupScope::Monitor => Scope::Monitor,
            },
            fired_at: a.fired_at,
            sinks: p.sinks.iter().map(|s| s.as_str().to_string()).collect(),
            attempts: p.attempts,
            delivered: p.delivered,
        }
    }
}

impl Record {
    fn restore(self, id: Uuid, router: &Router) -> Option<Pending> {
        let rule = router.rule(&self.program, &self.rule)?;
        let alert = Alert {
            id,
            rule,
            severity: match self.severity {
                0 => Severity::Info,
                1 => Severity::Warn,
                _ => Severity::Critical,
            },
            message: self.message,
            metadata: self.metadata,
            local_gate_key: DedupKey::new(rule, self.local_gate_key),
            fleet_gate_key: DedupKey::new(rule, self.fleet_gate_key),
            scope: match self.scope {
                Scope::Tx(sig) => DedupScope::Tx(sig),
                Scope::Slot(slot) => DedupScope::Slot(slot),
                Scope::Monitor => DedupScope::Monitor,
            },
            fired_at: self.fired_at,
            slot_hold: None,
        };
        Some(Pending {
            alert,
            sinks: self.sinks.into_iter().map(Into::into).collect(),
            attempts: self.attempts,
            delivered: self.delivered,
            next_try: None,
        })
    }
}
