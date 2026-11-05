//! Admitted alerts not yet settled, kept in the pod's sidecar Redis. Replaying the stream can't
//! reliably re-raise a stream alert (rules re-seed their baseline), so a restart re-admits these.
//! Parked alerts wait here rather than in memory, which bounds the resident working set.

use crate::router::Router;
use crate::service::Pending;
use chrono::{DateTime, Utc};
use domain::alert::{Alert, DedupKey, DedupScope, Severity};
use redis::aio::ConnectionManager;
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet};
use tracing::warn;
use uuid::Uuid;

/// Records live in a hash; parked ones (out of memory until due) also in a sorted set by due time.
#[derive(Clone)]
pub struct Outbox {
    conn: ConnectionManager,
    key: String,
    parked_key: String,
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
        let key = format!("{pod_id}:{namespace}:alerts:outbox");
        Self {
            conn,
            parked_key: format!("{key}:parked"),
            key,
        }
    }

    pub(crate) async fn put(&mut self, pending: &Pending) -> anyhow::Result<()> {
        let json = serde_json::to_string(&Record::from(pending))?;
        let id = pending.alert.id.to_string();
        self.conn.hset::<_, _, _, ()>(&self.key, id, json).await?;
        Ok(())
    }

    /// Out of memory until `due_ms` (unix millis); `take_due` brings it back.
    pub(crate) async fn park(&mut self, id: Uuid, due_ms: i64) -> anyhow::Result<()> {
        self.conn
            .zadd::<_, _, _, ()>(&self.parked_key, id.to_string(), due_ms)
            .await?;
        Ok(())
    }

    pub(crate) async fn remove(&mut self, id: Uuid) -> anyhow::Result<()> {
        let id = id.to_string();
        redis::pipe()
            .atomic()
            .hdel(&self.key, &id)
            .zrem(&self.parked_key, &id)
            .query_async::<()>(&mut self.conn)
            .await?;
        Ok(())
    }

    /// Parks everything a previous run left (resident or parked) as due now, skipping this
    /// run's own alerts in `ours`; returns how many.
    pub(crate) async fn adopt_all(&mut self, ours: &HashSet<Uuid>) -> anyhow::Result<usize> {
        let ids: Vec<String> = self.conn.hkeys(&self.key).await?;
        let ids: Vec<String> = ids
            .into_iter()
            .filter(|id| Uuid::parse_str(id).map_or(true, |id| !ours.contains(&id)))
            .collect();
        for chunk in ids.chunks(1000) {
            let members: Vec<(i64, &String)> = chunk.iter().map(|id| (0, id)).collect();
            self.conn
                .zadd_multiple::<_, _, _, ()>(&self.parked_key, &members)
                .await?;
        }
        Ok(ids.len())
    }

    /// Up to `limit` parked alerts due by `now_ms`, unparked. Unreadable records, or ones for a
    /// rule no longer routed, are dropped.
    pub(crate) async fn take_due(
        &mut self,
        router: &Router,
        now_ms: i64,
        limit: usize,
    ) -> anyhow::Result<Vec<Pending>> {
        let ids: Vec<String> = self
            .conn
            .zrangebyscore_limit(&self.parked_key, "-inf", now_ms, 0, limit as isize)
            .await?;
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let records: Vec<Option<String>> = redis::cmd("HMGET")
            .arg(&self.key)
            .arg(&ids)
            .query_async(&mut self.conn)
            .await?;
        let mut out = Vec::with_capacity(ids.len());
        for (id, json) in ids.iter().zip(records) {
            let restored = Uuid::parse_str(id)
                .ok()
                .zip(json.and_then(|j| serde_json::from_str::<Record>(&j).ok()))
                .and_then(|(id, record)| record.restore(id, router));
            match restored {
                Some(pending) => out.push(pending),
                None => {
                    warn!(%id, "Dropping unrestorable outbox record");
                    self.conn.hdel::<_, _, ()>(&self.key, id).await?;
                }
            }
        }
        // Resident from here; a crash before settling leaves the record for `adopt_all`.
        self.conn.zrem::<_, _, ()>(&self.parked_key, &ids).await?;
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
            persisted: true,
        })
    }
}
