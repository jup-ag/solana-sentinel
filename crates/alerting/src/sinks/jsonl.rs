//! Appends alerts to a file as JSON lines, with the sink id and the time the sink got
//! them (`emitted_at`). For drills and local runs that need the alerts afterwards
//! without a database. Several sinks may share one file: each line is one write.
//! Canaries are acknowledged without a line: reaching the sink is the whole probe, and a
//! drill's alert file must hold only what the rules raised.

use crate::sinks::{AlertSink, AlertSinkError};
use async_trait::async_trait;
use chrono::Utc;
use domain::alert::Alert;
use domain::sinks::SinkId;
use parking_lot::Mutex;
use serde_json::json;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

pub struct JsonlSink {
    id: SinkId,
    path: PathBuf,
    file: Mutex<File>,
}

impl JsonlSink {
    pub fn open(id: SinkId, path: &Path) -> std::io::Result<Self> {
        if let Some(dir) = path.parent().filter(|d| !d.as_os_str().is_empty()) {
            std::fs::create_dir_all(dir)?;
        }
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Self {
            id,
            path: path.to_path_buf(),
            file: Mutex::new(file),
        })
    }

    fn line(&self, alert: &Alert) -> String {
        let mut line = json!({
            "sink": self.id.to_string(),
            "id": alert.id,
            "rule": alert.rule.name,
            "program": alert.rule.program.as_str(),
            "severity": alert.severity.to_string(),
            "message": alert.message,
            "metadata": alert.metadata,
            "fired_at": alert.fired_at,
            "emitted_at": Utc::now(),
        })
        .to_string();
        line.push('\n');
        line
    }
}

#[async_trait]
impl AlertSink for JsonlSink {
    fn id(&self) -> &SinkId {
        &self.id
    }

    async fn send(&self, alert: &Alert) -> Result<(), AlertSinkError> {
        self.send_batch(std::slice::from_ref(alert)).await
    }

    async fn send_batch(&self, alerts: &[Alert]) -> Result<(), AlertSinkError> {
        let mut file = self.file.lock();
        for alert in alerts.iter().filter(|a| !a.is_canary()) {
            file.write_all(self.line(alert).as_bytes())
                .map_err(|e| AlertSinkError::Jsonl(format!("{}: {e}", self.path.display())))?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use domain::programs::Program;
    use domain::rules::RuleId;

    #[tokio::test]
    async fn appends_one_line_per_alert_with_emitted_at() {
        let dir = std::env::temp_dir().join(format!("jsonl-sink-{}", std::process::id()));
        let path = dir.join("alerts.jsonl");
        let _ = std::fs::remove_file(&path);
        let sink = JsonlSink::open(SinkId::new("drill"), &path).unwrap();
        let alert = Alert::for_rule(RuleId::new(Program::new("p"), "r"))
            .message("hello")
            .build();
        sink.send(&alert).await.unwrap();
        sink.send_batch(&[alert.clone(), alert]).await.unwrap();
        let text = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<serde_json::Value> = text
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        assert_eq!(lines.len(), 3);
        assert_eq!(lines[0]["sink"], "drill");
        assert_eq!(lines[0]["message"], "hello");
        assert!(lines[0]["emitted_at"].is_string());
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A canary is accepted (the probe arrived) but leaves no line in the drill's file.
    #[tokio::test]
    async fn canary_is_acknowledged_without_a_line() {
        let dir = std::env::temp_dir().join(format!("jsonl-sink-canary-{}", std::process::id()));
        let path = dir.join("alerts.jsonl");
        let _ = std::fs::remove_file(&path);
        let sink = JsonlSink::open(SinkId::new("drill"), &path).unwrap();
        let canary = Alert::for_rule(RuleId::new(Program::new("health"), "canary"))
            .meta(domain::canary::CANARY, "true")
            .build();
        let real = Alert::for_rule(RuleId::new(Program::new("p"), "r")).build();
        sink.send(&canary).await.unwrap();
        sink.send_batch(&[canary, real]).await.unwrap();
        let text = std::fs::read_to_string(&path).unwrap();
        assert_eq!(text.lines().count(), 1);
        assert!(text.contains("\"rule\":\"r\""));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
