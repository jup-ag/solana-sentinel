//! The cooldown gate: the dedup step every alert passes before it is routed.
//! Two layers run in order, the local gate (this node) then the fleet gate (any node).

use crate::fleet_gate::FleetGate;
use crate::local_gate::LocalGate;
use domain::alert::Alert;
use observability::prometheus::metrics::record_alert_suppressed;
use tracing::{debug, error};

/// Owns both dedup layers. A `None` layer is disabled and always passes.
#[derive(Default)]
pub struct CooldownGate {
    local_gate: Option<LocalGate>,
    fleet_gate: Option<FleetGate>,
}

impl CooldownGate {
    pub fn new(local_gate: Option<LocalGate>, fleet_gate: Option<FleetGate>) -> Self {
        Self {
            local_gate,
            fleet_gate,
        }
    }

    pub fn local_gate_enabled(&self) -> bool {
        self.local_gate.is_some()
    }

    pub fn fleet_gate_enabled(&self) -> bool {
        self.fleet_gate.is_some()
    }

    /// Local gate first (repeat of the same alert within its window), then the fleet
    /// gate (already claimed by another node). Fail-open: a Redis error must not lose alerts.
    pub async fn should_send(&mut self, alert: &Alert) -> bool {
        if let Some(gate) = &mut self.local_gate {
            match gate.check(alert).await {
                Ok(true) => {}
                Ok(false) => {
                    debug!(rule = alert.rule.name, "Alert suppressed by local gate");
                    record_alert_suppressed(alert.rule.name);
                    return false;
                }
                Err(e) => error!("Failed to check local gate: {e}"),
            }
        }

        if let Some(gate) = &mut self.fleet_gate {
            match gate.check(alert).await {
                Ok(true) => {}
                Ok(false) => {
                    debug!(rule = alert.rule.name, "Alert suppressed by fleet gate");
                    record_alert_suppressed(alert.rule.name);
                    return false;
                }
                Err(e) => error!("Failed to check fleet gate: {e}"),
            }
        }

        true
    }
}
