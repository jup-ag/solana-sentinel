use crate::alert::{Alert, Severity};
use solana_instruction::Instruction;

/// What a rule's `counteract` returns and Riposte sends: ixs run in order in one tx,
/// with the CU they need. Travels to Riposte next to its alert, not on it.
#[derive(Debug, Clone)]
pub struct CounterMeasure {
    pub ixs: Vec<Instruction>,
    pub compute_units: u32,
}

/// One item on Riposte's queue: the alert, its countermeasure, and the stream event's slot
/// (`None` for sweep alerts), so a replayed or long-queued trigger can be told from a live one.
pub type Trigger = (Alert, CounterMeasure, Option<u64>);

/// A countermeasure's outcome, on the triggering rule's sinks. Without a signature (never sent)
/// it's keyed on the condition, so the cooldown collapses repeats; otherwise unique per trigger.
pub fn outcome_alert(
    trigger: &Alert,
    outcome: &str,
    severity: Severity,
    detail: &str,
    signature: Option<String>,
) -> Alert {
    let id = match &signature {
        None => trigger.local_gate_key().key().to_string(),
        Some(_) => trigger.id.to_string(),
    };
    Alert::for_rule(trigger.rule)
        .severity(severity)
        .message(format!("Riposte countermeasure {outcome}: {detail}"))
        .meta("riposte", outcome)
        .meta("trigger", trigger.id)
        .meta_opt("signature", signature)
        .key(["riposte", outcome, &id])
        .build()
}
