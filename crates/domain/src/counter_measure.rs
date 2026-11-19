use crate::alert::Alert;
use solana_instruction::Instruction;

/// What a rule's `counteract` returns and Riposte sends: ixs run in order in one tx,
/// with the CU they need. Travels to Riposte next to its alert, not on it.
#[derive(Debug, Clone)]
pub struct CounterMeasure {
    pub ixs: Vec<Instruction>,
    pub compute_units: u32,
}

/// One item on Riposte's queue: the alert, its countermeasure, and the stream event's slot
/// (`None` for poll and sweep alerts), so a replayed trigger can be told from a live one.
pub type Trigger = (Alert, CounterMeasure, Option<u64>);
