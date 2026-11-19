//! Redis-backed dedup gates: claim a key before acting, settle it after. Alerting uses
//! [`cooldown_gate::CooldownGate`] (local + fleet, fail-open); Riposte a strict [`fleet_gate::FleetGate`].

mod claim;
pub mod cooldown_gate;
pub mod fleet_gate;
pub mod local_gate;
