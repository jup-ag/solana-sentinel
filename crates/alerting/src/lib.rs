//! Alert sinks, routing, and the dispatching service.
//!
//! This crate is a library: it knows how to send [`Alert`]s to sinks
//! and how to fan them out per [`RuleId`], but it does not know how
//! they are configured. Callers (typically the binary) are responsible
//! for building the [`router::Router`] from whichever source they
//! prefer (TOML, env, gRPC, ...) and handing it to
//! [`service::AlertingService`].
//!
//! [`Alert`]: domain::alert::Alert
//! [`RuleId`]: domain::rules::RuleId

pub mod cooldown_gate;
pub mod fleet_gate;
pub mod router;
pub mod service;
pub mod sinks;
