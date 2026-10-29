//! Ready-made programs and rules. A deployment passes [`PROGRAMS`] to
//! `solana_sentinel_service::run` and activates rules from its rules files;
//! nothing here runs until a `[[rules]]` block names it.
//!
//! | Program | Rules |
//! |---|---|
//! | `system` | `lamports_below`, `large_sol_transfer`, `nonce_activity` |
//! | `spl_token` | `supply_increase`, `mint_authority_change` |
//! | `bpf_loader` | `program_upgrade`, `upgrade_authority_change` |
//! | `wallet` | `unapproved_program_invoke`, `anchor_idl_instruction` |
//! | `health` | `slot_lag` |
//! | `squads_v4` | `new_proposal`, `new_member_addition`, `threshold_change`, `timelock_change` |
//!
//! Each program module is also a template: copy it into your own crate,
//! rename `PROGRAM`, and edit the rules.

pub mod bpf_loader;
pub mod health;
pub mod spl_token;
pub mod squads;
pub mod system;
pub mod wallet;

pub use solana_sentinel::ProgramSpec;

/// Every program in the registry, for `RunOptions { programs: PROGRAMS, .. }`.
pub const PROGRAMS: &[fn() -> ProgramSpec] = &[
    bpf_loader::spec,
    health::spec,
    spl_token::spec,
    squads::spec,
    system::spec,
    wallet::spec,
];
