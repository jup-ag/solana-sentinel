//! Ready-made programs and rules. A deployment passes [`PROGRAMS`] to
//! `solana_sentinel_service::run` and activates rules from its rules files;
//! nothing here runs until a `[[rules]]` block names it.
//!
//! | Program | Feed | Rule | Fires when |
//! |---|---|---|---|
//! | `system` | account | `lamports_below` | a watched account's SOL balance is under a floor |
//! | `system` | instruction | `large_sol_transfer` | a SOL transfer out of a watched account is at or above a size |
//! | `system` | instruction | `nonce_activity` | a watched authority initializes, re-authorizes or drains a nonce account |
//! | `spl_token` | account | `supply_increase` | a mint's supply grows by at least N basis points |
//! | `spl_token` | account | `mint_authority_change` | a mint's mint or freeze authority changes |
//! | `bpf_loader` | account | `program_upgrade` | a watched upgradeable program is redeployed |
//! | `bpf_loader` | account | `upgrade_authority_change` | a program's upgrade authority changes or is removed |
//! | `wallet` | transaction | `unapproved_program_invoke` | a watched wallet signs a transaction touching a non-allowlisted program |
//! | `wallet` | instruction | `anchor_idl_instruction` | an Anchor IDL admin instruction hits a watched program |
//! | `health` | poll | `slot_lag` | the stream's slot and the RPC slot drift apart |
//! | `squads_v4` | instruction | `new_proposal` | a proposal is created on a watched multisig |
//! | `squads_v4` | instruction | `new_member_addition` | a member is added to a watched multisig, directly or via a config transaction |
//! | `squads_v4` | instruction | `threshold_change` | a watched multisig's approval threshold changes |
//! | `squads_v4` | instruction | `timelock_change` | a watched multisig's time lock changes |
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
