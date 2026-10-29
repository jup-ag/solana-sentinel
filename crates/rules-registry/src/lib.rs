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
//! | `pyth` | `oracle_staleness`, `reference_price_drift` |
//! | `stable_coin` | `depeg_monitor` |
//!
//! Each program module is also a template: copy it into your own crate,
//! rename `PROGRAM`, and edit the rules. The generated IDL types (accounts,
//! instructions, defined types) are re-exported under [`idl`].

pub mod bpf_loader;
pub mod health;
pub mod pyth;
pub mod spl_token;
pub mod squads;
pub mod stable_coin;
pub mod system;
pub mod wallet;

pub use solana_sentinel::ProgramSpec;

/// Types generated from the bundled IDLs: account structs and their `AccountParser`,
/// instruction enums and their `InstructionParser`, defined types and `PROGRAM_ID`.
pub mod idl {
    pub use crate::squads::parser::squads_multisig_program as squads_v4;
    pub use crate::stable_coin::parser::{
        amm_v3 as raydium_clmm, meteora_dlmm, whirlpool as orca_whirlpool,
    };
}

/// Every program in the registry, for `RunOptions { programs: PROGRAMS, .. }`.
pub const PROGRAMS: &[fn() -> ProgramSpec] = &[
    bpf_loader::spec,
    health::spec,
    pyth::spec,
    spl_token::spec,
    squads::spec,
    stable_coin::spec,
    system::spec,
    wallet::spec,
];
