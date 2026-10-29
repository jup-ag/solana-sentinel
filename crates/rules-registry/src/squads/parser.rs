//! Instruction parser generated from the Squads v4 Anchor IDL (legacy format;
//! shipstern converts it to Codama at compile time).

#![allow(clippy::derive_partial_eq_without_eq)]

use shipstern_proc_macro::include_shipstern_parser;

include_shipstern_parser!("idls/squads_v4/idl.json");

pub type SquadsV4IxParser = squads_multisig_program::InstructionParser;
pub type SquadsV4Ixs = squads_multisig_program::Instructions;
pub type SquadsV4Ix = squads_multisig_program::instruction::Instruction;
pub use squads_multisig_program::config_action;
