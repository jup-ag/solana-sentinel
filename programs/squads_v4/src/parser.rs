#![allow(clippy::derive_partial_eq_without_eq)]

use shipstern_proc_macro::include_shipstern_parser;

include_shipstern_parser!("idls/idl.codama.json");

pub type SquadsV4IxParser = squads_multisig_program::InstructionParser;
pub type SquadsV4Ixs = squads_multisig_program::Instructions;
pub type SquadsV4Ix = squads_multisig_program::instruction::Instruction;
