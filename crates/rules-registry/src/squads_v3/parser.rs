//! Instruction parser generated from the Squads v3 (squads-mpl) Anchor IDL. The IDL is
//! `lib/idl/squads_mpl.json` from npm `@sqds/sdk@2.0.4` with `metadata.address` added,
//! which shipstern needs for legacy IDLs.

#![allow(clippy::derive_partial_eq_without_eq)]

use shipstern_proc_macro::include_shipstern_parser;

include_shipstern_parser!("idls/squads_v3/idl.json");

pub type SquadsV3IxParser = squads_mpl::InstructionParser;
pub type SquadsV3Ixs = squads_mpl::Instructions;
pub type SquadsV3Ix = squads_mpl::instruction::Instruction;
