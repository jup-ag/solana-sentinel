//! Watcher for Anchor's built-in IDL administrative instructions.
//!
//! Every Anchor program (unless compiled with `no-idl`) auto-injects a handful
//! of IDL maintenance instructions. They all share one 8-byte sighash
//! ([`IDL_IX_TAG`], `sha256("anchor:idl")[..8]`) followed by a Borsh enum tag.

use interface::raw::RawInstruction;
use interface::{Alert, GrpcRule, Prefilter, RuleDef, RuleId, Severity, StreamCtx};
use serde::Deserialize;
use solana_pubkey::Pubkey;
use utility::pubkey::deserialize_pubkey_vec;

/// First 8 bytes of `sha256("anchor:idl")`.
pub const IDL_IX_TAG: [u8; 8] = [0x0a, 0x69, 0xe9, 0xa7, 0x78, 0xbc, 0xf4, 0x40];

#[derive(Debug, Deserialize)]
pub struct AnchorIdlInstructionConfig {
    /// Programs whose IDL account must not change.
    #[serde(deserialize_with = "deserialize_pubkey_vec")]
    pub programs: Vec<Pubkey>,
}

#[derive(Debug)]
pub struct AnchorIdlInstruction {
    config: AnchorIdlInstructionConfig,
}

impl RuleDef for AnchorIdlInstruction {
    const ID: RuleId = RuleId::new(crate::PROGRAM, "anchor_idl_instruction");
    type Config = AnchorIdlInstructionConfig;
    type Deps = ();

    fn build(config: Self::Config, _deps: &()) -> anyhow::Result<Self> {
        Ok(Self { config })
    }
}

impl GrpcRule for AnchorIdlInstruction {
    type Event = RawInstruction;

    fn static_prefilter(cfg: &Self::Config) -> Prefilter {
        Prefilter::new().tx_accounts_include(cfg.programs.iter().copied())
    }

    fn evaluate(&self, ix: &RawInstruction, ctx: &StreamCtx) -> Option<Alert> {
        if !self.config.programs.contains(&ix.program_id) {
            return None;
        }
        // 8-byte sighash + 1-byte borsh enum tag
        if ix.data.len() < 9 || ix.data[..8] != IDL_IX_TAG {
            return None;
        }
        let variant = idl_variant_name(ix.data[8])?;

        Some(
            Alert::for_rule(Self::ID)
                .severity(Severity::Warn)
                .message(format!(
                    "Anchor IDL instruction {variant} observed on program {}",
                    ix.program_id
                ))
                .meta("program", ix.program_id)
                .meta("ix", variant)
                .meta("slot", ctx.slot)
                .key([ix.program_id])
                .scope(ctx.tx_scope())
                .build(),
        )
    }
}

/// Map the borsh enum tag of `anchor_lang::idl::IdlInstruction` to its public name.
/// Variant order: `Create, CreateBuffer, Write, SetBuffer, SetAuthority, Close, Resize`.
fn idl_variant_name(tag: u8) -> Option<&'static str> {
    Some(match tag {
        0 => "IdlCreateAccount",
        1 => "IdlCreateBuffer",
        2 => "IdlWrite",
        3 => "IdlSetBuffer",
        4 => "IdlSetAuthority",
        5 => "IdlCloseAccount",
        6 => "IdlResizeAccount",
        _ => return None,
    })
}
