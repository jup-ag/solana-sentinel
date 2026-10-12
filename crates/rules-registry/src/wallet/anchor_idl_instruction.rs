//! Instruction rule: one of Anchor's built-in IDL admin instructions hit a
//! watched program. Every Anchor program compiled without `no-idl` accepts
//! them; they share one 8-byte sighash followed by a Borsh enum tag.

use crate::wallet::PROGRAM;
use serde::Deserialize;
use solana_pubkey::Pubkey;
use solana_sentinel::raw::RawInstruction;
use solana_sentinel::utility::pubkey::deserialize_pubkey_vec;
use solana_sentinel::{Alert, Deps, RuleId, Severity, StreamCtx, StreamRule, Subscription};
use std::collections::HashSet;

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
    watched: HashSet<Pubkey>,
}

impl StreamRule for AnchorIdlInstruction {
    const ID: RuleId = RuleId::new(PROGRAM, "anchor_idl_instruction");
    type Config = AnchorIdlInstructionConfig;
    type Event = RawInstruction;

    fn subscription(cfg: &Self::Config) -> Subscription {
        Subscription::new().tx_accounts_include(cfg.programs.iter().copied())
    }

    fn build(cfg: Self::Config, _deps: &Deps) -> anyhow::Result<Self> {
        anyhow::ensure!(!cfg.programs.is_empty(), "programs must not be empty");
        Ok(Self {
            watched: cfg.programs.into_iter().collect(),
        })
    }

    fn evaluate(&self, ix: &RawInstruction, ctx: &StreamCtx) -> Option<Alert> {
        if !self.watched.contains(&ix.program_id) {
            return None;
        }
        if ix.data.len() < 9 || ix.data[..8] != IDL_IX_TAG {
            return None;
        }
        let variant = idl_variant_name(ix.data[8])?;
        Some(
            Alert::for_rule(Self::ID)
                .severity(Severity::Warn)
                .message(format!(
                    "Anchor IDL instruction {variant} on program {}",
                    ix.program_id
                ))
                .meta("program", ix.program_id)
                .meta("instruction", variant)
                .meta("slot", ctx.slot)
                .key([ix.program_id.to_string(), variant.to_string()])
                .scope(ctx.tx_scope())
                .build(),
        )
    }
}

/// Borsh tag of `anchor_lang::idl::IdlInstruction`, in declaration order.
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
