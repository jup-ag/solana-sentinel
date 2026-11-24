use solana_hash::Hash;
use solana_instruction::Instruction;
use solana_message::v1::{self, TransactionConfig};
use solana_message::VersionedMessage;
use solana_pubkey::Pubkey;
use std::collections::HashSet;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ComposeError {
    #[error("instruction invokes program {0}, which is not in allowed_programs")]
    ProgramNotAllowed(Pubkey),
    #[error("message needs {0} signers; only the payer signs")]
    ExtraSigners(u8),
    #[error("failed to compile message: {0}")]
    Compile(#[from] solana_message::CompileError),
    #[error("invalid v1 message: {0}")]
    Invalid(v1::MessageError),
    #[error("needs {0} CU; the runtime caps a tx at {MAX_COMPUTE_UNIT_LIMIT}")]
    ComputeUnits(u32),
}

/// Runtime default; v1 treats an unset limit as 0, which would fail every account load.
const MAX_LOADED_ACCOUNTS_DATA_SIZE: u32 = 64 * 1024 * 1024;
/// Runtime CU cap; over it is an error, not a clamp, so summed bundles never run short.
const MAX_COMPUTE_UNIT_LIMIT: u32 = 1_400_000;

/// A v1 message with the payer as sole signer; compute budget lives in the message config.
pub fn compose(
    ixs: &[Instruction],
    compute_units: u32,
    priority_fee_micro_lamports: u64,
    payer: &Pubkey,
    blockhash: Hash,
    allowed_programs: &HashSet<Pubkey>,
) -> Result<VersionedMessage, ComposeError> {
    if let Some(ix) = ixs
        .iter()
        .find(|ix| !allowed_programs.contains(&ix.program_id))
    {
        return Err(ComposeError::ProgramNotAllowed(ix.program_id));
    }
    if compute_units > MAX_COMPUTE_UNIT_LIMIT {
        return Err(ComposeError::ComputeUnits(compute_units));
    }
    // v1 takes the total fee in lamports, not a per-CU price; round up so the fee never drops.
    let fee =
        (u128::from(priority_fee_micro_lamports) * u128::from(compute_units)).div_ceil(1_000_000);
    let fee = u64::try_from(fee).unwrap_or(u64::MAX);
    let mut config = TransactionConfig::empty()
        .with_compute_unit_limit(compute_units)
        .with_loaded_accounts_data_size_limit(MAX_LOADED_ACCOUNTS_DATA_SIZE);
    if fee > 0 {
        config = config.with_priority_fee(fee);
    }

    let msg = v1::Message::try_compile_with_config(payer, ixs, blockhash, config)?;
    // Compile doesn't check v1 limits, and the encoder truncates counts past u8/u16.
    msg.validate().map_err(ComposeError::Invalid)?;
    let signers = msg.header.num_required_signatures;
    if signers != 1 {
        return Err(ComposeError::ExtraSigners(signers));
    }
    // validate() skips the wire size: version byte + message + one signature.
    if 1 + msg.size() + v1::SIGNATURE_SIZE > v1::MAX_TRANSACTION_SIZE {
        return Err(ComposeError::Invalid(v1::MessageError::TransactionTooLarge));
    }
    Ok(VersionedMessage::V1(msg))
}
