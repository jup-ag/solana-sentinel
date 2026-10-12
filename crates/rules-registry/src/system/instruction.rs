//! Decoder for the system instructions the rules care about. The wire format
//! is bincode: a `u32` little-endian variant tag, then the variant's fields.

use crate::system::SYSTEM_PROGRAM_ID;
use solana_pubkey::Pubkey;
use solana_sentinel::raw::RawInstruction;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SystemIx {
    Transfer {
        from: Pubkey,
        to: Pubkey,
        lamports: u64,
    },
    InitializeNonce {
        nonce: Pubkey,
        authority: Pubkey,
    },
    AuthorizeNonce {
        nonce: Pubkey,
        authority: Pubkey,
        new_authority: Pubkey,
    },
    WithdrawNonce {
        nonce: Pubkey,
        to: Pubkey,
        authority: Pubkey,
        lamports: u64,
    },
}

const TRANSFER: u32 = 2;
const WITHDRAW_NONCE: u32 = 5;
const INITIALIZE_NONCE: u32 = 6;
const AUTHORIZE_NONCE: u32 = 7;
const TRANSFER_WITH_SEED: u32 = 11;

/// `None` for other programs, other variants, or malformed data.
pub fn decode(ix: &RawInstruction) -> Option<SystemIx> {
    if ix.program_id != SYSTEM_PROGRAM_ID || ix.data.len() < 4 {
        return None;
    }
    let tag = u32::from_le_bytes(ix.data[..4].try_into().ok()?);
    let body = &ix.data[4..];
    let account = |i: usize| ix.accounts.get(i).copied();
    Some(match tag {
        TRANSFER => SystemIx::Transfer {
            from: account(0)?,
            to: account(1)?,
            lamports: read_u64(body)?,
        },
        // Accounts: [from, base, to]; the seed and owner after the amount are not needed.
        TRANSFER_WITH_SEED => SystemIx::Transfer {
            from: account(0)?,
            to: account(2)?,
            lamports: read_u64(body)?,
        },
        INITIALIZE_NONCE => SystemIx::InitializeNonce {
            nonce: account(0)?,
            authority: read_pubkey(body)?,
        },
        AUTHORIZE_NONCE => SystemIx::AuthorizeNonce {
            nonce: account(0)?,
            authority: account(1)?,
            new_authority: read_pubkey(body)?,
        },
        WITHDRAW_NONCE => SystemIx::WithdrawNonce {
            nonce: account(0)?,
            to: account(1)?,
            authority: account(4)?,
            lamports: read_u64(body)?,
        },
        _ => return None,
    })
}

fn read_u64(body: &[u8]) -> Option<u64> {
    Some(u64::from_le_bytes(body.get(..8)?.try_into().ok()?))
}

fn read_pubkey(body: &[u8]) -> Option<Pubkey> {
    Some(Pubkey::new_from_array(body.get(..32)?.try_into().ok()?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sentinel::raw::RawInstructionParser;
    use solana_sentinel::testing::{ix_update, parse};

    #[tokio::test]
    async fn decodes_transfer_and_authorize_nonce_layouts() {
        let (a, b, c) = (
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
        );

        let mut data = 2u32.to_le_bytes().to_vec();
        data.extend_from_slice(&5_000u64.to_le_bytes());
        let ix = parse(
            &RawInstructionParser,
            &ix_update(SYSTEM_PROGRAM_ID, &[a, b], data),
        )
        .await;
        assert_eq!(
            decode(&ix),
            Some(SystemIx::Transfer {
                from: a,
                to: b,
                lamports: 5_000
            })
        );

        let mut data = 7u32.to_le_bytes().to_vec();
        data.extend_from_slice(c.as_ref());
        let ix = parse(
            &RawInstructionParser,
            &ix_update(SYSTEM_PROGRAM_ID, &[a, b], data),
        )
        .await;
        assert_eq!(
            decode(&ix),
            Some(SystemIx::AuthorizeNonce {
                nonce: a,
                authority: b,
                new_authority: c
            })
        );
    }
}
