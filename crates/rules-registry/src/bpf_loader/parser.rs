//! Account parser for `UpgradeableLoaderState::ProgramData`. Only the 45-byte
//! header is decoded; the ELF that follows is ignored.
//!
//! Layout (bincode): `u32` variant tag (3 = ProgramData), `u64` slot, then an
//! `Option<Pubkey>` upgrade authority as a `u8` tag followed by 32 bytes.

use crate::bpf_loader::UPGRADEABLE_LOADER_ID;
use shipstern_core::{AccountUpdate, ParseError, ParseResult, Parser, Prefilter};
use solana_pubkey::Pubkey;
use std::borrow::Cow;

pub const PROGRAM_DATA_HEADER_LEN: usize = 45;
const PROGRAM_DATA_TAG: u32 = 3;

/// The header of one ProgramData account.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProgramData {
    pub address: Pubkey,
    pub last_upgrade_slot: u64,
    pub upgrade_authority: Option<Pubkey>,
}

/// A decoded header before it is tied to an address.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Header {
    pub last_upgrade_slot: u64,
    pub upgrade_authority: Option<Pubkey>,
}

impl Header {
    pub fn at(self, address: Pubkey) -> ProgramData {
        ProgramData {
            address,
            last_upgrade_slot: self.last_upgrade_slot,
            upgrade_authority: self.upgrade_authority,
        }
    }
}

/// `None` unless `data` starts with a ProgramData header.
pub fn decode_header(data: &[u8]) -> Option<Header> {
    if data.len() < PROGRAM_DATA_HEADER_LEN {
        return None;
    }
    if u32::from_le_bytes(data[..4].try_into().ok()?) != PROGRAM_DATA_TAG {
        return None;
    }
    let last_upgrade_slot = u64::from_le_bytes(data[4..12].try_into().ok()?);
    let upgrade_authority = match data[12] {
        0 => None,
        1 => Some(Pubkey::new_from_array(data[13..45].try_into().ok()?)),
        _ => return None,
    };
    Some(Header {
        last_upgrade_slot,
        upgrade_authority,
    })
}

#[derive(Debug, Clone, Default)]
pub struct ProgramDataParser;

impl Parser for ProgramDataParser {
    type Input = AccountUpdate;
    type Output = ProgramData;

    fn id(&self) -> Cow<'static, str> {
        Cow::Borrowed("program_data")
    }

    fn prefilter(&self) -> Prefilter {
        Prefilter::default()
    }

    async fn parse(&self, input: &Self::Input) -> ParseResult<Self::Output> {
        let acc = input.account.as_ref().ok_or(ParseError::Filtered)?;
        let owner = Pubkey::try_from(acc.owner.as_slice()).map_err(|_| ParseError::Filtered)?;
        if owner != UPGRADEABLE_LOADER_ID {
            return Err(ParseError::Filtered);
        }
        let address = Pubkey::try_from(acc.pubkey.as_slice()).map_err(|_| ParseError::Filtered)?;
        let header = decode_header(&acc.data).ok_or(ParseError::Filtered)?;
        Ok(header.at(address))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_program_data_header() {
        let authority = Pubkey::new_unique();
        let mut data = 3u32.to_le_bytes().to_vec();
        data.extend_from_slice(&123_456u64.to_le_bytes());
        data.push(1);
        data.extend_from_slice(authority.as_ref());
        data.extend_from_slice(&[0xAA; 16]); // start of the ELF, ignored
        assert_eq!(
            decode_header(&data),
            Some(Header {
                last_upgrade_slot: 123_456,
                upgrade_authority: Some(authority)
            })
        );

        // Immutable program: authority tag 0 with zeroed bytes after it.
        let mut immutable = data[..12].to_vec();
        immutable.extend_from_slice(&[0; 33]);
        assert_eq!(decode_header(&immutable).unwrap().upgrade_authority, None);

        // A Program (tag 2) or Buffer (tag 1) account is not ProgramData.
        let mut program = data.clone();
        program[..4].copy_from_slice(&2u32.to_le_bytes());
        assert_eq!(decode_header(&program), None);
    }
}
