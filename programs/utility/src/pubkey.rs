use crate::constants::{
    BPF_LOADER_UPGRADEABLE_PROGRAM_ID, SPL_ASSOCIATED_TOKEN_PROGRAM_ID, SPL_TOKEN_2022_PROGRAM_ID,
    SPL_TOKEN_PROGRAM_ID,
};
use serde::{Deserialize, Deserializer};
use solana_pubkey::Pubkey;
use std::collections::HashMap;
use std::str::FromStr;

// Commonly pubkey deserialization helpers for rules.

pub fn deserialize_pubkey<'de, D>(deserializer: D) -> Result<Pubkey, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    Pubkey::from_str(&s).map_err(serde::de::Error::custom)
}

pub fn deserialize_pubkey_vec<'de, D>(deserializer: D) -> Result<Vec<Pubkey>, D::Error>
where
    D: Deserializer<'de>,
{
    let strs = Vec::<String>::deserialize(deserializer)?;
    strs.iter()
        .map(|s| Pubkey::from_str(s).map_err(serde::de::Error::custom))
        .collect()
}

/// Deserialize a TOML/JSON array of base58 pubkey strings.
pub fn deserialize_pubkey_array<'de, D>(deserializer: D) -> Result<Vec<Pubkey>, D::Error>
where
    D: Deserializer<'de>,
{
    let strs = Vec::<String>::deserialize(deserializer)?;
    strs.iter()
        .map(|s| Pubkey::from_str(s).map_err(serde::de::Error::custom))
        .collect()
}

/// Deserialize a TOML/JSON object whose keys are base58 pubkey strings
/// and values are bps thresholds.
pub fn deserialize_pubkey_map<'de, D, T>(deserializer: D) -> Result<HashMap<Pubkey, T>, D::Error>
where
    D: Deserializer<'de>,
    T: serde::de::DeserializeOwned,
{
    let raw: HashMap<String, T> = HashMap::deserialize(deserializer)?;
    raw.into_iter()
        .map(|(k, v)| {
            Pubkey::from_str(&k)
                .map(|pk| (pk, v))
                .map_err(serde::de::Error::custom)
        })
        .collect()
}

/// Derive the associated token account address for a given wallet and mint.
pub fn derive_spl_ata_address(wallet: &Pubkey, mint: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(
        &[
            wallet.as_ref(),
            SPL_TOKEN_PROGRAM_ID.as_ref(),
            mint.as_ref(),
        ],
        &SPL_ASSOCIATED_TOKEN_PROGRAM_ID,
    )
    .0
}

/// Derive the associated token account address for a given wallet and mint (Token2022).
pub fn derive_spl_2022_ata_address(wallet: &Pubkey, mint: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(
        &[
            wallet.as_ref(),
            SPL_TOKEN_2022_PROGRAM_ID.as_ref(),
            mint.as_ref(),
        ],
        &SPL_ASSOCIATED_TOKEN_PROGRAM_ID,
    )
    .0
}

/// Derive program data account address for a given program id and seed.
pub fn derive_program_data_address(program_id: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(&[program_id.as_ref()], &BPF_LOADER_UPGRADEABLE_PROGRAM_ID).0
}

pub fn join_pubkeys(keys: &[Pubkey]) -> String {
    keys.iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(",")
}
