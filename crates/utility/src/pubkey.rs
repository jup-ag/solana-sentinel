use serde::{Deserialize, Deserializer};
use solana_pubkey::Pubkey;
use std::collections::HashMap;
use std::str::FromStr;

// Pubkey helpers for rule config and parsed events.

/// Deserialize a base58 pubkey string.
pub fn deserialize_pubkey<'de, D>(deserializer: D) -> Result<Pubkey, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    Pubkey::from_str(&s).map_err(serde::de::Error::custom)
}

/// Deserialize a TOML/JSON array of base58 pubkey strings.
pub fn deserialize_pubkey_vec<'de, D>(deserializer: D) -> Result<Vec<Pubkey>, D::Error>
where
    D: Deserializer<'de>,
{
    let strs = Vec::<String>::deserialize(deserializer)?;
    strs.iter()
        .map(|s| Pubkey::from_str(s).map_err(serde::de::Error::custom))
        .collect()
}

/// Deserialize a TOML/JSON object keyed by base58 pubkey strings.
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

/// Parsed events carry raw 32-byte keys; this turns one into a `Pubkey`.
pub fn to_solana_pubkey(bytes: [u8; 32]) -> Pubkey {
    Pubkey::new_from_array(bytes)
}
