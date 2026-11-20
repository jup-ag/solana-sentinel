//! `[riposte]` in the service config.

use crate::fees::FeeConfig;
use serde::de::Error as _;
use serde::{Deserialize, Deserializer};
use solana_pubkey::Pubkey;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use url::Url;

#[derive(Debug, Clone, Deserialize)]
pub struct RiposteConfig {
    /// Off means no rule's `counteract` runs; rules may keep `riposte = true`.
    #[serde(default = "defaults::enabled")]
    pub enabled: bool,
    #[serde(default)]
    pub mode: Mode,
    /// Program name (as in rules.toml) → key into `signers`. That signer is the program's
    /// on-chain authority and pays the fee, so each tx has one signature. Scoped per program
    /// so one leaked key can act on one program only.
    pub programs: HashMap<String, String>,
    pub signers: HashMap<String, SignerConfig>,
    /// CU price from Triton's percentile fees on `[rpc]`; sent as a v1 total fee in lamports.
    pub fee: FeeConfig,
    /// Programs a response may invoke (base58); compute budget is set in the message config.
    #[serde(deserialize_with = "pubkeys")]
    pub allowed_programs: HashSet<Pubkey>,
    #[serde(default)]
    pub broadcasters: Vec<BroadcasterConfig>,
    /// Per rule and dedup key: the same condition is answered at most once per window, counted
    /// from the claim. Also what stops a second pod while the first tx is still landing.
    #[serde(default = "defaults::cooldown_seconds")]
    pub cooldown_seconds: u64,
    /// Below this, a program signer's balance is logged as an error every minute.
    #[serde(default = "defaults::min_balance_lamports")]
    pub min_balance_lamports: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    /// Build, sign and simulate over `[rpc]`; never send.
    #[default]
    DryRun,
    Live,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SignerConfig {
    /// Ed25519 key in AWS KMS (`ECC_NIST_EDWARDS25519`); credentials from the default chain.
    AwsKms {
        key_id: String,
        region: Option<String>,
    },
    /// Solana CLI keypair JSON. Devnet and tests only.
    KeypairFile { path: PathBuf },
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum BroadcasterConfig {
    Rpc {
        id: String,
        url: Url,
        #[serde(default)]
        headers: HashMap<String, String>,
    },
}

fn pubkeys<'de, D: Deserializer<'de>>(d: D) -> Result<HashSet<Pubkey>, D::Error> {
    Vec::<String>::deserialize(d)?
        .iter()
        .map(|s| {
            s.parse()
                .map_err(|e| D::Error::custom(format!("invalid program id `{s}`: {e}")))
        })
        .collect()
}

mod defaults {
    pub fn enabled() -> bool {
        true
    }
    /// Also the duplicate barrier across pods; must outlive landing (min 180).
    pub fn cooldown_seconds() -> u64 {
        600
    }
    /// 0.05 SOL: thousands of countermeasures at typical fees.
    pub fn min_balance_lamports() -> u64 {
        50_000_000
    }
}
