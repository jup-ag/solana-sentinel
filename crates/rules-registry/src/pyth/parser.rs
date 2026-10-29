//! Pyth price account layouts, hand-ported because the SDKs pin incompatible deps:
//! `PriceUpdateV2` from pyth-solana-receiver-sdk and the legacy `SolanaPriceAccount`
//! from pyth-sdk-solana.

use crate::pyth::{PYTH_LEGACY_PROGRAM_ID, PYTH_RECEIVER_PROGRAM_ID};
use borsh::BorshDeserialize;
use bytemuck::{Pod, Zeroable};
use shipstern_core::{AccountUpdate, ParseResult, Parser, Prefilter};
use solana_account::Account;
use solana_pubkey::Pubkey;
use solana_sentinel::{PollCtx, PollSource};
use std::borrow::Cow;
use std::collections::HashMap;
use tracing::warn;

pub const SOLANA_PRICE_ACCOUNT_SIZE: usize = size_of::<SolanaPriceAccount>();
/// 8-byte discriminator + the largest `PriceUpdateV2` encoding.
pub const PRICE_UPDATE_V2_SIZE: usize = 134;
const LEGACY_MAGIC: u32 = 0xa1b2_c3d4;

/// A decoded feed and the account it was read from; both feeds produce this.
#[derive(Debug, Clone)]
pub struct PriceFeedUpdate {
    pub pubkey: Pubkey,
    pub feed: PriceFeed,
}

#[derive(Debug, Clone)]
pub enum PriceFeed {
    Legacy(Box<SolanaPriceAccount>),
    New(PriceUpdateV2),
}

impl PriceFeed {
    pub fn try_from_raw_account(account: &Account) -> anyhow::Result<Self> {
        Self::decode(&account.data, account.owner)
    }

    pub fn try_from_account_update(update: &AccountUpdate) -> anyhow::Result<PriceFeedUpdate> {
        let acc = update
            .account
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("price feed account update has no account"))?;
        let pubkey = Pubkey::try_from(acc.pubkey.as_slice())
            .map_err(|_| anyhow::anyhow!("invalid account pubkey"))?;
        let owner = Pubkey::try_from(acc.owner.as_slice())
            .map_err(|_| anyhow::anyhow!("invalid owner pubkey"))?;
        Ok(PriceFeedUpdate {
            pubkey,
            feed: Self::decode(&acc.data, owner)?,
        })
    }

    fn decode(data: &[u8], owner: Pubkey) -> anyhow::Result<Self> {
        if owner == PYTH_LEGACY_PROGRAM_ID && data.len() == SOLANA_PRICE_ACCOUNT_SIZE {
            let pf = bytemuck::try_from_bytes::<SolanaPriceAccount>(data)
                .copied()
                .map_err(|_| anyhow::anyhow!("failed to parse legacy price account"))?;
            anyhow::ensure!(pf.magic == LEGACY_MAGIC, "invalid legacy price feed magic");
            Ok(Self::Legacy(Box::new(pf)))
        } else if owner == PYTH_RECEIVER_PROGRAM_ID && data.len() == PRICE_UPDATE_V2_SIZE {
            // A `Full` verification level leaves one trailing byte, which borsh ignores here.
            let pf = PriceUpdateV2::deserialize(&mut &data[8..])
                .map_err(|_| anyhow::anyhow!("failed to deserialize PriceUpdateV2"))?;
            Ok(Self::New(pf))
        } else {
            anyhow::bail!("account owned by {owner} is not a Pyth price feed")
        }
    }

    pub fn publish_timestamp_seconds(&self) -> i64 {
        match self {
            Self::Legacy(pf) => pf.timestamp,
            Self::New(pf) => pf.price_message.publish_time,
        }
    }

    pub fn price(&self) -> i64 {
        match self {
            Self::Legacy(pf) => pf.agg.price,
            Self::New(pf) => pf.price_message.price,
        }
    }

    pub fn exponent(&self) -> i32 {
        match self {
            Self::Legacy(pf) => pf.expo,
            Self::New(pf) => pf.price_message.exponent,
        }
    }

    /// Legacy feeds are identified by their product account.
    pub fn feed_id(&self) -> String {
        match self {
            Self::Legacy(pf) => bs58::encode(pf.prod.to_bytes()).into_string(),
            Self::New(pf) => bs58::encode(pf.price_message.feed_id).into_string(),
        }
    }
}

pub type FeedId = [u8; 32];

#[derive(Debug, Copy, Clone, PartialEq, Eq, BorshDeserialize)]
pub enum VerificationLevel {
    /// Only `num_signatures` guardian signatures checked; encodes as 2 bytes.
    Partial { num_signatures: u8 },
    /// Every guardian signature checked; encodes as 1 byte.
    Full,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, BorshDeserialize)]
pub struct PriceFeedMessage {
    pub feed_id: FeedId,
    pub price: i64,
    pub conf: u64,
    pub exponent: i32,
    pub publish_time: i64,
    pub prev_publish_time: i64,
    pub ema_price: i64,
    pub ema_conf: u64,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, BorshDeserialize)]
pub struct PriceUpdateV2 {
    pub write_authority: [u8; 32],
    pub verification_level: VerificationLevel,
    pub price_message: PriceFeedMessage,
    pub posted_slot: u64,
}

/// A price + confidence at a slot. `status` and `corp_act` stay raw bytes so the
/// `Pod` cast never sees an invalid enum discriminant.
#[repr(C)]
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub struct PriceInfo {
    pub price: i64,
    pub conf: u64,
    pub status: u8,
    pub corp_act: u8,
    pub pub_slot: u64,
}

#[repr(C)]
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub struct PriceComp {
    pub publisher: Pubkey,
    pub agg: PriceInfo,
    pub latest: PriceInfo,
}

#[repr(C)]
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub struct Rational {
    pub val: i64,
    pub numer: i64,
    pub denom: i64,
}

/// Legacy Solana Pyth price account; `size_of` is 3312, matching on chain.
#[repr(C)]
#[derive(PartialEq, Eq, Debug, Clone, Copy)]
pub struct SolanaPriceAccount {
    pub magic: u32,
    pub ver: u32,
    pub atype: u32,
    pub size: u32,
    pub ptype: u8,
    pub expo: i32,
    pub num: u32,
    pub num_qt: u32,
    pub last_slot: u64,
    pub valid_slot: u64,
    pub ema_price: Rational,
    pub ema_conf: Rational,
    pub timestamp: i64,
    pub min_pub: u8,
    pub drv2: u8,
    pub drv3: u16,
    pub drv4: u32,
    pub prod: Pubkey,
    pub next: Pubkey,
    pub prev_slot: u64,
    pub prev_price: i64,
    pub prev_conf: u64,
    pub prev_timestamp: i64,
    pub agg: PriceInfo,
    pub comp: [PriceComp; 32],
}

// SAFETY: every field is an integer, byte array or `Pubkey`, so any bit pattern is valid.
#[cfg(target_endian = "little")]
unsafe impl Zeroable for SolanaPriceAccount {}
#[cfg(target_endian = "little")]
unsafe impl Pod for SolanaPriceAccount {}

/// Stream feed: every account update that decodes as a price feed.
#[derive(Debug, Clone, Copy, Default)]
pub struct PythPriceFeedParser;

impl Parser for PythPriceFeedParser {
    type Input = AccountUpdate;
    type Output = PriceFeedUpdate;

    fn id(&self) -> Cow<'static, str> {
        Cow::Borrowed("pyth_price_feed_parser")
    }

    fn prefilter(&self) -> Prefilter {
        Prefilter::default()
    }

    async fn parse(&self, input: &Self::Input) -> ParseResult<Self::Output> {
        PriceFeed::try_from_account_update(input).map_err(Into::into)
    }
}

/// Poll feed: every fetched account that decodes as a price feed becomes one item.
#[derive(Debug, Clone, Copy, Default)]
pub struct PriceFeedSource;

impl PollSource for PriceFeedSource {
    type Item = PriceFeedUpdate;

    fn parse(&self, fetched: &HashMap<Pubkey, Account>, _ctx: &PollCtx) -> Vec<Self::Item> {
        fetched
            .iter()
            .filter_map(|(pubkey, raw)| match PriceFeed::try_from_raw_account(raw) {
                Ok(feed) => Some(PriceFeedUpdate {
                    pubkey: *pubkey,
                    feed,
                }),
                Err(e) => {
                    warn!(oracle = %pubkey, error = %e, "failed to parse oracle feed; skipping");
                    None
                }
            })
            .collect()
    }
}

#[cfg(test)]
pub(crate) mod fixtures {
    /// `PriceUpdateV2` bytes laid out by offset, independent of the borsh structs.
    pub fn price_update_v2(price: i64, exponent: i32, publish_time: i64) -> Vec<u8> {
        let mut data = vec![34, 241, 35, 99, 157, 126, 244, 205]; // discriminator
        data.extend([9; 32]); // write_authority
        data.push(1); // VerificationLevel::Full
        data.extend([5; 32]); // feed_id
        data.extend(price.to_le_bytes());
        data.extend(10u64.to_le_bytes()); // conf
        data.extend(exponent.to_le_bytes());
        data.extend(publish_time.to_le_bytes());
        data.extend((publish_time - 1).to_le_bytes()); // prev_publish_time
        data.extend(price.to_le_bytes()); // ema_price
        data.extend(10u64.to_le_bytes()); // ema_conf
        data.extend(42u64.to_le_bytes()); // posted_slot
        data.push(0); // unused tail of the 134-byte allocation
        data
    }
}
