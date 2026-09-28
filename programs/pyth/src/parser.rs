#![allow(clippy::derive_partial_eq_without_eq)]

use borsh::{BorshDeserialize, BorshSerialize};
use shipstern_core::{AccountUpdate, ParseResult, Parser, Prefilter};
use solana_account::Account;
use solana_pubkey::Pubkey;
use solana_sentinel::{PollCtx, PollSource};
use std::borrow::Cow;
use std::collections::HashMap;
use tracing::warn;

/// A price feed together with the account it was read from; both feeds produce this.
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

pub const SOLANA_PRICE_ACCOUNT_SIZE: usize = size_of::<SolanaPriceAccount>();
pub const PRICE_UPDATE_V2_SIZE: usize = 134; // 8 bytes dicriminator + size of PriceUpdateV2 struct without padding

impl PriceFeed {
    pub fn try_from_raw_account(account: &Account) -> anyhow::Result<Self> {
        Self::try_from(&account.data, account.owner)
    }

    pub fn try_from_account_update(update: &AccountUpdate) -> anyhow::Result<PriceFeedUpdate> {
        let Some(acc) = &update.account else {
            anyhow::bail!(
                "account update is missing, expected non-empty for price feed account update"
            );
        };

        let pubkey = Pubkey::try_from(acc.pubkey.as_slice())
            .map_err(|_| anyhow::anyhow!("failed to convert pubkey"))?;
        let owner = Pubkey::try_from(acc.owner.as_slice())
            .map_err(|_| anyhow::anyhow!("failed to convert owner to Pubkey"))?;
        let feed = Self::try_from(&acc.data, owner)?;
        Ok(PriceFeedUpdate { pubkey, feed })
    }

    fn try_from(data: &[u8], owner: Pubkey) -> anyhow::Result<Self> {
        if owner == PUBKEY_LEGACY && data.len() == SOLANA_PRICE_ACCOUNT_SIZE {
            let pf = bytemuck::try_from_bytes::<SolanaPriceAccount>(data)
                .copied()
                .map_err(|_| anyhow::anyhow!("failed to parse legacy price account"))?;
            if pf.magic != 0xa1b2c3d4 {
                return Err(anyhow::anyhow!(
                    "invalid magic or account type for legacy price feed"
                ));
            }
            Ok(Self::Legacy(Box::new(pf)))
        } else if owner == PUBKEY_NEW && data.len() == PRICE_UPDATE_V2_SIZE {
            let mut body = data
                .get(8..) // skip 8-byte discriminator
                .ok_or_else(|| anyhow::anyhow!("invalid account data"))?;

            let pf = PriceUpdateV2::deserialize(&mut body)
                .map_err(|_| anyhow::anyhow!("failed to deserialize PriceUpdateV2"))?;

            Ok(Self::New(pf))
        } else {
            Err(anyhow::anyhow!("unknown data size for price feed"))
        }
    }

    pub fn publish_timestamp_seconds(&self) -> i64 {
        match self {
            Self::Legacy(price_feed) => price_feed.timestamp,
            Self::New(price_feed) => price_feed.price_message.publish_time,
        }
    }

    pub fn price(&self) -> i64 {
        match self {
            Self::Legacy(price_feed) => price_feed.agg.price,
            Self::New(price_feed) => price_feed.price_message.price,
        }
    }

    pub fn feed_id(&self) -> String {
        match self {
            Self::Legacy(price_feed) => bs58::encode(price_feed.prod.to_bytes()).into_string(),
            Self::New(price_feed) => bs58::encode(price_feed.price_message.feed_id).into_string(),
        }
    }

    pub fn exponent(&self) -> i32 {
        match self {
            Self::Legacy(price_feed) => price_feed.expo,
            Self::New(price_feed) => price_feed.price_message.exponent,
        }
    }
}

// New PriceFeedUpdateV2 layout definitions (from pyth-solana-receiver-sdk, as the sdk is not compatible with dependency requirements of this program)
// reference : https://github.com/pyth-network/pyth-crosschain/blob/main/target_chains/solana/pyth_solana_receiver_sdk/src/price_update.rs

pub type FeedId = [u8; 32];

#[derive(Debug, Copy, Clone, PartialEq, BorshSerialize, BorshDeserialize)]
pub enum VerificationLevel {
    /// Partially verified: only `num_signatures` of the guardian signatures
    /// were checked. Borsh-encodes as 2 bytes (1-byte variant tag + the u8).
    Partial { num_signatures: u8 },
    /// Fully verified: all guardian signatures checked. Borsh-encodes as 1 byte
    /// (variant tag only) — this is why the account is variable-length.
    Full,
}

#[derive(Debug, Copy, Clone, PartialEq, BorshSerialize, BorshDeserialize)]
pub struct PriceFeedMessage {
    pub feed_id: [u8; 32],
    pub price: i64,
    pub conf: u64,
    pub exponent: i32,
    /// Timestamp of this price update, in seconds.
    pub publish_time: i64,
    /// Timestamp of the previous price update. For any time t, the unique update
    /// is the one where prev_publish_time < t <= publish_time.
    pub prev_publish_time: i64,
    pub ema_price: i64,
    pub ema_conf: u64,
}

#[derive(Debug, Copy, Clone, PartialEq, BorshSerialize, BorshDeserialize)]
pub struct PriceUpdateV2 {
    pub write_authority: Pubkey,
    pub verification_level: VerificationLevel,
    pub price_message: PriceFeedMessage,
    pub posted_slot: u64,
}

// Legacy PriceAccout Layout definitions (as sdk is not compatible with dependency requirements of this program)
// reference : https://github.com/pyth-network/pyth-sdk-rs/blob/main/pyth-sdk-solana/src/state.rs

use crate::{PUBKEY_LEGACY, PUBKEY_NEW};
use bytemuck::{Pod, Zeroable};

#[repr(u8)]
#[derive(Copy, Clone, PartialEq, Eq, Default, Debug)]
pub enum PriceType {
    #[default]
    Unknown,
    Price,
}

#[repr(u8)]
#[derive(Copy, Clone, PartialEq, Eq, Default, Debug)]
pub enum PriceStatus {
    #[default]
    Unknown,
    Trading,
    Halted,
    Auction,
    Ignored,
}

#[repr(u8)]
#[derive(Copy, Clone, PartialEq, Eq, Default, Debug)]
pub enum CorpAction {
    #[default]
    NoCorpAct,
}

/// A price + confidence at a slot (publisher input or the aggregate).
#[repr(C)]
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub struct PriceInfo {
    pub price: i64,
    pub conf: u64,
    pub status: PriceStatus,
    pub corp_act: CorpAction,
    pub pub_slot: u64,
}

/// One publisher's contribution.
#[repr(C)]
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub struct PriceComp {
    pub publisher: Pubkey,
    pub agg: PriceInfo,
    pub latest: PriceInfo,
}

/// A value also expressed as a rational `numer/denom`.
#[repr(C)]
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub struct Rational {
    pub val: i64,
    pub numer: i64,
    pub denom: i64,
}

/// Legacy Solana Pyth price account. `size_of` == 3312, matching on-chain.
#[repr(C)]
#[derive(PartialEq, Eq, Debug, Clone, Copy)]
pub struct SolanaPriceAccount {
    pub magic: u32,
    pub ver: u32,
    pub atype: u32,
    pub size: u32,
    pub ptype: PriceType,
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

#[cfg(target_endian = "little")]
unsafe impl Zeroable for SolanaPriceAccount {}
#[cfg(target_endian = "little")]
unsafe impl Pod for SolanaPriceAccount {}

// Shipstern Parser

#[derive(Debug, Clone)]
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
        PriceFeed::try_from_account_update(input).map_err(std::convert::Into::into)
    }
}

/// Poll feed: every fetched account that parses as a price feed becomes one item.
#[derive(Debug, Clone, Default)]
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
                    warn!(oracle = %pubkey, error = %e, "failed to parse oracle feed account; skipping");
                    None
                }
            })
            .collect()
    }
}
