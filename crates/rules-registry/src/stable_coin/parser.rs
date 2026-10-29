//! DEX pool decoding for peg checks, over account types generated from the
//! Meteora DLMM, Orca Whirlpool and Raydium CLMM Codama IDLs.

#![allow(clippy::derive_partial_eq_without_eq)]

use anyhow::Context;
use borsh::BorshDeserialize;
use shipstern_core::KeyBytes;
use shipstern_proc_macro::include_shipstern_parser;
use solana_account::Account;
use solana_pubkey::Pubkey;
use solana_sentinel::{PollCtx, PollSource};
use std::collections::HashMap;
use std::fmt;
use tracing::warn;

include_shipstern_parser!("idls/meteora_dlmm/idl.codama.json");
include_shipstern_parser!("idls/orca_whirlpool/idl.codama.json");
include_shipstern_parser!("idls/raydium_clmm/idl.codama.json");

pub type LbPair = meteora_dlmm::LbPair;
pub type Whirlpool = whirlpool::Whirlpool;
pub type PoolState = amm_v3::PoolState;

pub const METEORA_DLMM_PROGRAM_ID: Pubkey = Pubkey::new_from_array(meteora_dlmm::PROGRAM_ID);
pub const ORCA_WHIRLPOOL_PROGRAM_ID: Pubkey = Pubkey::new_from_array(whirlpool::PROGRAM_ID);
pub const RAYDIUM_CLMM_PROGRAM_ID: Pubkey = Pubkey::new_from_array(amm_v3::PROGRAM_ID);

/// Anchor account discriminator; pool state starts right after it.
const DISCRIMINATOR_LEN: usize = 8;

const fn to_pubkey(key: KeyBytes<32>) -> Pubkey {
    Pubkey::new_from_array(key.0)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dex {
    MeteoraDlmm,
    OrcaWhirlpool,
    RaydiumClmm,
}

impl fmt::Display for Dex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::MeteoraDlmm => "Meteora DLMM",
            Self::OrcaWhirlpool => "Orca Whirlpool",
            Self::RaydiumClmm => "Raydium CLMM",
        })
    }
}

/// One DEX pool reduced to what a peg check needs. `raw_price` is quote raw
/// units per base raw unit; `price_of` orients it to a mint and applies decimals.
#[derive(Debug, Clone)]
pub struct Pool {
    pub pubkey: Pubkey,
    pub dex: Dex,
    pub base_mint: Pubkey,
    pub quote_mint: Pubkey,
    pub raw_price: f64,
    /// Raydium stores mint decimals in the pool; the others are filled in by the rule.
    pub base_decimals: Option<u8>,
    pub quote_decimals: Option<u8>,
    /// In-range liquidity for CLMMs; Meteora tracks liquidity per bin, so `None`.
    pub liquidity: Option<u128>,
}

impl Pool {
    /// Decode a pool account, picking the layout from the owning DEX program.
    pub fn parse(pubkey: Pubkey, account: &Account) -> anyhow::Result<Self> {
        let mut body = account
            .data
            .get(DISCRIMINATOR_LEN..)
            .with_context(|| format!("pool {pubkey} data is shorter than a discriminator"))?;
        match account.owner {
            METEORA_DLMM_PROGRAM_ID => {
                let pair = LbPair::deserialize(&mut body)
                    .with_context(|| format!("failed to decode Meteora LbPair {pubkey}"))?;
                Ok(Self {
                    pubkey,
                    dex: Dex::MeteoraDlmm,
                    base_mint: to_pubkey(pair.token_x_mint),
                    quote_mint: to_pubkey(pair.token_y_mint),
                    raw_price: meteora_bin_price(pair.active_id, pair.bin_step),
                    base_decimals: None,
                    quote_decimals: None,
                    liquidity: None,
                })
            }
            ORCA_WHIRLPOOL_PROGRAM_ID => {
                let pool = Whirlpool::deserialize(&mut body)
                    .with_context(|| format!("failed to decode Orca Whirlpool {pubkey}"))?;
                Ok(Self {
                    pubkey,
                    dex: Dex::OrcaWhirlpool,
                    base_mint: to_pubkey(pool.token_mint_a),
                    quote_mint: to_pubkey(pool.token_mint_b),
                    raw_price: sqrt_price_x64_to_price(pool.sqrt_price),
                    base_decimals: None,
                    quote_decimals: None,
                    liquidity: Some(pool.liquidity),
                })
            }
            RAYDIUM_CLMM_PROGRAM_ID => {
                let pool = PoolState::deserialize(&mut body)
                    .with_context(|| format!("failed to decode Raydium PoolState {pubkey}"))?;
                Ok(Self {
                    pubkey,
                    dex: Dex::RaydiumClmm,
                    base_mint: to_pubkey(pool.token_mint0),
                    quote_mint: to_pubkey(pool.token_mint1),
                    raw_price: sqrt_price_x64_to_price(pool.sqrt_price_x64),
                    base_decimals: Some(pool.mint_decimals0),
                    quote_decimals: Some(pool.mint_decimals1),
                    liquidity: Some(pool.liquidity),
                })
            }
            owner => anyhow::bail!("pool {pubkey} is owned by unsupported program {owner}"),
        }
    }

    pub fn contains(&self, mint: &Pubkey) -> bool {
        self.base_mint == *mint || self.quote_mint == *mint
    }

    /// The mint on the other side of the pool from `mint`.
    pub fn counterpart(&self, mint: &Pubkey) -> Option<Pubkey> {
        if self.base_mint == *mint {
            Some(self.quote_mint)
        } else if self.quote_mint == *mint {
            Some(self.base_mint)
        } else {
            None
        }
    }

    /// Price of one whole `mint` token in whole counterpart tokens; `None` if the
    /// mint is not in this pool or decimals are not yet known.
    pub fn price_of(&self, mint: &Pubkey) -> Option<f64> {
        let (base_decimals, quote_decimals) = (self.base_decimals?, self.quote_decimals?);
        let price =
            self.raw_price * 10f64.powi(i32::from(base_decimals) - i32::from(quote_decimals));
        if self.base_mint == *mint {
            Some(price)
        } else if self.quote_mint == *mint {
            Some(1.0 / price)
        } else {
            None
        }
    }
}

/// Poll feed: every fetched account that decodes as a pool becomes one item.
#[derive(Debug, Clone, Copy, Default)]
pub struct PoolSource;

impl PollSource for PoolSource {
    type Item = Pool;

    fn parse(&self, fetched: &HashMap<Pubkey, Account>, _ctx: &PollCtx) -> Vec<Self::Item> {
        fetched
            .iter()
            .filter_map(|(pubkey, account)| match Pool::parse(*pubkey, account) {
                Ok(pool) => Some(pool),
                Err(e) => {
                    warn!(pool = %pubkey, error = %e, "failed to parse pool account; skipping");
                    None
                }
            })
            .collect()
    }
}

/// Meteora DLMM: the active bin's price is `(1 + bin_step / 10_000)^active_id`.
pub fn meteora_bin_price(active_id: i32, bin_step: u16) -> f64 {
    (1.0 + f64::from(bin_step) / 10_000.0).powi(active_id)
}

/// Orca / Raydium CLMM: sqrt price is Q64.64 fixed point, price = (sqrt / 2^64)^2.
pub fn sqrt_price_x64_to_price(sqrt_price_x64: u128) -> f64 {
    let sqrt_price = sqrt_price_x64 as f64 / 2f64.powi(64);
    sqrt_price * sqrt_price
}

#[cfg(test)]
mod tests {
    use super::*;

    const JUPUSD: Pubkey = Pubkey::from_str_const("JuprjznTrTSp2UFa3ZBUFgwdAmtZCq4MQCwysN55USD");
    const USDC: Pubkey = Pubkey::from_str_const("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v");

    fn approx(got: f64, want: f64) {
        assert!((got - want).abs() < 1e-9, "got {got}, want {want}");
    }

    // Raw mainnet JupUSD/USDC pool accounts, all at tick/bin -5; guards the
    // generated Borsh layouts and the price math together.
    #[test]
    fn decodes_live_pool_fixtures() {
        let cases = [
            (
                Dex::MeteoraDlmm,
                METEORA_DLMM_PROGRAM_ID,
                &include_bytes!("../../fixtures/stable_coin/meteora_lb_pair.bin")[..],
                0.999_500_149_965_007,
                None,
            ),
            (
                Dex::OrcaWhirlpool,
                ORCA_WHIRLPOOL_PROGRAM_ID,
                &include_bytes!("../../fixtures/stable_coin/orca_whirlpool.bin")[..],
                0.999_541_556_108_003,
                None,
            ),
            (
                Dex::RaydiumClmm,
                RAYDIUM_CLMM_PROGRAM_ID,
                &include_bytes!("../../fixtures/stable_coin/raydium_pool_state.bin")[..],
                0.999_548_152_462_960,
                Some(6),
            ),
        ];

        for (dex, owner, data, raw_price, decimals) in cases {
            let account = Account {
                lamports: 0,
                data: data.to_vec(),
                owner,
                executable: false,
                rent_epoch: 0,
            };
            let mut pool = Pool::parse(Pubkey::new_unique(), &account).unwrap();
            assert_eq!(pool.dex, dex);
            assert_eq!((pool.base_mint, pool.quote_mint), (JUPUSD, USDC));
            assert_eq!(
                (pool.base_decimals, pool.quote_decimals),
                (decimals, decimals)
            );
            approx(pool.raw_price, raw_price);

            pool.base_decimals = Some(6);
            pool.quote_decimals = Some(6);
            approx(pool.price_of(&JUPUSD).unwrap(), raw_price);
            approx(pool.price_of(&USDC).unwrap(), 1.0 / raw_price);
        }
    }
}
