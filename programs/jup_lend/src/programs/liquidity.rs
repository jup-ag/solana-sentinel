#![allow(clippy::derive_partial_eq_without_eq)]

use interface::Prefilter as WatchPrefilter;
use shipstern_core::{AccountUpdate, ParseError, Parser};
use shipstern_parser::prelude::{ParseResult, Prefilter};
use shipstern_proc_macro::include_shipstern_parser;
use solana_pubkey::Pubkey;
use spl_token_2022_interface::extension::StateWithExtensions;
use spl_token_2022_interface::state::Account as TokenAccount;
use std::borrow::Cow;
use utility::constants::{SPL_TOKEN_2022_PROGRAM_ID, SPL_TOKEN_PROGRAM_ID};
use utility::geyser::get_tx_sig_from_acc_update;

include_shipstern_parser!("idls/liquidity-idl.codama.json");

pub type AuthorizationList = jup_lend_liquidity::AuthorizationList;
pub type LiquidityAccountParser = jup_lend_liquidity::AccountParser;
pub type LiquidityState = jup_lend_liquidity::account::Account;
pub type TokenReserve = jup_lend_liquidity::TokenReserve;
pub type UserClass = jup_lend_liquidity::UserClass;
pub type TokenConfig = jup_lend_liquidity::TokenConfig;
pub type UserSupplyConfig = jup_lend_liquidity::UserSupplyConfig;
pub type UserBorrowConfig = jup_lend_liquidity::UserBorrowConfig;
pub type RateDataV1Params = jup_lend_liquidity::RateDataV1Params;
pub type RateDataV2Params = jup_lend_liquidity::RateDataV2Params;

#[derive(Debug)]
pub enum LiquidityConfig {
    TokenConfig(TokenConfig),
    UserSupplyConfig(UserSupplyConfig),
    UserBorrowConfig(UserBorrowConfig),
    RateDataV1Params(RateDataV1Params),
    RateDataV2Params(RateDataV2Params),
}

pub const JUP_LIQUIDITY_PROGRAM_ID: Pubkey = Pubkey::new_from_array(jup_lend_liquidity::PROGRAM_ID);

const AUTH_LIST_PDA_SEED: &[u8] = b"auth_list";

/// Exchange prices in `TokenReserve` are fixed-point with 12 decimals.
pub const PRICE_PRECISION: u128 = 1_000_000_000_000;

pub fn shares_to_tokens(shares: u64, raw_price: u64) -> u128 {
    (shares as u128 * raw_price as u128) / PRICE_PRECISION
}

impl AuthorizationList {
    pub fn derive_pda() -> Pubkey {
        Pubkey::find_program_address(&[AUTH_LIST_PDA_SEED], &JUP_LIQUIDITY_PROGRAM_ID).0
    }
}

impl TokenReserve {
    /// Every reserve of the liquidity program, selected server-side by discriminator, so
    /// reserves created after startup are streamed without a restart.
    pub fn subscription() -> WatchPrefilter {
        WatchPrefilter::new().owned(
            JUP_LIQUIDITY_PROGRAM_ID,
            [WatchPrefilter::memcmp(
                Self::DISCRIMINATOR_OFFSET,
                Self::DISCRIMINATOR,
            )],
        )
    }
}

// Account updates scoped to the liquidity program; reserve vaults are SPL token accounts but belong to a reserve, so they are modeled here.
#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum LiquidityStateUpdate {
    TokenReserve {
        reserve_pubkey: Pubkey,
        reserve: TokenReserve,
        tx_signature: String, // Signature of the transaction that produced this update
    },
    VaultAccount {
        vault_pubkey: Pubkey,
        token_account: TokenAccount,
        tx_signature: String, // Signature of the transaction that produced this update
    },
    AuthorizationList {
        auth_list_pubkey: Pubkey,
        state: AuthorizationList,
        tx_signature: String, // Signature of the transaction that produced this update
    },
    Noop,
}

pub async fn try_parse_liquidity_state(
    update: &AccountUpdate,
    parser: &LiquidityAccountParser,
) -> anyhow::Result<LiquidityStateUpdate> {
    let Some(acc) = &update.account else {
        anyhow::bail!("account update is missing, expected non-empty for liquidity account update")
    };

    let acc_pubkey = Pubkey::new_from_array(
        acc.pubkey
            .clone()
            .try_into()
            .map_err(|_| anyhow::anyhow!("failed to convert to Pubkey"))?,
    );

    let res = parser
        .parse(update)
        .await
        .map_err(|e| anyhow::anyhow!("failed to parse account update :{:?}", e))?;

    let state = match res.account {
        LiquidityState::AuthorizationList(auth_list) => LiquidityStateUpdate::AuthorizationList {
            auth_list_pubkey: acc_pubkey,
            state: auth_list,
            tx_signature: get_tx_sig_from_acc_update(update),
        },
        LiquidityState::TokenReserve(reserve) => LiquidityStateUpdate::TokenReserve {
            reserve_pubkey: acc_pubkey,
            reserve,
            tx_signature: get_tx_sig_from_acc_update(update),
        },
        _ => LiquidityStateUpdate::Noop,
    };

    Ok(state)
}

// The Token-2022 unpacker handles both token programs: classic accounts are the base layout with no extension data.
pub fn try_parse_vault_account(update: &AccountUpdate) -> anyhow::Result<LiquidityStateUpdate> {
    let Some(acc) = &update.account else {
        anyhow::bail!("account update is missing, expected non-empty for vault account update")
    };

    let vault_pubkey = Pubkey::new_from_array(
        acc.pubkey
            .clone()
            .try_into()
            .map_err(|_| anyhow::anyhow!("failed to convert to Pubkey"))?,
    );

    let token_account = StateWithExtensions::<TokenAccount>::unpack(&acc.data)
        .map_err(|e| anyhow::anyhow!("failed to unpack vault token account :{:?}", e))?;

    Ok(LiquidityStateUpdate::VaultAccount {
        vault_pubkey,
        token_account: token_account.base,
        tx_signature: get_tx_sig_from_acc_update(update),
    })
}

// Parses liquidity-program account updates, dispatching on the account owner into LiquidityStateUpdate.
#[derive(Debug, Clone)]
pub struct LiquidityProgramStateParser {
    acc_parser: LiquidityAccountParser,
}

impl Default for LiquidityProgramStateParser {
    fn default() -> Self {
        Self::new()
    }
}

impl LiquidityProgramStateParser {
    pub fn new() -> Self {
        Self {
            acc_parser: LiquidityAccountParser {},
        }
    }
}

impl Parser for LiquidityProgramStateParser {
    type Input = AccountUpdate;

    type Output = LiquidityStateUpdate;

    fn id(&self) -> Cow<'static, str> {
        Cow::Borrowed("jup_lend_liquidity_state_parser")
    }

    fn prefilter(&self) -> Prefilter {
        Prefilter::default()
    }

    async fn parse(&self, input: &Self::Input) -> ParseResult<Self::Output> {
        let account = input.account.as_ref().ok_or(ParseError::Filtered)?;

        let owner = Pubkey::new_from_array(
            account
                .owner
                .clone()
                .try_into()
                .map_err(|_| ParseError::Filtered)?,
        );

        match owner {
            JUP_LIQUIDITY_PROGRAM_ID => try_parse_liquidity_state(input, &self.acc_parser)
                .await
                .map_err(Into::into),

            // Reserve vaults are plain SPL token accounts that belong to a liquidity reserve.
            SPL_TOKEN_PROGRAM_ID | SPL_TOKEN_2022_PROGRAM_ID => {
                try_parse_vault_account(input).map_err(Into::into)
            }
            _ => Err(ParseError::Filtered),
        }
    }
}
