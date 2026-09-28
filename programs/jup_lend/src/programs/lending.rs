#![allow(clippy::derive_partial_eq_without_eq)]

use shipstern_core::instruction::InstructionUpdate;
use shipstern_core::Parser;
use shipstern_parser::prelude::{ParseResult, Prefilter};
use shipstern_proc_macro::include_shipstern_parser;
use solana_pubkey::Pubkey;
use spl_token_2022_interface::instruction::TokenInstruction;
use std::borrow::Cow;
use tracing::warn;
use utility::geyser::get_tx_sig_from_ix_update;

include_shipstern_parser!("idls/lending-idl.codama.json");

pub const JUP_LENDING_PROGRAM_ID: Pubkey = Pubkey::new_from_array(jup_lend_lending::PROGRAM_ID);
pub type LendingAccountParser = jup_lend_lending::AccountParser;
pub type LendingState = jup_lend_lending::account::Account;

pub type DepositIx = jup_lend_lending::instruction::Deposit;
pub type DepositWithMinAmountOutIx = jup_lend_lending::instruction::DepositWithMinAmountOut;
pub type WithdrawIx = jup_lend_lending::instruction::Withdraw;
pub type WithdrawWithMaxSharesBurnIx = jup_lend_lending::instruction::WithdrawWithMaxSharesBurn;
pub type RedeemIx = jup_lend_lending::instruction::Redeem;
pub type RedeemWithMinAmountOutIx = jup_lend_lending::instruction::RedeemWithMinAmountOut;
pub type LendingIxParser = jup_lend_lending::InstructionParser;
pub type LendingIx = jup_lend_lending::instruction::Instruction;

/// Passing `u64::MAX` as the amount/shares of a withdraw-side instruction tells the lending
/// program to exit the signer's entire position. The literal argument is meaningless as a
/// quantity, so the real movement has to be read from the fToken burn the program performs.
pub const WITHDRAW_ALL_SENTINEL: u64 = u64::MAX;

// A fund-movement instruction on the lending program (deposit/withdraw/redeem), paired with the transaction's success.
#[derive(Debug)]
pub enum LendingMovementIx {
    Deposit {
        ix: DepositIx,
        is_success: bool,
        tx_sig: String,
    },
    DepositWithMinAmountOut {
        ix: DepositWithMinAmountOutIx,
        is_success: bool,
        tx_sig: String,
    },
    Withdraw {
        ix: WithdrawIx,
        is_success: bool,
        tx_sig: String,
        burned_shares: Option<u64>,
    },
    WithdrawWithMaxSharesBurn {
        ix: WithdrawWithMaxSharesBurnIx,
        is_success: bool,
        tx_sig: String,
        burned_shares: Option<u64>,
    },
    Redeem {
        ix: RedeemIx,
        is_success: bool,
        tx_sig: String,
        /// fTokens burned by the inner token-program CPI; resolves a [`WITHDRAW_ALL_SENTINEL`] argument.
        burned_shares: Option<u64>,
    },
    RedeemWithMinAmountOut {
        ix: RedeemWithMinAmountOutIx,
        is_success: bool,
        tx_sig: String,
        /// fTokens burned by the inner token-program CPI; resolves a [`WITHDRAW_ALL_SENTINEL`] argument.
        burned_shares: Option<u64>,
    },
    Noop, // Noop variant for instructions that don't move funds
}

/// An instruction amount tagged with its denomination; withdrawals can be in assets (`withdraw`) or fTokens (`redeem`).
pub enum VaultAmount {
    /// Denominated in the vault's underlying token.
    Assets(u64),
    /// Denominated in vault shares (fTokens); convert via the exchange price.
    Shares(u64),
}

/// Whether an instruction moves funds into or out of the vault.
pub enum MovementDirection {
    Deposit,
    Withdraw,
}

impl LendingMovementIx {
    pub fn extract_signer_pubkey(&self) -> Option<Pubkey> {
        match self {
            Self::Deposit { ix, .. } => Some(Pubkey::new_from_array(ix.accounts.signer.0)),
            Self::DepositWithMinAmountOut { ix, .. } => {
                Some(Pubkey::new_from_array(ix.accounts.signer.0))
            }
            Self::Withdraw { ix, .. } => Some(Pubkey::new_from_array(ix.accounts.signer.0)),
            Self::WithdrawWithMaxSharesBurn { ix, .. } => {
                Some(Pubkey::new_from_array(ix.accounts.signer.0))
            }
            Self::Redeem { ix, .. } => Some(Pubkey::new_from_array(ix.accounts.signer.0)),
            Self::RedeemWithMinAmountOut { ix, .. } => {
                Some(Pubkey::new_from_array(ix.accounts.signer.0))
            }
            Self::Noop => None,
        }
    }
    pub fn extract_vault_amount(&self) -> Option<VaultAmount> {
        match self {
            Self::Deposit { ix, .. } => Some(VaultAmount::Assets(ix.args.assets)),
            Self::DepositWithMinAmountOut { ix, .. } => Some(VaultAmount::Assets(ix.args.assets)),
            Self::Withdraw {
                ix, burned_shares, ..
            } => Self::withdraw_amount(ix.args.amount, VaultAmount::Assets, *burned_shares),
            // `amount` is asset-denominated; `max_shares_burn` is only a slippage cap on the fTokens burned.
            Self::WithdrawWithMaxSharesBurn {
                ix, burned_shares, ..
            } => Self::withdraw_amount(ix.args.amount, VaultAmount::Assets, *burned_shares),
            Self::Redeem {
                ix, burned_shares, ..
            } => Self::withdraw_amount(ix.args.shares, VaultAmount::Shares, *burned_shares),
            Self::RedeemWithMinAmountOut {
                ix, burned_shares, ..
            } => Self::withdraw_amount(ix.args.shares, VaultAmount::Shares, *burned_shares),
            Self::Noop => None,
        }
    }

    /// A withdraw-side argument is the movement itself unless it is the withdraw-all sentinel,
    /// in which case the movement is whatever the program actually burned.
    fn withdraw_amount(
        arg: u64,
        denomination: fn(u64) -> VaultAmount,
        burned_shares: Option<u64>,
    ) -> Option<VaultAmount> {
        if arg == WITHDRAW_ALL_SENTINEL {
            burned_shares.map(VaultAmount::Shares)
        } else {
            Some(denomination(arg))
        }
    }

    pub fn extract_direction(&self) -> Option<MovementDirection> {
        match self {
            Self::Deposit { .. } | Self::DepositWithMinAmountOut { .. } => {
                Some(MovementDirection::Deposit)
            }
            Self::Withdraw { .. }
            | Self::WithdrawWithMaxSharesBurn { .. }
            | Self::Redeem { .. }
            | Self::RedeemWithMinAmountOut { .. } => Some(MovementDirection::Withdraw),
            Self::Noop => None,
        }
    }

    pub fn extract_token_reserve_pubkey(&self) -> Option<Pubkey> {
        let reserve = match self {
            Self::Deposit { ix, .. } => Some(ix.accounts.supply_token_reserves_liquidity),
            Self::DepositWithMinAmountOut { ix, .. } => {
                Some(ix.accounts.supply_token_reserves_liquidity)
            }
            Self::Withdraw { ix, .. } => Some(ix.accounts.supply_token_reserves_liquidity),
            Self::WithdrawWithMaxSharesBurn { ix, .. } => {
                Some(ix.accounts.supply_token_reserves_liquidity)
            }
            Self::Redeem { ix, .. } => Some(ix.accounts.supply_token_reserves_liquidity),
            Self::RedeemWithMinAmountOut { ix, .. } => {
                Some(ix.accounts.supply_token_reserves_liquidity)
            }
            Self::Noop => None,
        };

        reserve.map(|r| Pubkey::new_from_array(r.0))
    }

    pub fn extract_tx_sig(&self) -> Option<&str> {
        match self {
            Self::Deposit { tx_sig, .. }
            | Self::DepositWithMinAmountOut { tx_sig, .. }
            | Self::Withdraw { tx_sig, .. }
            | Self::WithdrawWithMaxSharesBurn { tx_sig, .. }
            | Self::Redeem { tx_sig, .. }
            | Self::RedeemWithMinAmountOut { tx_sig, .. } => Some(tx_sig),
            Self::Noop => None,
        }
    }

    pub fn is_success(&self) -> Option<bool> {
        match self {
            Self::Deposit { is_success, .. }
            | Self::DepositWithMinAmountOut { is_success, .. }
            | Self::Withdraw { is_success, .. }
            | Self::WithdrawWithMaxSharesBurn { is_success, .. }
            | Self::Redeem { is_success, .. }
            | Self::RedeemWithMinAmountOut { is_success, .. } => Some(*is_success),
            Self::Noop => None,
        }
    }
}

/// For a withdraw-all instruction, the fTokens burned by the lending program's inner
/// token-program CPI. `None` when the argument is not the sentinel, or when no burn can be
/// found (e.g. the transaction failed before the CPI ran).
fn burned_shares_for_withdraw_all(
    update: &InstructionUpdate,
    arg: u64,
    token_program: &shipstern_core::Pubkey,
    f_token_mint: &shipstern_core::Pubkey,
    tx_sig: &str,
) -> Option<u64> {
    if arg != WITHDRAW_ALL_SENTINEL {
        return None;
    }

    let burned = find_burn_amount(&update.inner, token_program, f_token_mint);

    if burned.is_none() && update.shared.err.is_none() {
        warn!(
            tx_sig,
            "withdraw-all sentinel used but no fToken burn found in inner instructions"
        );
    }

    burned
}

/// Depth-first search of `ixs` for a token-program `Burn`/`BurnChecked` of `mint`, returning its amount.
fn find_burn_amount(
    ixs: &[InstructionUpdate],
    token_program: &shipstern_core::Pubkey,
    mint: &shipstern_core::Pubkey,
) -> Option<u64> {
    ixs.iter().find_map(|ix| {
        // SPL Token `Burn`/`BurnChecked` accounts: [source, mint, authority].
        let is_burn_of_mint = ix.program == *token_program && ix.accounts.get(1) == Some(mint);

        let own = if is_burn_of_mint {
            match TokenInstruction::unpack(&ix.data) {
                Ok(TokenInstruction::Burn { amount })
                | Ok(TokenInstruction::BurnChecked { amount, .. }) => Some(amount),
                _ => None,
            }
        } else {
            None
        };

        own.or_else(|| find_burn_amount(&ix.inner, token_program, mint))
    })
}

pub async fn try_parse_lending_ix(
    update: &InstructionUpdate,
    parser: &LendingIxParser,
) -> anyhow::Result<LendingMovementIx> {
    let program_id = Pubkey::new_from_array(update.program.0);

    if program_id != JUP_LENDING_PROGRAM_ID {
        return Ok(LendingMovementIx::Noop);
    }

    let ix = parser
        .parse(update)
        .await
        .map_err(|e| {
            anyhow::anyhow!(
                "failed to parse instruction update for lending program :{:?}",
                e
            )
        })?
        .instruction;

    let is_success = update.shared.err.is_none();
    let tx_sig = get_tx_sig_from_ix_update(update);

    let ix = match ix {
        LendingIx::Deposit { accounts, args } => LendingMovementIx::Deposit {
            ix: DepositIx { accounts, args },
            is_success,
            tx_sig,
        },
        LendingIx::DepositWithMinAmountOut { accounts, args } => {
            LendingMovementIx::DepositWithMinAmountOut {
                ix: DepositWithMinAmountOutIx { accounts, args },
                is_success,
                tx_sig,
            }
        }
        LendingIx::Withdraw { accounts, args } => {
            let burned_shares = burned_shares_for_withdraw_all(
                update,
                args.amount,
                &accounts.token_program,
                &accounts.f_token_mint,
                &tx_sig,
            );
            LendingMovementIx::Withdraw {
                ix: WithdrawIx { accounts, args },
                is_success,
                tx_sig,
                burned_shares,
            }
        }
        LendingIx::WithdrawWithMaxSharesBurn { accounts, args } => {
            let burned_shares = burned_shares_for_withdraw_all(
                update,
                args.amount,
                &accounts.token_program,
                &accounts.f_token_mint,
                &tx_sig,
            );
            LendingMovementIx::WithdrawWithMaxSharesBurn {
                ix: WithdrawWithMaxSharesBurnIx { accounts, args },
                is_success,
                tx_sig,
                burned_shares,
            }
        }
        LendingIx::Redeem { accounts, args } => {
            let burned_shares = burned_shares_for_withdraw_all(
                update,
                args.shares,
                &accounts.token_program,
                &accounts.f_token_mint,
                &tx_sig,
            );
            LendingMovementIx::Redeem {
                ix: RedeemIx { accounts, args },
                is_success,
                tx_sig,
                burned_shares,
            }
        }
        LendingIx::RedeemWithMinAmountOut { accounts, args } => {
            let burned_shares = burned_shares_for_withdraw_all(
                update,
                args.shares,
                &accounts.token_program,
                &accounts.f_token_mint,
                &tx_sig,
            );
            LendingMovementIx::RedeemWithMinAmountOut {
                ix: RedeemWithMinAmountOutIx { accounts, args },
                is_success,
                tx_sig,
                burned_shares,
            }
        }
        _ => LendingMovementIx::Noop,
    };

    Ok(ix)
}

// Parses lending-program instruction updates; for now only the whale-movement instructions.
#[derive(Debug, Clone)]
pub struct LendingProgramIxParser {
    ix_parser: LendingIxParser,
}

impl Default for LendingProgramIxParser {
    fn default() -> Self {
        Self::new()
    }
}

impl LendingProgramIxParser {
    pub fn new() -> Self {
        Self {
            ix_parser: LendingIxParser {},
        }
    }
}

impl Parser for LendingProgramIxParser {
    type Input = InstructionUpdate;

    type Output = LendingMovementIx;

    fn id(&self) -> Cow<'static, str> {
        Cow::Borrowed("jup_lend_lending_ix_parser")
    }

    fn prefilter(&self) -> Prefilter {
        Prefilter::default()
    }

    async fn parse(&self, input: &Self::Input) -> ParseResult<Self::Output> {
        try_parse_lending_ix(input, &self.ix_parser)
            .await
            .map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use shipstern_core::instruction::{InstructionShared, Path};
    use shipstern_core::{KeyBytes, Pubkey as VixenPubkey};
    use std::sync::Arc;

    fn key(byte: u8) -> VixenPubkey {
        KeyBytes([byte; 32])
    }

    fn ix(
        program: VixenPubkey,
        accounts: Vec<VixenPubkey>,
        data: Vec<u8>,
        inner: Vec<InstructionUpdate>,
    ) -> InstructionUpdate {
        InstructionUpdate {
            program,
            accounts,
            data,
            shared: Arc::new(InstructionShared::default()),
            inner,
            path: Path::new_single(0),
            log_range: 0..0,
        }
    }

    const TOKEN_PROGRAM: u8 = 1;
    const F_TOKEN_MINT: u8 = 2;
    const UNDERLYING_MINT: u8 = 3;

    fn burn(mint: u8, amount: u64) -> InstructionUpdate {
        ix(
            key(TOKEN_PROGRAM),
            vec![key(9), key(mint), key(8)],
            TokenInstruction::Burn { amount }.pack(),
            vec![],
        )
    }

    #[test]
    fn finds_ftoken_burn_among_siblings() {
        let transfer = ix(
            key(TOKEN_PROGRAM),
            vec![key(9), key(7), key(8)],
            TokenInstruction::TransferChecked {
                amount: 5,
                decimals: 9,
            }
            .pack(),
            vec![],
        );
        let inner = vec![transfer, burn(F_TOKEN_MINT, 1_234)];

        assert_eq!(
            find_burn_amount(&inner, &key(TOKEN_PROGRAM), &key(F_TOKEN_MINT)),
            Some(1_234)
        );
    }

    #[test]
    fn finds_nested_burn_checked() {
        let nested = ix(
            key(TOKEN_PROGRAM),
            vec![key(9), key(F_TOKEN_MINT), key(8)],
            TokenInstruction::BurnChecked {
                amount: 77,
                decimals: 9,
            }
            .pack(),
            vec![],
        );
        let liquidity_cpi = ix(key(5), vec![], vec![], vec![nested]);

        assert_eq!(
            find_burn_amount(&[liquidity_cpi], &key(TOKEN_PROGRAM), &key(F_TOKEN_MINT)),
            Some(77)
        );
    }

    #[test]
    fn ignores_burns_of_other_mints_and_other_programs() {
        let wrong_mint = burn(UNDERLYING_MINT, 10);
        let mut wrong_program = burn(F_TOKEN_MINT, 20);
        wrong_program.program = key(6);

        assert_eq!(
            find_burn_amount(
                &[wrong_mint, wrong_program],
                &key(TOKEN_PROGRAM),
                &key(F_TOKEN_MINT)
            ),
            None
        );
    }

    #[test]
    fn sentinel_resolves_to_burned_shares() {
        let update = ix(key(4), vec![], vec![], vec![burn(F_TOKEN_MINT, 500)]);

        assert_eq!(
            burned_shares_for_withdraw_all(
                &update,
                WITHDRAW_ALL_SENTINEL,
                &key(TOKEN_PROGRAM),
                &key(F_TOKEN_MINT),
                "sig",
            ),
            Some(500)
        );
        // A literal amount is never resolved through the burn.
        assert_eq!(
            burned_shares_for_withdraw_all(
                &update,
                500,
                &key(TOKEN_PROGRAM),
                &key(F_TOKEN_MINT),
                "sig"
            ),
            None
        );
    }

    #[test]
    fn withdraw_amount_prefers_literal_and_falls_back_to_shares() {
        assert!(matches!(
            LendingMovementIx::withdraw_amount(42, VaultAmount::Assets, Some(7)),
            Some(VaultAmount::Assets(42))
        ));
        assert!(matches!(
            LendingMovementIx::withdraw_amount(WITHDRAW_ALL_SENTINEL, VaultAmount::Assets, Some(7)),
            Some(VaultAmount::Shares(7))
        ));
        assert!(LendingMovementIx::withdraw_amount(
            WITHDRAW_ALL_SENTINEL,
            VaultAmount::Assets,
            None
        )
        .is_none());
    }
}
