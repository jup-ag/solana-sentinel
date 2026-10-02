//! An example program crate for the SPL Token program. A program is `PROGRAM`,
//! `spec()` listing one parser per feed with the rules registered on it, and one
//! file per rule under `rules/`. The framework does the rest: config validation,
//! bootstrap, subscription, alert routing.

pub mod rules;

use solana_account::Account;
use solana_program_pack::Pack;
use solana_pubkey::Pubkey;
use solana_sentinel::{PollCtx, PollRules, PollSource, Program, ProgramSpec, StreamRules};
use std::collections::HashMap;
use std::time::Duration;

/// What `rules.toml` calls this program in `program = "..."`.
pub const PROGRAM: Program = Program::new("spl_token");

/// Every feed the program exposes and every rule that can run on it. `rules.toml`
/// picks which rules are active; an unregistered rule name is a startup error.
pub fn spec() -> ProgramSpec {
    ProgramSpec::new(PROGRAM)
        // Stream feeds: parsed instructions and account updates from the data source.
        .instruction(
            shipstern_spl_token_parser::InstructionParser,
            StreamRules::new().register::<rules::large_transfer::LargeTransfer>(),
        )
        .account(
            shipstern_spl_token_parser::AccountParser,
            StreamRules::new().register::<rules::balance_drop::BalanceDrop>(),
        )
        // Poll feed: `getMultipleAccounts` every 60s unless a rule sets its own cadence.
        .poll(
            TokenAccountSource,
            Duration::from_secs(60),
            PollRules::new().register::<rules::low_balance::LowBalance>(),
        )
}

/// One fetched token account, as poll rules see it.
#[derive(Debug, Clone)]
pub struct TokenBalance {
    pub account: Pubkey,
    pub mint: Pubkey,
    pub amount: u64,
}

/// Turns one `getMultipleAccounts` response into items for the poll rules.
#[derive(Debug, Clone, Copy)]
pub struct TokenAccountSource;

impl PollSource for TokenAccountSource {
    type Item = TokenBalance;

    fn parse(&self, fetched: &HashMap<Pubkey, Account>, _ctx: &PollCtx) -> Vec<TokenBalance> {
        fetched
            .iter()
            .filter_map(|(pubkey, account)| {
                let state = spl_token_interface::state::Account::unpack(&account.data).ok()?;
                Some(TokenBalance {
                    account: *pubkey,
                    mint: state.mint,
                    amount: state.amount,
                })
            })
            .collect()
    }
}
