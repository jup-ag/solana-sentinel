//! SPL Token mints: supply growth and authority changes, on shipstern's
//! token account parser. Both rules seed their caches from RPC at bootstrap.

pub mod mint_authority_change;
pub mod supply_increase;

use shipstern_spl_token_parser::AccountParser;
use solana_program_pack::Pack;
use solana_pubkey::Pubkey;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sentinel::utility::rpc::try_fetch_accounts_with_slot;
use solana_sentinel::{Program, ProgramSpec, StreamRules};
use spl_token_interface::state::Mint;
use std::collections::HashMap;

pub const PROGRAM: Program = Program::new("spl_token");

pub fn spec() -> ProgramSpec {
    ProgramSpec::new(PROGRAM).account(
        AccountParser,
        StreamRules::new()
            .register::<supply_increase::SupplyIncrease>()
            .register::<mint_authority_change::MintAuthorityChange>(),
    )
}

/// Current on-chain state of `mints`; ones that are missing or not mints are skipped.
pub async fn fetch_mints(
    rpc: &RpcClient,
    mints: &[Pubkey],
) -> anyhow::Result<HashMap<Pubkey, Mint>> {
    let (fetched, _slot) = try_fetch_accounts_with_slot(rpc, mints, None).await?;
    Ok(fetched
        .iter()
        .filter_map(|(pubkey, account)| Some((*pubkey, Mint::unpack(&account.data).ok()?)))
        .collect())
}
