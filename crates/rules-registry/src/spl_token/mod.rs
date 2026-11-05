//! SPL Token mints: supply growth and authority changes, on shipstern's
//! token account parser. Both rules seed their caches from RPC at bootstrap.

pub mod mint_authority_change;
pub mod supply_increase;

use shipstern_spl_token_parser::AccountParser;
use solana_program_pack::Pack;
use solana_pubkey::Pubkey;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sentinel::utility::rpc::try_fetch_account_chunks;
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

/// Current on-chain state of `mints`, per RPC chunk with the slot it was read at; ones that
/// are missing or not mints are skipped.
pub async fn fetch_mints(
    rpc: &RpcClient,
    mints: &[Pubkey],
) -> anyhow::Result<Vec<(u64, HashMap<Pubkey, Mint>)>> {
    let chunks = try_fetch_account_chunks(rpc, mints, None).await?;
    Ok(chunks
        .into_iter()
        .map(|(slot, fetched)| {
            let mints = fetched
                .iter()
                .filter_map(|(pubkey, account)| Some((*pubkey, Mint::unpack(&account.data).ok()?)))
                .collect();
            (slot, mints)
        })
        .collect())
}
