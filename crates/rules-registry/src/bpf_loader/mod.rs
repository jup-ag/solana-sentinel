//! Upgradeable BPF loader: watches a program's `ProgramData` account, which is
//! where its upgrade slot and upgrade authority live.

pub mod parser;
pub mod program_upgrade;
pub mod upgrade_authority_change;

use crate::bpf_loader::parser::{ProgramData, ProgramDataParser, PROGRAM_DATA_HEADER_LEN};
use serde::Deserialize;
use solana_account_decoder_client_types::UiAccountEncoding;
use solana_pubkey::Pubkey;
use solana_rpc_client::api::config::{RpcAccountInfoConfig, UiDataSliceConfig};
use solana_sentinel::utility::pubkey::deserialize_pubkey_vec;
use solana_sentinel::utility::rpc::try_fetch_accounts_with_slot;
use solana_sentinel::{BootstrapCtx, Program, ProgramSpec, StateCache, StreamRules, Subscription};
use std::collections::HashMap;
use tracing::warn;

pub const PROGRAM: Program = Program::new("bpf_loader");
pub const UPGRADEABLE_LOADER_ID: Pubkey =
    Pubkey::from_str_const("BPFLoaderUpgradeab1e11111111111111111111111");

pub fn spec() -> ProgramSpec {
    ProgramSpec::new(PROGRAM).account(
        ProgramDataParser,
        StreamRules::new()
            .register::<program_upgrade::ProgramUpgrade>()
            .register::<upgrade_authority_change::UpgradeAuthorityChange>(),
    )
}

/// The `ProgramData` PDA of an upgradeable program.
pub fn program_data_address(program_id: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(&[program_id.as_ref()], &UPGRADEABLE_LOADER_ID).0
}

/// Shared config of both loader rules: programs to watch, by program id.
#[derive(Debug, Deserialize)]
pub struct WatchedPrograms {
    #[serde(deserialize_with = "deserialize_pubkey_vec")]
    pub programs: Vec<Pubkey>,
}

impl WatchedPrograms {
    pub fn subscription(&self) -> Subscription {
        Subscription::new().accounts(self.programs.iter().map(program_data_address))
    }

    /// ProgramData address -> program id; the ProgramData account itself does not carry it.
    pub fn program_ids(&self) -> HashMap<Pubkey, Pubkey> {
        self.programs
            .iter()
            .map(|id| (program_data_address(id), *id))
            .collect()
    }

    /// Seed `cache` with the current header of every watched program, fetching
    /// only the metadata prefix and never the program binary.
    pub async fn seed(
        &self,
        ctx: &BootstrapCtx,
        cache: &StateCache<Pubkey, ProgramData>,
    ) -> anyhow::Result<()> {
        let addresses: Vec<Pubkey> = self.programs.iter().map(program_data_address).collect();
        let config = RpcAccountInfoConfig {
            encoding: Some(UiAccountEncoding::Base64),
            commitment: Some(ctx.rpc.commitment()),
            data_slice: Some(UiDataSliceConfig {
                offset: 0,
                length: PROGRAM_DATA_HEADER_LEN,
            }),
            min_context_slot: None,
        };
        let (fetched, _slot) =
            try_fetch_accounts_with_slot(&ctx.rpc, &addresses, Some(config)).await?;
        cache.seed(fetched.iter().filter_map(|(address, account)| {
            let Some(header) = parser::decode_header(&account.data) else {
                warn!(%address, "ProgramData account is not in ProgramData state; skipping");
                return None;
            };
            Some((*address, header.at(*address)))
        }));
        Ok(())
    }
}
