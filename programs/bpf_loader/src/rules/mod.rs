pub mod program_upgrade;
pub mod program_upgrade_auth_change;

use crate::parser::{fetch_program_metadata, ProgramMetadata};
use serde::Deserialize;
use solana_pubkey::Pubkey;
use solana_sentinel::utility::pubkey::{derive_program_data_address, deserialize_pubkey_array};
use solana_sentinel::{BootstrapCtx, Prefilter, StateCache};
use std::collections::HashMap;

/// Both loader rules watch a list of programs by their ProgramData PDA.
#[derive(Debug, Deserialize)]
pub struct WatchedPrograms {
    #[serde(deserialize_with = "deserialize_pubkey_array")]
    pub programs: Vec<Pubkey>,
}

impl WatchedPrograms {
    pub fn prefilter(&self) -> Prefilter {
        Prefilter::new().accounts(self.programs.iter().map(derive_program_data_address))
    }

    /// ProgramData PDA → program id; ProgramData accounts do not contain the program id.
    pub fn program_data_to_id(&self) -> HashMap<Pubkey, Pubkey> {
        self.programs
            .iter()
            .map(|id| (derive_program_data_address(id), *id))
            .collect()
    }

    pub async fn seed(
        &self,
        ctx: &BootstrapCtx,
        cache: &StateCache<Pubkey, ProgramMetadata>,
    ) -> anyhow::Result<()> {
        cache.seed(fetch_program_metadata(&ctx.rpc, &self.programs).await?);
        Ok(())
    }
}
