use anyhow::Context;
use shipstern_core::{AccountUpdate, ParseError, ParseResult, Parser, Prefilter};
use solana_loader_v3_interface::state::UpgradeableLoaderState;
use solana_pubkey::Pubkey;
use solana_rpc_client::api::config::{RpcAccountInfoConfig, UiAccountEncoding, UiDataSliceConfig};
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sentinel::utility::constants::{
    BPF_LOADER_2_PROGRAM_ID, BPF_LOADER_PROGRAM_ID, BPF_LOADER_UPGRADEABLE_PROGRAM_ID,
};
use solana_sentinel::utility::pubkey::derive_program_data_address;
use solana_sentinel::utility::rpc::try_fetch_accounts_from_rpc;
use std::borrow::Cow;
use std::collections::HashMap;
use tracing::warn;
use wincode::Deserialize;

pub enum BPFLoaderProgram {
    V1, // non upgradeable BPF loader v1
    V2, // non upgradeable BPF loader v2
    V3, // upgradeable BPF loader (sometimes called "bpf_loader_upgradeable" or "loader-v3")
}

impl BPFLoaderProgram {
    pub fn from_program_id(program_id: &Pubkey) -> Option<Self> {
        if program_id == &BPF_LOADER_PROGRAM_ID {
            Some(Self::V1)
        } else if program_id == &BPF_LOADER_2_PROGRAM_ID {
            Some(Self::V2)
        } else if program_id == &BPF_LOADER_UPGRADEABLE_PROGRAM_ID {
            Some(Self::V3)
        } else {
            None
        }
    }
}

#[derive(Debug, Clone)]
pub struct ProgramMetadata {
    pub program_data_pubkey: Pubkey,
    pub last_upgrade_slot: u64,
    pub upgrade_authority_address: Option<Pubkey>,
}

impl ProgramMetadata {
    pub fn new(
        program_data_pubkey: Pubkey,
        last_upgrade_slot: u64,
        upgrade_authority_address: Option<Pubkey>,
    ) -> Self {
        Self {
            program_data_pubkey,
            last_upgrade_slot,
            upgrade_authority_address,
        }
    }
}

/// Fetch the ProgramData metadata for each program, keyed by ProgramData address.
/// Only the 45-byte metadata prefix is requested, never the program binary.
pub async fn fetch_program_metadata(
    rpc: &RpcClient,
    programs: &[Pubkey],
) -> anyhow::Result<HashMap<Pubkey, ProgramMetadata>> {
    let pairs: Vec<(Pubkey, Pubkey)> = programs
        .iter()
        .map(|program_id| (*program_id, derive_program_data_address(program_id)))
        .collect();
    let addresses: Vec<Pubkey> = pairs.iter().map(|(_, pda)| *pda).collect();

    let config = RpcAccountInfoConfig {
        commitment: Some(rpc.commitment()),
        encoding: Some(UiAccountEncoding::Base64),
        data_slice: Some(UiDataSliceConfig {
            offset: 0,
            length: UpgradeableLoaderState::size_of_programdata_metadata(),
        }),
        min_context_slot: None,
    };
    let accounts = try_fetch_accounts_from_rpc(rpc, &addresses, Some(config)).await?;

    let mut out = HashMap::with_capacity(pairs.len());
    for (program_id, program_data_address) in &pairs {
        let Some(raw) = accounts.get(program_data_address) else {
            warn!(%program_id, %program_data_address, "ProgramData account not found during bootstrap, skipping");
            continue;
        };
        let parsed = wincode::deserialize::<UpgradeableLoaderState>(&raw.data)
            .with_context(|| format!("failed to deserialize ProgramData for {program_id}"))?;
        let UpgradeableLoaderState::ProgramData {
            slot,
            upgrade_authority_address,
        } = parsed
        else {
            anyhow::bail!("unexpected loader state for {program_id} (expected ProgramData)");
        };
        out.insert(
            *program_data_address,
            ProgramMetadata::new(*program_data_address, slot, upgrade_authority_address),
        );
    }
    Ok(out)
}

#[derive(Debug, Clone)]
pub struct BPFLoaderParser;

impl Parser for BPFLoaderParser {
    type Input = AccountUpdate;
    type Output = ProgramMetadata;

    fn id(&self) -> Cow<'static, str> {
        Cow::Borrowed("bpf_loader_parser")
    }

    fn prefilter(&self) -> Prefilter {
        Prefilter::default()
    }

    async fn parse(&self, acc_update: &Self::Input) -> ParseResult<Self::Output> {
        let Some(acc) = acc_update.account.as_ref() else {
            return Err(ParseError::Other(
                "account update event missing account payload; skipping".into(),
            ));
        };

        let program_data_pubkey = Pubkey::try_from(acc.pubkey.as_slice())?;
        let program_owner = Pubkey::try_from(acc.owner.as_slice())?;

        // Only loader-v3 (BPF Upgradeable) has a ProgramData account / upgrade path.
        if !matches!(
            BPFLoaderProgram::from_program_id(&program_owner),
            Some(BPFLoaderProgram::V3)
        ) {
            return Err(ParseError::Other(
                format!(
                    "account owned by unsupported program {program_owner}, expected a BPF \
                     upgradeable loader; skipping"
                )
                .into(),
            ));
        }

        let parsed = UpgradeableLoaderState::deserialize(acc.data.as_slice())?;

        let UpgradeableLoaderState::ProgramData {
            slot,
            upgrade_authority_address,
        } = parsed
        else {
            return Err(ParseError::Other(
                format!(
                    "unexpected loader state for {} (expected ProgramData)",
                    program_data_pubkey
                )
                .into(),
            ));
        };

        Ok(ProgramMetadata::new(
            program_data_pubkey,
            slot,
            upgrade_authority_address,
        ))
    }
}
