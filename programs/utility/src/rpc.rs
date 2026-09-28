use anyhow::{anyhow, Context};
use observability::prometheus::metrics::{
    record_rpc_fetch_status, record_rpc_latency, RpcFetchStatus,
};
use shipstern_core::{AccountUpdateInfo, KeyBytes};
use shipstern_spl_token_parser::Mint;
use solana_account::Account;
use solana_account_decoder_client_types::{UiAccount, UiAccountEncoding};
use solana_pubkey::Pubkey;
use solana_rpc_client::api::client_error::Result as ClientResult;
use solana_rpc_client::api::config::{CommitmentConfig, RpcAccountInfoConfig};
use solana_rpc_client::api::response::{Response, RpcResult};
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use spl_token_2022_interface::extension::StateWithExtensions;
use spl_token_2022_interface::state::Mint as RawMint;
use std::collections::HashMap;
use std::future::Future;
use std::str::FromStr;
use std::time::Duration;
use tokio::time::sleep;
use tracing::warn;

/// Max accounts per `getMultipleAccounts` call. Most RPCs cap at 100.
const BATCH_SIZE: usize = 100;
const MAX_RETRIES: u32 = 3;
const INITIAL_BACKOFF: Duration = Duration::from_millis(250);

pub async fn try_fetch_mint_from_rpc_with_config(
    rpc: &RpcClient,
    mint_pubkey: &Pubkey,
    config: Option<RpcAccountInfoConfig>,
) -> anyhow::Result<Option<Mint>> {
    let accounts = try_fetch_mints_from_rpc(rpc, &[*mint_pubkey], config).await?;
    Ok(accounts.get(mint_pubkey).cloned())
}

pub async fn try_fetch_mints_from_rpc(
    rpc: &RpcClient,
    mints: &[Pubkey],
    config: Option<RpcAccountInfoConfig>,
) -> anyhow::Result<HashMap<Pubkey, Mint>> {
    if mints.is_empty() {
        return Ok(HashMap::new());
    }

    let mut out = HashMap::with_capacity(mints.len());

    let accounts = try_fetch_accounts_from_rpc(rpc, mints, config).await?;
    for (pubkey, account) in accounts {
        // The Token-2022 unpacker handles both token programs: classic mints are
        // the base layout with no extension data.
        let Ok(raw_mint) = StateWithExtensions::<RawMint>::unpack(&account.data).map(|m| m.base)
        else {
            warn!(%pubkey, "failed to parse mint account data, skipping");
            continue;
        };

        let mint = Mint {
            mint_authority: raw_mint
                .mint_authority
                .map_or(None, |ma| Some(KeyBytes::new(ma.to_bytes()))),
            supply: raw_mint.supply,
            decimals: raw_mint.decimals.into(),
            is_initialized: raw_mint.is_initialized,
            freeze_authority: raw_mint
                .freeze_authority
                .map_or(None, |fa| Some(KeyBytes::new(fa.to_bytes()))),
        };

        out.insert(pubkey, mint);
    }

    Ok(out)
}

pub async fn try_fetch_accounts_from_rpc(
    rpc: &RpcClient,
    pubkeys: &[Pubkey],
    config: Option<RpcAccountInfoConfig>,
) -> anyhow::Result<HashMap<Pubkey, Account>> {
    let mut accounts = HashMap::with_capacity(pubkeys.len());

    for chunk in pubkeys.chunks(BATCH_SIZE) {
        let rpc_accounts = fetch_accounts_chunk_with_retry(rpc, chunk, &config).await?;
        collect_chunk(chunk, rpc_accounts, &mut accounts)?;
    }

    Ok(accounts)
}

fn collect_chunk(
    chunk: &[Pubkey],
    rpc_accounts: Vec<Option<UiAccount>>,
    accounts: &mut HashMap<Pubkey, Account>,
) -> anyhow::Result<()> {
    for (pubkey, maybe_account) in chunk.iter().zip(rpc_accounts) {
        let Some(account) = maybe_account else {
            warn!(%pubkey, "account not found on-chain, skipping");
            continue;
        };

        let acc_data = account
            .data
            .decode()
            .with_context(|| "failed to retrieve account data bytes")?;

        accounts.insert(
            *pubkey,
            Account {
                lamports: account.lamports,
                owner: Pubkey::from_str(&account.owner)
                    .with_context(|| "failed to parse owner pubkey")?,
                executable: account.executable,
                rent_epoch: account.rent_epoch,
                data: acc_data,
            },
        );
    }
    Ok(())
}

// Fetches a chunk of accounts from the RPC with retry logic.
async fn fetch_accounts_chunk_with_retry(
    rpc: &RpcClient,
    chunk: &[Pubkey],
    config: &Option<RpcAccountInfoConfig>,
) -> anyhow::Result<Vec<Option<UiAccount>>> {
    let config = config.clone().unwrap_or_else(|| RpcAccountInfoConfig {
        encoding: Some(UiAccountEncoding::Base64),
        commitment: Some(rpc.commitment()),
        ..Default::default()
    });

    rpc_req_with_retry("get_multiple_ui_accounts_with_config", || async {
        rpc.get_multiple_ui_accounts_with_config(chunk, config.clone())
            .await
    })
    .await
}

pub fn pubkey_from_account_update(update: &Option<AccountUpdateInfo>) -> Option<Pubkey> {
    let update = update.as_ref()?;
    Pubkey::try_from(update.pubkey.as_slice()).ok()
}

/// Like [`try_fetch_accounts_from_rpc`], also returning the highest response
/// context slot across chunks so pollers can stamp their evaluation.
pub async fn try_fetch_accounts_with_slot(
    rpc: &RpcClient,
    pubkeys: &[Pubkey],
    config: Option<RpcAccountInfoConfig>,
) -> anyhow::Result<(HashMap<Pubkey, Account>, u64)> {
    let mut accounts = HashMap::with_capacity(pubkeys.len());
    let mut slot = 0;

    for chunk in pubkeys.chunks(BATCH_SIZE) {
        let config = config.clone().unwrap_or_else(|| RpcAccountInfoConfig {
            encoding: Some(UiAccountEncoding::Base64),
            commitment: Some(rpc.commitment()),
            ..Default::default()
        });
        let response =
            rpc_req_with_retry_response("get_multiple_ui_accounts_with_config", || async {
                rpc.get_multiple_ui_accounts_with_config(chunk, config.clone())
                    .await
            })
            .await?;
        slot = slot.max(response.context.slot);
        collect_chunk(chunk, response.value, &mut accounts)?;
    }

    Ok((accounts, slot))
}

/// Latest processed slot, with the same retry and metrics as account fetches.
pub async fn try_fetch_latest_slot(rpc: &RpcClient) -> anyhow::Result<u64> {
    retry_with_backoff("get_slot", || async {
        rpc.get_slot_with_commitment(CommitmentConfig::processed())
            .await
    })
    .await
}

pub async fn rpc_req_with_retry<F, Fut, T>(method: &str, op: F) -> anyhow::Result<T>
where
    F: Fn() -> Fut,
    Fut: Future<Output = RpcResult<T>>,
{
    rpc_req_with_retry_response(method, op)
        .await
        .map(|response| response.value)
}

pub async fn rpc_req_with_retry_response<F, Fut, T>(
    method: &str,
    op: F,
) -> anyhow::Result<Response<T>>
where
    F: Fn() -> Fut,
    Fut: Future<Output = RpcResult<T>>,
{
    retry_with_backoff(method, op).await
}

async fn retry_with_backoff<F, Fut, T>(method: &str, op: F) -> anyhow::Result<T>
where
    F: Fn() -> Fut,
    Fut: Future<Output = ClientResult<T>>,
{
    let mut backoff = INITIAL_BACKOFF;
    let mut last_err = None;

    for attempt in 1..=MAX_RETRIES {
        let start = tokio::time::Instant::now();
        let result = op().await;
        let latency = start.elapsed();

        // record latency into the histogram regardless of success/failure
        record_rpc_latency(method, latency.as_secs_f64());

        match result {
            Ok(response) => {
                record_rpc_fetch_status(method, RpcFetchStatus::Success);
                return Ok(response);
            }
            Err(e) => {
                record_rpc_fetch_status(method, RpcFetchStatus::Failure);
                warn!(method, attempt, error = %e, "RPC request failed, retrying after {:?}", backoff);
                last_err = Some(e);
                sleep(backoff).await;
                backoff *= 2;
            }
        }
    }

    let error = last_err
        .as_ref()
        .map(|e| format!("{:?}", e))
        .unwrap_or_else(|| "unknown error".to_string());

    Err(anyhow!(error)).context(format!("exhausted retries for RPC method {method}"))
}
