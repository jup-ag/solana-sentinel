use crate::traits::Broadcaster;
use anyhow::{bail, Context};
use async_trait::async_trait;
use solana_rpc_client::api::config::{
    CommitmentConfig, RpcSendTransactionConfig, RpcSimulateTransactionConfig,
};
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_transaction::versioned::VersionedTransaction;
use std::sync::Arc;

/// `sendTransaction` without preflight or RPC-side retries; Riposte owns retries.
pub struct RpcBroadcaster {
    id: String,
    rpc: RpcClient,
}

impl RpcBroadcaster {
    pub fn new(id: String, rpc: RpcClient) -> Self {
        Self { id, rpc }
    }
}

#[async_trait]
impl Broadcaster for RpcBroadcaster {
    fn id(&self) -> &str {
        &self.id
    }

    async fn send(&self, tx: &VersionedTransaction) -> anyhow::Result<()> {
        let config = RpcSendTransactionConfig {
            skip_preflight: true,
            max_retries: Some(0),
            ..Default::default()
        };
        self.rpc
            .send_transaction_with_config(tx, config)
            .await
            .context("sendTransaction failed")?;
        Ok(())
    }
}

/// Dry-run stand-in for the broadcasters: simulates with signature checks, never sends.
pub struct SimulateBroadcaster(Arc<RpcClient>);

impl SimulateBroadcaster {
    pub fn new(rpc: Arc<RpcClient>) -> Self {
        Self(rpc)
    }
}

#[async_trait]
impl Broadcaster for SimulateBroadcaster {
    fn id(&self) -> &str {
        "simulate"
    }

    async fn send(&self, tx: &VersionedTransaction) -> anyhow::Result<()> {
        let config = RpcSimulateTransactionConfig {
            sig_verify: true,
            // The blockhash cache reads `confirmed`; the client default (`finalized`) lacks it.
            commitment: Some(CommitmentConfig::confirmed()),
            ..Default::default()
        };
        let sim = self
            .0
            .simulate_transaction_with_config(tx, config)
            .await
            .context("simulateTransaction failed")?
            .value;
        if let Some(err) = sim.err {
            bail!("simulation failed: {err:?}, logs: {:?}", sim.logs);
        }
        tracing::info!(units = ?sim.units_consumed, "riposte dry-run simulation ok");
        Ok(())
    }
}
