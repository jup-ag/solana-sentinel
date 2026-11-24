//! Extension points: signers and broadcasters ship with the crate and are picked by config.
//! What to send comes from the rule's `counteract`, queued with its alert.

use async_trait::async_trait;
use domain::alert::Alert;
use solana_message::VersionedMessage;
use solana_pubkey::Pubkey;
use solana_signature::Signature;
use solana_transaction::versioned::VersionedTransaction;

/// Signs a composed message. Async so a remote (quorum) signer fits behind it.
#[async_trait]
pub trait TxSigner: Send + Sync + 'static {
    fn pubkey(&self) -> Pubkey;
    async fn sign(&self, msg: &VersionedMessage, trigger: &Alert) -> anyhow::Result<Signature>;
}

/// Gets a signed transaction to the network. Returns once accepted, not landed.
#[async_trait]
pub trait Broadcaster: Send + Sync + 'static {
    fn id(&self) -> &str;
    async fn send(&self, tx: &VersionedTransaction) -> anyhow::Result<()>;
}
