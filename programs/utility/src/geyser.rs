use shipstern_core::instruction::InstructionUpdate;
use shipstern_core::{AccountUpdate, TransactionUpdate};

fn default_tx_sig() -> String {
    "NOT_AVAILABLE".to_string()
}

pub fn get_tx_sig_from_acc_update(acc_update: &AccountUpdate) -> String {
    // tx sig should be available in the account update, but if not, return a random value ( should not happen in practice )
    acc_update
        .account
        .as_ref()
        .map_or_else(default_tx_sig, |a| {
            bs58::encode(a.txn_signature()).into_string()
        })
}

pub fn get_tx_sig_from_ix_update(ix_update: &InstructionUpdate) -> String {
    bs58::encode(&ix_update.shared.signature).into_string()
}

pub fn get_tx_sig_from_tx_update(tx_update: &TransactionUpdate) -> String {
    // tx sig should be available in the transaction update, but if not, return a random value ( should not happen in practice )
    tx_update
        .transaction
        .as_ref()
        .map_or_else(default_tx_sig, |t| bs58::encode(&t.signature).into_string())
}
