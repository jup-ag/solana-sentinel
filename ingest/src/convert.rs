//! JSON-RPC shapes to the Yellowstone protobuf shapes the parsers consume.

use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use solana_account_decoder_client_types::UiAccount;
use solana_message::VersionedMessage;
use solana_pubkey::Pubkey;
use solana_transaction_status_client_types::option_serializer::OptionSerializer;
use solana_transaction_status_client_types::{
    EncodedTransactionWithStatusMeta, UiInnerInstructions, UiInstruction, UiTransactionStatusMeta,
    UiTransactionTokenBalance,
};
use std::str::FromStr;
use yellowstone_grpc_proto::geyser::{
    SubscribeUpdateAccount, SubscribeUpdateAccountInfo, SubscribeUpdateTransaction,
    SubscribeUpdateTransactionInfo,
};
use yellowstone_grpc_proto::solana::storage::confirmed_block::{
    CompiledInstruction, InnerInstruction, InnerInstructions, Message, MessageAddressTableLookup,
    MessageHeader, TokenBalance, Transaction, TransactionError, TransactionStatusMeta,
    UiTokenAmount,
};

pub fn account_update(
    pubkey: &Pubkey,
    account: &UiAccount,
    slot: u64,
) -> Option<SubscribeUpdateAccount> {
    Some(SubscribeUpdateAccount {
        account: Some(SubscribeUpdateAccountInfo {
            pubkey: pubkey.to_bytes().to_vec(),
            lamports: account.lamports,
            owner: Pubkey::from_str(&account.owner).ok()?.to_bytes().to_vec(),
            executable: account.executable,
            rent_epoch: account.rent_epoch,
            data: account.data.decode()?,
            write_version: 0,
            // PubSub account notifications do not carry the writing transaction.
            txn_signature: None,
        }),
        slot,
        is_startup: false,
    })
}

/// A transaction as fetched by `getTransaction` (base64 encoding), plus every
/// account key it touches for local filter matching.
pub struct ConvertedTransaction {
    pub update: SubscribeUpdateTransaction,
    pub account_keys: Vec<Pubkey>,
    pub failed: bool,
}

pub fn transaction_update(
    slot: u64,
    signature: &[u8],
    tx: &EncodedTransactionWithStatusMeta,
) -> Option<ConvertedTransaction> {
    let decoded = tx.transaction.decode()?;
    let meta = tx.meta.as_ref();
    let failed = meta.is_some_and(|m| m.err.is_some());

    let (header, static_keys, blockhash, instructions, versioned, lookups) = match &decoded.message
    {
        VersionedMessage::Legacy(m) => (
            m.header,
            m.account_keys.clone(),
            m.recent_blockhash,
            m.instructions.clone(),
            false,
            Vec::new(),
        ),
        VersionedMessage::V0(m) => (
            m.header,
            m.account_keys.clone(),
            m.recent_blockhash,
            m.instructions.clone(),
            true,
            m.address_table_lookups.clone(),
        ),
    };

    let (loaded_writable, loaded_readonly) = meta
        .map(|m| match &m.loaded_addresses {
            OptionSerializer::Some(l) => (pubkeys(&l.writable), pubkeys(&l.readonly)),
            _ => (Vec::new(), Vec::new()),
        })
        .unwrap_or_default();

    let account_keys: Vec<Pubkey> = static_keys
        .iter()
        .copied()
        .chain(loaded_writable.iter().copied())
        .chain(loaded_readonly.iter().copied())
        .collect();

    let message = Message {
        header: Some(MessageHeader {
            num_required_signatures: header.num_required_signatures.into(),
            num_readonly_signed_accounts: header.num_readonly_signed_accounts.into(),
            num_readonly_unsigned_accounts: header.num_readonly_unsigned_accounts.into(),
        }),
        account_keys: static_keys.iter().map(|k| k.to_bytes().to_vec()).collect(),
        recent_blockhash: blockhash.to_bytes().to_vec(),
        instructions: instructions
            .iter()
            .map(|ix| CompiledInstruction {
                program_id_index: ix.program_id_index.into(),
                accounts: ix.accounts.clone(),
                data: ix.data.clone(),
            })
            .collect(),
        versioned,
        address_table_lookups: lookups
            .iter()
            .map(|l| MessageAddressTableLookup {
                account_key: l.account_key.to_bytes().to_vec(),
                writable_indexes: l.writable_indexes.clone(),
                readonly_indexes: l.readonly_indexes.clone(),
            })
            .collect(),
        ..Default::default()
    };

    let status = meta.map(|m| status_meta(m, &loaded_writable, &loaded_readonly));

    Some(ConvertedTransaction {
        update: SubscribeUpdateTransaction {
            transaction: Some(SubscribeUpdateTransactionInfo {
                signature: signature.to_vec(),
                is_vote: false,
                transaction: Some(Transaction {
                    signatures: decoded
                        .signatures
                        .iter()
                        .map(|s| s.as_ref().to_vec())
                        .collect(),
                    message: Some(message),
                }),
                meta: status,
                index: 0,
            }),
            slot,
        },
        account_keys,
        failed,
    })
}

fn status_meta(
    m: &UiTransactionStatusMeta,
    writable: &[Pubkey],
    readonly: &[Pubkey],
) -> TransactionStatusMeta {
    let inner = match &m.inner_instructions {
        OptionSerializer::Some(v) => v.iter().map(inner_instructions).collect(),
        _ => Vec::new(),
    };
    let logs = match &m.log_messages {
        OptionSerializer::Some(v) => v.clone(),
        _ => Vec::new(),
    };
    TransactionStatusMeta {
        // The JSON error is not bincode-encodable here; presence is what rules check.
        err: m.err.as_ref().map(|_| TransactionError { err: Vec::new() }),
        fee: m.fee,
        pre_balances: m.pre_balances.clone(),
        post_balances: m.post_balances.clone(),
        inner_instructions_none: inner.is_empty(),
        inner_instructions: inner,
        log_messages_none: logs.is_empty(),
        log_messages: logs,
        pre_token_balances: token_balances(&m.pre_token_balances),
        post_token_balances: token_balances(&m.post_token_balances),
        loaded_writable_addresses: writable.iter().map(|k| k.to_bytes().to_vec()).collect(),
        loaded_readonly_addresses: readonly.iter().map(|k| k.to_bytes().to_vec()).collect(),
        compute_units_consumed: match m.compute_units_consumed {
            OptionSerializer::Some(v) => Some(v),
            _ => None,
        },
        ..Default::default()
    }
}

fn inner_instructions(ui: &UiInnerInstructions) -> InnerInstructions {
    InnerInstructions {
        index: ui.index.into(),
        instructions: ui
            .instructions
            .iter()
            .filter_map(|ix| match ix {
                UiInstruction::Compiled(c) => Some(InnerInstruction {
                    program_id_index: c.program_id_index.into(),
                    accounts: c.accounts.clone(),
                    data: bs58::decode(&c.data).into_vec().ok()?,
                    stack_height: c.stack_height,
                }),
                // Only produced by jsonParsed encoding, which the fetch never asks for.
                UiInstruction::Parsed(_) => None,
            })
            .collect(),
    }
}

fn token_balances(v: &OptionSerializer<Vec<UiTransactionTokenBalance>>) -> Vec<TokenBalance> {
    let OptionSerializer::Some(v) = v else {
        return Vec::new();
    };
    v.iter()
        .map(|b| TokenBalance {
            account_index: b.account_index.into(),
            mint: b.mint.clone(),
            ui_token_amount: Some(UiTokenAmount {
                ui_amount: b.ui_token_amount.ui_amount.unwrap_or_default(),
                decimals: b.ui_token_amount.decimals.into(),
                amount: b.ui_token_amount.amount.clone(),
                ui_amount_string: b.ui_token_amount.ui_amount_string.clone(),
            }),
            owner: match &b.owner {
                OptionSerializer::Some(o) => o.clone(),
                _ => String::new(),
            },
            program_id: match &b.program_id {
                OptionSerializer::Some(p) => p.clone(),
                _ => String::new(),
            },
        })
        .collect()
}

fn pubkeys(strs: &[String]) -> Vec<Pubkey> {
    strs.iter()
        .filter_map(|s| Pubkey::from_str(s).ok())
        .collect()
}

/// Base64 data field as the PubSub `accountSubscribe` returns it; kept for callers that decode by hand.
pub fn decode_base64(data: &str) -> Option<Vec<u8>> {
    STANDARD.decode(data).ok()
}
