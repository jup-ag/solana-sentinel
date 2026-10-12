//! Transaction rule: a watched wallet signed a transaction that invokes, at
//! any CPI depth, a program outside the allowlist. A compromised or phished
//! key usually shows up as a call into something it has never touched.

use crate::wallet::PROGRAM;
use serde::Deserialize;
use solana_pubkey::Pubkey;
use solana_sentinel::raw::RawTransaction;
use solana_sentinel::utility::pubkey::deserialize_pubkey_vec;
use solana_sentinel::utility::signers::signers_in_watchlist_shared;
use solana_sentinel::{Alert, Deps, RuleId, Severity, StreamCtx, StreamRule, Subscription};
use std::collections::HashSet;

/// Native and SPL programs a wallet touches in ordinary use.
pub const DEFAULT_ALLOWLIST: &[Pubkey] = &[
    Pubkey::from_str_const("11111111111111111111111111111111"), // System
    Pubkey::from_str_const("ComputeBudget111111111111111111111111111111"),
    Pubkey::from_str_const("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"), // SPL Token
    Pubkey::from_str_const("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"), // Token-2022
    Pubkey::from_str_const("ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL"), // Associated Token
    Pubkey::from_str_const("MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr"), // Memo
    Pubkey::from_str_const("Memo1UhkJRfHyvLMcVucJwxXeuD728EqVDDwQDxFMNo"), // Memo v1
    Pubkey::from_str_const("BPFLoaderUpgradeab1e11111111111111111111111"), // BPF Loader Upgradeable
    Pubkey::from_str_const("BPFLoader2111111111111111111111111111111111"), // BPF Loader v2
    Pubkey::from_str_const("BPFLoader1111111111111111111111111111111111"), // BPF Loader v1
    Pubkey::from_str_const("Stake11111111111111111111111111111111111111"), // Stake
    Pubkey::from_str_const("Vote111111111111111111111111111111111111111"), // Vote
    Pubkey::from_str_const("AddressLookupTab1e1111111111111111111111111"), // Address Lookup Table
    Pubkey::from_str_const("Ed25519SigVerify111111111111111111111111111"), // Ed25519 Signature Verification
    Pubkey::from_str_const("KeccakSecp256k11111111111111111111111111111"), // Keccak Secp256k1
];

fn default_true() -> bool {
    true
}

#[derive(Debug, Deserialize)]
pub struct UnapprovedProgramInvokeConfig {
    /// Signers to watch.
    #[serde(deserialize_with = "deserialize_pubkey_vec")]
    pub wallets: Vec<Pubkey>,
    /// Programs these wallets are expected to call, on top of the default allowlist.
    #[serde(default, deserialize_with = "deserialize_pubkey_vec")]
    pub approved_programs: Vec<Pubkey>,
    /// Set to false to allow only `approved_programs`.
    #[serde(default = "default_true")]
    pub include_default_allowlist: bool,
}

#[derive(Debug)]
pub struct UnapprovedProgramInvoke {
    wallets: Vec<Pubkey>,
    approved: HashSet<Pubkey>,
}

impl StreamRule for UnapprovedProgramInvoke {
    const ID: RuleId = RuleId::new(PROGRAM, "unapproved_program_invoke");
    type Config = UnapprovedProgramInvokeConfig;
    type Event = RawTransaction;

    fn subscription(cfg: &Self::Config) -> Subscription {
        Subscription::new().tx_accounts_include(cfg.wallets.iter().copied())
    }

    fn build(cfg: Self::Config, _deps: &Deps) -> anyhow::Result<Self> {
        anyhow::ensure!(!cfg.wallets.is_empty(), "wallets must not be empty");
        let mut approved: HashSet<Pubkey> = cfg.approved_programs.into_iter().collect();
        if cfg.include_default_allowlist {
            approved.extend(DEFAULT_ALLOWLIST);
        }
        Ok(Self {
            wallets: cfg.wallets,
            approved,
        })
    }

    fn evaluate(&self, tx: &RawTransaction, ctx: &StreamCtx) -> Option<Alert> {
        let signers = signers_in_watchlist_shared(&tx.shared, &self.wallets);
        if signers.is_empty() {
            return None;
        }
        let mut unapproved: Vec<Pubkey> = tx
            .ixs
            .iter()
            .map(|ix| ix.program_id)
            .filter(|program| !self.approved.contains(program))
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        if unapproved.is_empty() {
            return None;
        }
        // Sorted so the dedup key is identical for the same tx on every node.
        unapproved.sort_unstable();
        let programs = join(&unapproved);
        let wallets = join(&signers);
        Some(
            Alert::for_rule(Self::ID)
                .severity(Severity::Warn)
                .message(format!(
                    "Watched wallet(s) {wallets} signed a call into unapproved program(s) \
                     {programs} in {}",
                    ctx.tx_sig()
                ))
                .meta("wallets", &wallets)
                .meta("programs", &programs)
                .meta("slot", ctx.slot)
                .key([programs, wallets])
                .scope(ctx.tx_scope())
                .build(),
        )
    }
}

fn join(keys: &[Pubkey]) -> String {
    keys.iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(",")
}
