//! Account post-states rebuilt from recorded transactions.
//!
//! The RPC serves no historical account state, so the stream's account updates
//! are rebuilt from what each transaction's metadata says about the accounts it
//! wrote:
//! - **SPL token / Token-2022 accounts** (any account in the token balances):
//!   mint, owner, amount and lamports are exact. The rest of the data (delegate,
//!   close authority, Token-2022 extensions) comes from the recording-time
//!   snapshot when it holds the same mint, else a plain 165-byte account is
//!   synthesised. A token account that disappears from the post balances is
//!   emitted closed (0 lamports, no data, system owner).
//! - **System-owned wallets** (snapshot owned by the system program with no data):
//!   lamports are exact.
//! - **Anything else** (program state: vaults, obligations, oracles, configs) cannot
//!   be rebuilt: the metadata has no account data. Those writes are counted in
//!   [`ReconstructStats::skipped_program_accounts`] and not emitted; account rules on
//!   them see nothing during a replay.

use solana_account_decoder_client_types::UiAccount;
use solana_pubkey::Pubkey;
use std::collections::HashMap;
use std::str::FromStr;
use yellowstone_grpc_proto::geyser::{SubscribeUpdateAccount, SubscribeUpdateAccountInfo};
use yellowstone_grpc_proto::solana::storage::confirmed_block::{
    TokenBalance, TransactionStatusMeta,
};

pub const SYSTEM_PROGRAM: Pubkey = Pubkey::from_str_const("11111111111111111111111111111111");
pub const TOKEN_PROGRAM: Pubkey =
    Pubkey::from_str_const("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA");
const NATIVE_MINT: Pubkey = Pubkey::from_str_const("So11111111111111111111111111111111111111112");
const TOKEN_ACCOUNT_LEN: usize = 165;
/// Rent-exempt reserve of a 165-byte account, the `is_native` amount of a wSOL account.
const TOKEN_ACCOUNT_RENT: u64 = 2_039_280;

#[derive(Debug, Clone, PartialEq, Eq)]
struct AccountState {
    lamports: u64,
    owner: Pubkey,
    data: Vec<u8>,
    executable: bool,
    rent_epoch: u64,
}

impl AccountState {
    fn from_ui(a: &UiAccount) -> Option<Self> {
        Some(Self {
            lamports: a.lamports,
            owner: Pubkey::from_str(&a.owner).ok()?,
            data: a.data.decode()?,
            executable: a.executable,
            rent_epoch: a.rent_epoch,
        })
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ReconstructStats {
    pub token_accounts: u64,
    pub token_from_snapshot: u64,
    pub token_synthesised: u64,
    pub token_closed: u64,
    pub wallets: u64,
    /// Writable accounts whose post-state cannot be rebuilt (program-owned data, or no snapshot).
    pub skipped_program_accounts: u64,
}

/// One transaction's view for the rebuild: its keys in index order (static, loaded
/// writable, loaded readonly) and which of them it could write.
pub struct TxAccounts<'a> {
    pub keys: &'a [Pubkey],
    pub writable: &'a [bool],
    pub meta: &'a TransactionStatusMeta,
    pub signature: &'a [u8],
    pub slot: u64,
}

pub struct Reconstructor {
    snapshots: HashMap<Pubkey, AccountState>,
    write_version: u64,
    pub stats: ReconstructStats,
}

impl Reconstructor {
    pub fn new(snapshots: &HashMap<Pubkey, UiAccount>) -> Self {
        Self {
            snapshots: snapshots
                .iter()
                .filter_map(|(k, a)| Some((*k, AccountState::from_ui(a)?)))
                .collect(),
            write_version: 0,
            stats: ReconstructStats::default(),
        }
    }

    /// Pre-states of the transaction's rebuildable accounts, for seeding rules with the
    /// window's opening state. `seen` skips accounts already seeded.
    pub fn pre_states(
        &mut self,
        tx: &TxAccounts<'_>,
        seen: &mut std::collections::HashSet<Pubkey>,
    ) -> Vec<SubscribeUpdateAccount> {
        let mut out = Vec::new();
        for (i, key) in tx.keys.iter().enumerate() {
            if !tx.writable.get(i).copied().unwrap_or(false) || seen.contains(key) {
                continue;
            }
            let lamports = tx.meta.pre_balances.get(i).copied().unwrap_or_default();
            let token = find_balance(&tx.meta.pre_token_balances, i);
            let state = match token {
                Some(b) => self.token_state(key, b, lamports),
                None => self.wallet_state(key, lamports),
            };
            if let Some(state) = state {
                seen.insert(*key);
                out.push(self.update(key, state, tx.slot, None));
            }
        }
        out
    }

    /// Post-states of every rebuildable account the transaction wrote, in key order.
    pub fn post_states(&mut self, tx: &TxAccounts<'_>) -> Vec<SubscribeUpdateAccount> {
        let mut out = Vec::new();
        for (i, key) in tx.keys.iter().enumerate() {
            if !tx.writable.get(i).copied().unwrap_or(false) {
                continue;
            }
            let lamports = tx.meta.post_balances.get(i).copied().unwrap_or_default();
            let post = find_balance(&tx.meta.post_token_balances, i);
            let pre = find_balance(&tx.meta.pre_token_balances, i);
            let state = match (pre, post) {
                (_, Some(b)) => {
                    self.stats.token_accounts += 1;
                    self.token_state(key, b, lamports)
                }
                (Some(_), None) => {
                    self.stats.token_closed += 1;
                    Some(AccountState {
                        lamports,
                        owner: SYSTEM_PROGRAM,
                        data: Vec::new(),
                        executable: false,
                        rent_epoch: 0,
                    })
                }
                (None, None) => {
                    let wallet = self.wallet_state(key, lamports);
                    if wallet.is_some() {
                        self.stats.wallets += 1;
                    } else if tx.meta.pre_balances.get(i) != tx.meta.post_balances.get(i)
                        || !self.snapshots.contains_key(key)
                    {
                        self.stats.skipped_program_accounts += 1;
                    }
                    wallet
                }
            };
            if let Some(state) = state {
                out.push(self.update(key, state, tx.slot, Some(tx.signature)));
            }
        }
        out
    }

    fn token_state(
        &mut self,
        key: &Pubkey,
        b: &TokenBalance,
        lamports: u64,
    ) -> Option<AccountState> {
        let mint = Pubkey::from_str(&b.mint).ok()?;
        let owner = Pubkey::from_str(&b.owner).ok()?;
        let program = Pubkey::from_str(&b.program_id).unwrap_or(TOKEN_PROGRAM);
        let amount: u64 = b.ui_token_amount.as_ref()?.amount.parse().ok()?;
        let template = self
            .snapshots
            .get(key)
            .filter(|s| s.data.len() >= TOKEN_ACCOUNT_LEN && s.data[..32] == mint.to_bytes());
        let (data, rent_epoch) = match template {
            Some(s) => {
                self.stats.token_from_snapshot += 1;
                let mut d = s.data.clone();
                d[32..64].copy_from_slice(&owner.to_bytes());
                d[64..72].copy_from_slice(&amount.to_le_bytes());
                (d, s.rent_epoch)
            }
            None => {
                self.stats.token_synthesised += 1;
                (token_account_data(&mint, &owner, amount), u64::MAX)
            }
        };
        Some(AccountState {
            lamports,
            owner: program,
            data,
            executable: false,
            rent_epoch,
        })
    }

    fn wallet_state(&self, key: &Pubkey, lamports: u64) -> Option<AccountState> {
        let snap = self.snapshots.get(key)?;
        (snap.owner == SYSTEM_PROGRAM && snap.data.is_empty()).then(|| AccountState {
            lamports,
            ..snap.clone()
        })
    }

    fn update(
        &mut self,
        key: &Pubkey,
        state: AccountState,
        slot: u64,
        signature: Option<&[u8]>,
    ) -> SubscribeUpdateAccount {
        self.write_version += 1;
        SubscribeUpdateAccount {
            account: Some(SubscribeUpdateAccountInfo {
                pubkey: key.to_bytes().to_vec(),
                lamports: state.lamports,
                owner: state.owner.to_bytes().to_vec(),
                executable: state.executable,
                rent_epoch: state.rent_epoch,
                data: state.data,
                write_version: self.write_version,
                txn_signature: signature.map(<[u8]>::to_vec),
            }),
            slot,
            is_startup: false,
        }
    }
}

fn find_balance(balances: &[TokenBalance], index: usize) -> Option<&TokenBalance> {
    balances.iter().find(|b| b.account_index as usize == index)
}

/// A plain initialized SPL token account: no delegate, no close authority.
pub fn token_account_data(mint: &Pubkey, owner: &Pubkey, amount: u64) -> Vec<u8> {
    let mut d = vec![0u8; TOKEN_ACCOUNT_LEN];
    d[..32].copy_from_slice(&mint.to_bytes());
    d[32..64].copy_from_slice(&owner.to_bytes());
    d[64..72].copy_from_slice(&amount.to_le_bytes());
    d[108] = 1; // AccountState::Initialized
    if *mint == NATIVE_MINT {
        d[109] = 1; // is_native: COption::Some(rent reserve)
        d[113..121].copy_from_slice(&TOKEN_ACCOUNT_RENT.to_le_bytes());
    }
    d
}

/// Which keys (static, then loaded writable, then loaded readonly) a message may write.
pub fn writable_flags(
    num_required_signatures: u32,
    num_readonly_signed: u32,
    num_readonly_unsigned: u32,
    static_len: usize,
    loaded_writable: usize,
    loaded_readonly: usize,
) -> Vec<bool> {
    let signed = num_required_signatures as usize;
    let ro_signed = num_readonly_signed as usize;
    let ro_unsigned = num_readonly_unsigned as usize;
    let mut out = Vec::with_capacity(static_len + loaded_writable + loaded_readonly);
    for i in 0..static_len {
        out.push(if i < signed {
            i < signed.saturating_sub(ro_signed)
        } else {
            i < static_len.saturating_sub(ro_unsigned)
        });
    }
    out.extend(std::iter::repeat_n(true, loaded_writable));
    out.extend(std::iter::repeat_n(false, loaded_readonly));
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_account_decoder_client_types::{UiAccountData, UiAccountEncoding};
    use yellowstone_grpc_proto::solana::storage::confirmed_block::UiTokenAmount;

    fn balance(index: u32, mint: &Pubkey, owner: &Pubkey, amount: u64) -> TokenBalance {
        TokenBalance {
            account_index: index,
            mint: mint.to_string(),
            owner: owner.to_string(),
            program_id: TOKEN_PROGRAM.to_string(),
            ui_token_amount: Some(UiTokenAmount {
                amount: amount.to_string(),
                ..Default::default()
            }),
        }
    }

    fn ui(owner: &Pubkey, lamports: u64, data: &[u8]) -> UiAccount {
        use base64::Engine;
        UiAccount {
            lamports,
            data: UiAccountData::Binary(
                base64::engine::general_purpose::STANDARD.encode(data),
                UiAccountEncoding::Base64,
            ),
            owner: owner.to_string(),
            executable: false,
            rent_epoch: 7,
            space: Some(data.len() as u64),
        }
    }

    #[test]
    fn writable_flags_follow_the_header() {
        // 2 signers (1 readonly), 4 static keys with 1 readonly unsigned, 1 + 1 loaded.
        let f = writable_flags(2, 1, 1, 4, 1, 1);
        assert_eq!(f, vec![true, false, true, false, true, false]);
    }

    #[test]
    fn rebuilds_token_wallet_and_closed_accounts_skips_program_state() {
        let payer = Pubkey::new_unique();
        let vault = Pubkey::new_unique();
        let closed = Pubkey::new_unique();
        let state = Pubkey::new_unique();
        let mint = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let mut template = token_account_data(&mint, &authority, 1);
        template[129] = 1; // close authority set: must survive the rebuild
        let snapshots = HashMap::from([
            (payer, ui(&SYSTEM_PROGRAM, 5, &[])),
            (vault, ui(&TOKEN_PROGRAM, 2_039_280, &template)),
            (state, ui(&Pubkey::new_unique(), 1, &[1, 2, 3])),
        ]);
        let mut r = Reconstructor::new(&snapshots);
        let keys = [payer, vault, closed, state];
        let meta = TransactionStatusMeta {
            pre_balances: vec![100, 2_039_280, 2_039_280, 1],
            post_balances: vec![90, 2_039_280, 0, 2],
            pre_token_balances: vec![
                balance(1, &mint, &authority, 1_000),
                balance(2, &mint, &authority, 5),
            ],
            post_token_balances: vec![balance(1, &mint, &authority, 400)],
            ..Default::default()
        };
        let tx = TxAccounts {
            keys: &keys,
            writable: &[true, true, true, true],
            meta: &meta,
            signature: &[9; 64],
            slot: 42,
        };
        let mut seen = std::collections::HashSet::new();
        let pre = r.pre_states(&tx, &mut seen);
        assert_eq!(
            pre.len(),
            3,
            "payer, vault and the closed account have pre-states"
        );
        let pre_vault = pre[1].account.as_ref().unwrap();
        assert_eq!(pre_vault.data[64..72], 1_000u64.to_le_bytes());

        let post = r.post_states(&tx);
        assert_eq!(post.len(), 3);
        let wallet = post[0].account.as_ref().unwrap();
        assert_eq!(wallet.lamports, 90);
        let v = post[1].account.as_ref().unwrap();
        assert_eq!(v.data[64..72], 400u64.to_le_bytes());
        assert_eq!(v.data[129], 1, "snapshot template kept");
        assert_eq!(v.txn_signature.as_deref(), Some(&[9u8; 64][..]));
        let c = post[2].account.as_ref().unwrap();
        assert_eq!((c.lamports, c.data.len()), (0, 0));
        assert_eq!(r.stats.skipped_program_accounts, 1);
        assert!(
            post[1].account.as_ref().unwrap().write_version
                > pre[2].account.as_ref().unwrap().write_version
        );
    }

    #[test]
    fn synthesised_wsol_account_is_native() {
        let d = token_account_data(&NATIVE_MINT, &Pubkey::new_unique(), 3);
        assert_eq!(d.len(), 165);
        assert_eq!(d[108], 1);
        assert_eq!(d[109], 1);
    }
}
