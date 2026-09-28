use shipstern_core::instruction::InstructionShared;
use solana_pubkey::Pubkey;

/// Returns the static account keys that signed the transaction.
///
/// Signers are runtime-guaranteed to be the first `num_required_signatures`
/// entries in `static_keys`. LUT-loaded keys cannot sign.
fn tx_signers_shared(shared: &InstructionShared) -> Vec<Pubkey> {
    let header = &shared.message_header;
    let static_keys = &shared.accounts.static_keys;
    let n = (header.num_required_signatures as usize).min(static_keys.len());

    static_keys[..n]
        .iter()
        .filter_map(|b| <[u8; 32]>::try_from(b.as_slice()).ok().map(Pubkey::from))
        .collect()
}

/// Returns all pubkeys from `watchlist` that signed the transaction.
/// Empty `Vec` means none signed.
pub fn signers_in_watchlist_shared(
    shared: &InstructionShared,
    watchlist: &[Pubkey],
) -> Vec<Pubkey> {
    tx_signers_shared(shared)
        .into_iter()
        .filter(|s| watchlist.contains(s))
        .collect()
}
