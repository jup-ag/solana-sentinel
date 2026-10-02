//! One file per rule. Each implements `StreamRule` (stream-fed) or
//! `PollRule` (poll-fed): identity, typed config, subscription, `evaluate`.

pub mod balance_drop;
pub mod large_transfer;
pub mod low_balance;
