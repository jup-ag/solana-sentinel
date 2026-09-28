//! One file per rule. Each implements `RuleDef` (identity, typed config,
//! construction) plus `GrpcRule` (stream-fed) or `RpcRule` (poll-fed).

pub mod balance_drop;
pub mod large_transfer;
pub mod low_balance;
