//! Instruction rule: a watched authority initializes, re-authorizes or drains
//! a durable nonce account. Nonce changes are how a signer's queued
//! transactions get invalidated or hijacked, so each one is worth a look.

use crate::system::instruction::{decode, SystemIx};
use crate::system::PROGRAM;
use serde::Deserialize;
use solana_pubkey::Pubkey;
use solana_sentinel::raw::RawInstruction;
use solana_sentinel::utility::pubkey::deserialize_pubkey_vec;
use solana_sentinel::{Alert, Deps, RuleId, Severity, StreamCtx, StreamRule, Subscription};
use std::collections::HashSet;

#[derive(Debug, Deserialize)]
pub struct NonceActivityConfig {
    /// Wallets whose nonce-authority actions are reported.
    #[serde(deserialize_with = "deserialize_pubkey_vec")]
    pub authorities: Vec<Pubkey>,
}

#[derive(Debug)]
pub struct NonceActivity {
    watched: HashSet<Pubkey>,
}

impl StreamRule for NonceActivity {
    const ID: RuleId = RuleId::new(PROGRAM, "nonce_activity");
    type Config = NonceActivityConfig;
    type Event = RawInstruction;

    fn subscription(cfg: &Self::Config) -> Subscription {
        Subscription::new().tx_accounts_include(cfg.authorities.iter().copied())
    }

    fn build(cfg: Self::Config, _deps: &Deps) -> anyhow::Result<Self> {
        anyhow::ensure!(!cfg.authorities.is_empty(), "authorities must not be empty");
        Ok(Self {
            watched: cfg.authorities.into_iter().collect(),
        })
    }

    fn evaluate(&self, ix: &RawInstruction, ctx: &StreamCtx) -> Option<Alert> {
        let (action, nonce, authority, new_authority) = match decode(ix)? {
            SystemIx::InitializeNonce { nonce, authority } => {
                ("initialize_nonce", nonce, authority, None)
            }
            SystemIx::AuthorizeNonce {
                nonce,
                authority,
                new_authority,
            } => ("authorize_nonce", nonce, authority, Some(new_authority)),
            SystemIx::WithdrawNonce {
                nonce, authority, ..
            } => ("withdraw_nonce", nonce, authority, None),
            SystemIx::Transfer { .. } => return None,
        };
        if !self.watched.contains(&authority) {
            return None;
        }
        let mut key = vec![action.to_string(), nonce.to_string(), authority.to_string()];
        key.extend(new_authority.map(|k| k.to_string()));
        Some(
            Alert::for_rule(Self::ID)
                .severity(Severity::Warn)
                .message(format!(
                    "{action} on {nonce} by watched authority {authority}"
                ))
                .meta("action", action)
                .meta("nonce_account", nonce)
                .meta("authority", authority)
                .meta_opt("new_authority", new_authority)
                .meta("slot", ctx.slot)
                .key(key)
                .scope(ctx.tx_scope())
                .build(),
        )
    }
}
