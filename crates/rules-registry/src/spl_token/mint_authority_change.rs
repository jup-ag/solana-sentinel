//! Account rule: a watched mint's mint authority or freeze authority changed,
//! including being set to none (revoked).

use crate::spl_token::{fetch_mints, PROGRAM};
use async_trait::async_trait;
use serde::Deserialize;
use shipstern_spl_token_parser::account::Account as TokenState;
use shipstern_spl_token_parser::TokenProgramState;
use solana_pubkey::Pubkey;
use solana_sentinel::utility::pubkey::{deserialize_pubkey_vec, to_solana_pubkey};
use solana_sentinel::{
    Alert, BootstrapCtx, Deps, RuleId, Severity, StateCache, StreamCtx, StreamRule, Subscription,
};

#[derive(Debug, Deserialize)]
pub struct MintAuthorityChangeConfig {
    #[serde(deserialize_with = "deserialize_pubkey_vec")]
    pub mints: Vec<Pubkey>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Authorities {
    pub mint: Option<Pubkey>,
    pub freeze: Option<Pubkey>,
}

#[derive(Debug)]
pub struct MintAuthorityChange {
    mints: Vec<Pubkey>,
    last: StateCache<Pubkey, Authorities>,
}

fn fmt_authority(key: Option<Pubkey>) -> String {
    key.map_or_else(|| "none".to_string(), |k| k.to_string())
}

#[async_trait]
impl StreamRule for MintAuthorityChange {
    const ID: RuleId = RuleId::new(PROGRAM, "mint_authority_change");
    type Config = MintAuthorityChangeConfig;
    type Event = TokenProgramState;

    fn subscription(cfg: &Self::Config) -> Subscription {
        Subscription::new().accounts(cfg.mints.iter().copied())
    }

    fn build(cfg: Self::Config, _deps: &Deps) -> anyhow::Result<Self> {
        anyhow::ensure!(!cfg.mints.is_empty(), "mints must not be empty");
        Ok(Self {
            mints: cfg.mints,
            last: StateCache::default(),
        })
    }

    async fn bootstrap(&mut self, ctx: &BootstrapCtx) -> anyhow::Result<()> {
        let fetched = fetch_mints(&ctx.rpc, &self.mints).await?;
        self.last.seed(fetched.iter().map(|(pk, m)| {
            let auth = Authorities {
                mint: m.mint_authority.into(),
                freeze: m.freeze_authority.into(),
            };
            (*pk, auth)
        }));
        Ok(())
    }

    fn evaluate(&self, ev: &Self::Event, ctx: &StreamCtx) -> Option<Alert> {
        let TokenState::Mint(mint) = ev.account.as_ref()? else {
            return None;
        };
        let pubkey = ctx.account?;
        let current = Authorities {
            mint: mint.mint_authority.as_ref().map(|k| to_solana_pubkey(k.0)),
            freeze: mint
                .freeze_authority
                .as_ref()
                .map(|k| to_solana_pubkey(k.0)),
        };
        let previous = self.last.replace(Self::ID, pubkey, current)?;
        if previous == current {
            return None;
        }
        let changed = match (
            previous.mint != current.mint,
            previous.freeze != current.freeze,
        ) {
            (true, true) => "mint_and_freeze_authority",
            (true, false) => "mint_authority",
            _ => "freeze_authority",
        };
        Some(
            Alert::for_rule(Self::ID)
                .severity(Severity::Critical)
                .message(format!(
                    "{pubkey} {changed} changed: mint {} -> {}, freeze {} -> {}",
                    fmt_authority(previous.mint),
                    fmt_authority(current.mint),
                    fmt_authority(previous.freeze),
                    fmt_authority(current.freeze)
                ))
                .meta("mint", pubkey)
                .meta("changed", changed)
                .meta("previous_mint_authority", fmt_authority(previous.mint))
                .meta("current_mint_authority", fmt_authority(current.mint))
                .meta("previous_freeze_authority", fmt_authority(previous.freeze))
                .meta("current_freeze_authority", fmt_authority(current.freeze))
                .meta("slot", ctx.slot)
                .key([
                    pubkey.to_string(),
                    fmt_authority(current.mint),
                    fmt_authority(current.freeze),
                ])
                .scope(ctx.tx_scope())
                .build(),
        )
    }
}
