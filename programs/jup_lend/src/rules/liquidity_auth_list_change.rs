use crate::programs::liquidity::{AuthorizationList, LiquidityStateUpdate, UserClass};
use crate::rules::cache::JupLendCache;
use anyhow::Context;
use async_trait::async_trait;
use borsh::BorshDeserialize;
use interface::{
    Alert, BootstrapCtx, DedupScope, GrpcRule, NoConfig, Prefilter, RuleDef, RuleId, Severity,
    StreamCtx,
};
use parking_lot::Mutex;
use shipstern_core::KeyBytes;
use solana_pubkey::Pubkey;
use solana_rpc_client::api::config::{RpcAccountInfoConfig, UiAccountEncoding};
use tracing::debug;

#[derive(Debug)]
pub struct LiquidityAuthListChange {
    auth_list_pubkey: Pubkey,
    /// Last known state, so each change diffs against the latest observed list.
    auth_list_state: Mutex<Option<AuthorizationList>>,
}

/// Base58 pubkeys present in `a` but not in `b`.
fn missing_from(a: &[KeyBytes<32>], b: &[KeyBytes<32>]) -> Vec<String> {
    a.iter()
        .filter(|key| !b.contains(key))
        .map(ToString::to_string)
        .collect()
}

/// `addr:class` entries present in `a` but not in `b`. A class change on an
/// existing addr shows up as one added and one removed entry.
fn user_classes_missing_from(a: &[UserClass], b: &[UserClass]) -> Vec<String> {
    a.iter()
        .filter(|entry| !b.contains(entry))
        .map(|entry| format!("{}:{}", entry.addr, entry.class))
        .collect()
}

#[async_trait]
impl RuleDef for LiquidityAuthListChange {
    const ID: RuleId = RuleId::new(crate::PROGRAM, "jup_lend_liquidity_auth_list_change");
    type Config = NoConfig;
    type Deps = JupLendCache;

    fn build(_cfg: NoConfig, _deps: &JupLendCache) -> anyhow::Result<Self> {
        Ok(Self {
            auth_list_pubkey: AuthorizationList::derive_pda(),
            auth_list_state: Mutex::new(None),
        })
    }

    async fn bootstrap(&mut self, ctx: &BootstrapCtx) -> anyhow::Result<()> {
        let account = ctx
            .rpc
            .get_ui_account_with_config(
                &self.auth_list_pubkey,
                RpcAccountInfoConfig {
                    encoding: Some(UiAccountEncoding::Base64),
                    ..RpcAccountInfoConfig::default()
                },
            )
            .await
            .context("Failed to fetch authorization list account")?
            .value
            .ok_or_else(|| anyhow::anyhow!("Authorization list account not found"))?;

        let auth_list_state = account
            .data
            .decode()
            .and_then(|data| AuthorizationList::deserialize(&mut &data[8..]).ok())
            .context("Failed to deserialize authorization list")?;

        *self.auth_list_state.lock() = Some(auth_list_state);
        Ok(())
    }
}

impl GrpcRule for LiquidityAuthListChange {
    type Event = LiquidityStateUpdate;

    fn static_prefilter(_cfg: &NoConfig) -> Prefilter {
        Prefilter::new().accounts([AuthorizationList::derive_pda()])
    }

    fn evaluate(&self, parsed: &LiquidityStateUpdate, _ctx: &StreamCtx) -> Option<Alert> {
        let LiquidityStateUpdate::AuthorizationList {
            auth_list_pubkey,
            state: new_auth_list,
            tx_signature,
        } = parsed
        else {
            return None;
        };

        let old_auth_list = self.auth_list_state.lock().replace(new_auth_list.clone())?;

        let added_auth_users = missing_from(&new_auth_list.auth_users, &old_auth_list.auth_users);
        let added_guardians = missing_from(&new_auth_list.guardians, &old_auth_list.guardians);
        let added_user_classes =
            user_classes_missing_from(&new_auth_list.user_classes, &old_auth_list.user_classes);

        // Removing an entry shrinks the authority set; only additions grant new authority.
        if added_auth_users.is_empty()
            && added_guardians.is_empty()
            && added_user_classes.is_empty()
        {
            return None;
        }

        let added = [
            ("added_auth_users", added_auth_users),
            ("added_guardians", added_guardians),
            ("added_user_classes", added_user_classes),
        ]
        .into_iter()
        .filter(|(_, keys)| !keys.is_empty())
        .map(|(key, keys)| (key, format!("{keys:?}")));

        debug!("jup_lend: Authorization list changed");

        Some(
            Alert::for_rule(Self::ID)
                .severity(Severity::Warn)
                .message("JUP Lend: Authorization list changed")
                .metas(added)
                .meta("tx_sig", tx_signature)
                .key([auth_list_pubkey.to_string(), tx_signature.clone()])
                .scope(DedupScope::Tx(tx_signature.clone()))
                .build(),
        )
    }
}
