use crate::parser::squads_multisig_program::config_action;
use crate::parser::{SquadsV4Ix, SquadsV4Ixs};
use crate::rules::{config_change, joined};
use serde::Deserialize;
use solana_pubkey::Pubkey;
use solana_sentinel::utility::pubkey::deserialize_pubkey_vec;
use solana_sentinel::{Alert, GrpcRule, Prefilter, RuleDef, RuleId, Severity, StreamCtx};

#[derive(Debug, Deserialize)]
pub struct TimelockChangeConfig {
    #[serde(deserialize_with = "deserialize_pubkey_vec")]
    pub multisigs: Vec<Pubkey>,
}

#[derive(Debug)]
pub struct TimelockChange {
    config: TimelockChangeConfig,
}

impl RuleDef for TimelockChange {
    const ID: RuleId = RuleId::new(crate::PROGRAM, "timelock_change");
    type Config = TimelockChangeConfig;
    type Deps = ();

    fn build(config: Self::Config, _deps: &()) -> anyhow::Result<Self> {
        Ok(Self { config })
    }
}

impl GrpcRule for TimelockChange {
    type Event = SquadsV4Ixs;

    fn static_prefilter(cfg: &Self::Config) -> Prefilter {
        Prefilter::new().tx_accounts_include(cfg.multisigs.iter().copied())
    }

    fn evaluate(&self, parsed: &SquadsV4Ixs, ctx: &StreamCtx) -> Option<Alert> {
        let _ = &self.config;
        let change = config_change(
            parsed,
            |ix| match ix {
                SquadsV4Ix::MultisigSetTimeLock { accounts, args } => Some((
                    accounts.multisig.to_string(),
                    args.time_lock,
                    args.memo.clone(),
                )),
                _ => None,
            },
            |kind| match kind {
                config_action::Kind::SetTimeLock(tl) => Some(tl.new_time_lock),
                _ => None,
            },
        )?;

        let timelocks = joined(&change.values);
        let key = if change.values.len() == 1 {
            "new_timelock"
        } else {
            "new_timelocks"
        };

        Some(
            Alert::for_rule(Self::ID)
                .severity(Severity::Critical)
                .message(format!("Timelock changed on multisig {}", change.multisig))
                .meta("multisig", &change.multisig)
                .meta("path", change.path)
                .meta(key, &timelocks)
                .meta("memo", change.memo)
                .key([change.multisig, timelocks])
                .scope(ctx.tx_scope())
                .build(),
        )
    }
}
