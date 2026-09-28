use crate::parser::squads_multisig_program::config_action;
use crate::parser::{SquadsV4Ix, SquadsV4Ixs};
use crate::rules::{config_change, joined};
use interface::{Alert, GrpcRule, Prefilter, RuleDef, RuleId, Severity, StreamCtx};
use serde::Deserialize;
use solana_pubkey::Pubkey;
use utility::pubkey::deserialize_pubkey_vec;

#[derive(Debug, Deserialize)]
pub struct ThresholdChangeConfig {
    #[serde(deserialize_with = "deserialize_pubkey_vec")]
    pub multisigs: Vec<Pubkey>,
}

#[derive(Debug)]
pub struct ThresholdChange {
    config: ThresholdChangeConfig,
}

impl RuleDef for ThresholdChange {
    const ID: RuleId = RuleId::new(crate::PROGRAM, "threshold_change");
    type Config = ThresholdChangeConfig;
    type Deps = ();

    fn build(config: Self::Config, _deps: &()) -> anyhow::Result<Self> {
        Ok(Self { config })
    }
}

impl GrpcRule for ThresholdChange {
    type Event = SquadsV4Ixs;

    fn static_prefilter(cfg: &Self::Config) -> Prefilter {
        Prefilter::new().tx_accounts_include(cfg.multisigs.iter().copied())
    }

    fn evaluate(&self, parsed: &SquadsV4Ixs, ctx: &StreamCtx) -> Option<Alert> {
        let _ = &self.config;
        let change = config_change(
            parsed,
            |ix| match ix {
                SquadsV4Ix::MultisigChangeThreshold { accounts, args } => Some((
                    accounts.multisig.to_string(),
                    args.new_threshold,
                    args.memo.clone(),
                )),
                _ => None,
            },
            |kind| match kind {
                config_action::Kind::ChangeThreshold(ct) => Some(ct.new_threshold),
                _ => None,
            },
        )?;

        let thresholds = joined(&change.values);
        // Ideally one change per transaction, but a config transaction may bundle several.
        let key = if change.values.len() == 1 {
            "new_threshold"
        } else {
            "new_thresholds"
        };

        Some(
            Alert::for_rule(Self::ID)
                .severity(Severity::Critical)
                .message(format!("Threshold changed on multisig {}", change.multisig))
                .meta("multisig", &change.multisig)
                .meta("path", change.path)
                .meta(key, &thresholds)
                .meta("memo", change.memo)
                .key([change.multisig, thresholds])
                .scope(ctx.tx_scope())
                .build(),
        )
    }
}
