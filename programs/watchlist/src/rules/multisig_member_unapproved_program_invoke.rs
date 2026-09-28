use serde::Deserialize;
use solana_pubkey::Pubkey;
use solana_sdk_ids::system_program::ID as SYSTEM_PROGRAM_ID;
use solana_sentinel::raw::RawTransaction;
use solana_sentinel::utility::constants::*;
use solana_sentinel::utility::pubkey::{deserialize_pubkey_vec, join_pubkeys};
use solana_sentinel::utility::signers::signers_in_watchlist_shared;
use solana_sentinel::{Alert, GrpcRule, Prefilter, RuleDef, RuleId, Severity, StreamCtx};
use std::collections::HashSet;

const APPROVED_PROGRAMS: &[Pubkey] = &[
    // System Program
    SYSTEM_PROGRAM_ID,
    // SPL Token Program
    SPL_TOKEN_PROGRAM_ID,
    // SPL Token 2022 Program
    SPL_TOKEN_2022_PROGRAM_ID,
    // Associated Token Program
    ASSOCIATED_TOKEN_PROGRAM_ID,
    // Squads V4 Program
    SQUADS_V4_PROGRAM_ID,
    // Squads V3 Program
    SQUADS_V3_PROGRAM_ID,
    // Squads MPL
    SQUADS_MPL_PROGRAM_ID,
    // Squads MPL V2
    SQUADS_MPL_V2_PROGRAM_ID,
    // Squads TX Meta Program
    SQUADS_TX_META_PROGRAM_ID,
    // Compute Budget Program
    COMPUTE_BUDGET_PROGRAM_ID,
    // BPF Loader Programs
    BPF_LOADER_PROGRAM_ID,
    BPF_LOADER_2_PROGRAM_ID,
    BPF_LOADER_UPGRADEABLE_PROGRAM_ID,
    // Memo Program
    MEMO_PROGRAM_ID,
    // Memo Program V1
    MEMO_PROGRAM_V1_ID,
    // Metaplex Token Metadata Program
    MPL_TOKEN_METADATA_PROGRAM_ID,
    // Metaplex Core Program
    MPL_CORE_PROGRAM_ID,
    // Ed25519 Program
    ED25519_PROGRAM_ID,
    // Stake Program
    STAKE_PROGRAM_ID,
    // ALT Program
    ADDRESS_LOOKUP_TABLE_PROGRAM_ID,
    // Vote Stake History Program
    VOTE_STAKE_HISTORY_PROGRAM_ID,
];

#[derive(Debug, Deserialize)]
pub struct MultisigMemberUnapprovedProgramInvokeConfig {
    #[serde(deserialize_with = "deserialize_pubkey_vec")]
    pub members: Vec<Pubkey>,
}

#[derive(Debug)]
pub struct MultisigMemberUnapprovedProgramInvoke {
    config: MultisigMemberUnapprovedProgramInvokeConfig,
}

impl RuleDef for MultisigMemberUnapprovedProgramInvoke {
    const ID: RuleId = RuleId::new(crate::PROGRAM, "multisig_member_unknown_program_invoke");
    type Config = MultisigMemberUnapprovedProgramInvokeConfig;
    type Deps = ();

    fn build(config: Self::Config, _deps: &()) -> anyhow::Result<Self> {
        Ok(Self { config })
    }
}

impl GrpcRule for MultisigMemberUnapprovedProgramInvoke {
    type Event = RawTransaction;

    fn static_prefilter(cfg: &Self::Config) -> Prefilter {
        Prefilter::new().tx_accounts_include(cfg.members.iter().copied())
    }

    fn evaluate(&self, parsed: &RawTransaction, ctx: &StreamCtx) -> Option<Alert> {
        let signers = signers_in_watchlist_shared(&parsed.shared, &self.config.members);
        if signers.is_empty() {
            return None;
        }

        // Unique non-approved programs invoked anywhere in the tx.
        // Skip any ix whose caller is Squads — those are authorized by the
        // multisig program itself and considered safe.
        let mut unapproved: Vec<Pubkey> = parsed
            .ixs
            .iter()
            .filter(|ix| ix.invoked_by != Some(SQUADS_V4_PROGRAM_ID))
            .map(|ix| ix.program_id)
            .filter(|program_id| !APPROVED_PROGRAMS.contains(program_id))
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        // HashSet iteration order varies per process; sort so the dedup key is
        // identical for the same tx across pods.
        unapproved.sort_unstable();

        if unapproved.is_empty() {
            return None;
        }

        let signers_str = join_pubkeys(&signers);
        let unapproved_str = join_pubkeys(&unapproved);

        Some(
            Alert::for_rule(Self::ID)
                .severity(Severity::Warn)
                .message(format!(
                    "Invoke of unapproved program(s) {unapproved_str} by multisig signer(s) \
                     {signers_str}"
                ))
                .meta(
                    if signers.len() == 1 {
                        "multisig_signer"
                    } else {
                        "members"
                    },
                    &signers_str,
                )
                .meta(
                    if unapproved.len() == 1 {
                        "program_id"
                    } else {
                        "program_ids"
                    },
                    &unapproved_str,
                )
                // Key on the condition only; the sig lives in the scope, so the
                // fleet gate still dedups per tx while the cooldown gate can
                // rate-limit a member repeatedly invoking the same programs.
                .key([unapproved_str, signers_str])
                .scope(ctx.tx_scope())
                .build(),
        )
    }
}
