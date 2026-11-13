//! Squads v3 (squads-mpl) multisig: governance changes and new proposals on watched
//! multisigs, over an instruction parser generated from the program's IDL.
//!
//! Config instructions must be signed by the multisig itself, so they only run as a
//! self-CPI inside `execute_transaction`. Each change is seen twice: when proposed, as an
//! `add_instruction` carrying the config instruction, and when executed.

pub mod member_removal;
pub mod new_member_addition;
pub mod new_proposal;
pub mod parser;
pub mod threshold_change;

use crate::squads::{self, to_pubkey, ConfigChange};
use crate::squads_v3::parser::{squads_mpl, SquadsV3Ix, SquadsV3IxParser, SquadsV3Ixs};
use shipstern_core::instruction::Path;
use shipstern_core::KeyBytes;
use solana_pubkey::Pubkey;
use solana_sentinel::domain::alert::AlertBuilder;
use solana_sentinel::{Program, ProgramSpec, RuleId, StreamCtx, StreamRules};
use std::collections::HashSet;

pub const PROGRAM: Program = Program::new("squads_v3");
pub const SQUADS_V3_PROGRAM_ID: Pubkey =
    Pubkey::from_str_const("SMPLecH534NA9acpos4G6x7uf3LWbCAwZQE9e8ZekMu");

pub fn spec() -> ProgramSpec {
    ProgramSpec::new(PROGRAM).instruction(
        SquadsV3IxParser {},
        StreamRules::new()
            .register::<member_removal::MemberRemoval>()
            .register::<new_member_addition::NewMemberAddition>()
            .register::<new_proposal::NewProposal>()
            .register::<threshold_change::ThresholdChange>(),
    )
}

/// The config change `pick` selects, proposed or executed, if its multisig is watched.
/// `pick` returns the multisig the config instruction targets and the value to report.
pub fn watched_config_change<T>(
    parsed: &SquadsV3Ixs,
    watched: &HashSet<Pubkey>,
    pick: impl Fn(&SquadsV3Ix) -> Option<(KeyBytes<32>, T)>,
) -> Option<ConfigChange<T>> {
    let (multisig, transaction, path, value) = match &parsed.instruction {
        SquadsV3Ix::AddInstruction { accounts, args } => {
            if args.program_id.0 != squads_mpl::PROGRAM_ID
                || !watched.contains(&to_pubkey(accounts.multisig))
            {
                return None;
            }
            let keys: Vec<_> = args.keys.iter().map(|meta| meta.pubkey).collect();
            let nested =
                squads_mpl::resolve_instruction_default(&keys, &args.data, &Path::new_single(0))
                    .ok()?;
            // Only this transaction's multisig signs at execution, so a config instruction
            // aimed at another multisig can never run.
            let (target, value) = pick(&nested.instruction)?;
            if target != accounts.multisig {
                return None;
            }
            (
                accounts.multisig,
                Some(to_pubkey(accounts.transaction)),
                "proposal",
                value,
            )
        }
        ix => {
            let (multisig, value) = pick(ix)?;
            (multisig, None, "executed", value)
        }
    };
    let multisig = to_pubkey(multisig);
    watched.contains(&multisig).then(|| ConfigChange {
        multisig,
        transaction,
        values: vec![value],
        memo: None,
        path,
    })
}

/// v4's alert skeleton plus the v3 transaction. v4 turns `transaction` into a v4 app URL, so
/// it gets `None`. The key carries `path` and the signature so a cooldown never suppresses a
/// different proposal or execution of the same change.
pub fn config_change_alert<T: Clone>(
    id: RuleId,
    change: &ConfigChange<T>,
    value_key: &str,
    value: &str,
    ctx: &StreamCtx,
) -> AlertBuilder {
    let without_url = ConfigChange {
        transaction: None,
        ..change.clone()
    };
    squads::config_change_alert(id, &without_url, value_key, value, ctx)
        .meta_opt("transaction", change.transaction)
        .key([
            change.multisig.to_string(),
            value.to_string(),
            change.path.to_string(),
            ctx.tx_sig().to_string(),
        ])
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sentinel::testing::{ix_update, parse, stream_ctx};

    /// `sha256("global:add_instruction")[..8]`.
    const ADD_INSTRUCTION: [u8; 8] = [0x0b, 0x46, 0x88, 0xa6, 0xca, 0x37, 0xf6, 0x4a];
    /// `sha256("global:add_member")[..8]`.
    const ADD_MEMBER: [u8; 8] = [0x0d, 0x74, 0x7b, 0x82, 0x7e, 0xc6, 0x39, 0x22];

    fn add_member(member: &Pubkey) -> Vec<u8> {
        [&ADD_MEMBER[..], member.as_ref()].concat()
    }

    fn pick_member(ix: &SquadsV3Ix) -> Option<(KeyBytes<32>, String)> {
        match ix {
            SquadsV3Ix::AddMember { accounts, args } => {
                Some((accounts.multisig, args.new_member.to_string()))
            }
            _ => None,
        }
    }

    /// `add_instruction` on `multisig` proposing `inner` for `program`; the nested keys are
    /// `target` x3.
    async fn propose(
        multisig: Pubkey,
        target: Pubkey,
        tx: Pubkey,
        program: Pubkey,
        inner: &[u8],
    ) -> SquadsV3Ixs {
        let mut data = ADD_INSTRUCTION.to_vec();
        data.extend_from_slice(program.as_ref());
        data.extend_from_slice(&3u32.to_le_bytes());
        for _ in 0..3 {
            data.extend_from_slice(target.as_ref());
            data.extend_from_slice(&[1, 1]);
        }
        data.extend_from_slice(&(inner.len() as u32).to_le_bytes());
        data.extend_from_slice(inner);
        let creator = Pubkey::new_unique();
        let accounts = [
            multisig,
            tx,
            Pubkey::new_unique(),
            creator,
            Pubkey::default(),
        ];
        parse(
            &SquadsV3IxParser {},
            &ix_update(SQUADS_V3_PROGRAM_ID, &accounts, data),
        )
        .await
    }

    #[test]
    fn idl_address_matches_program_id() {
        assert_eq!(
            Pubkey::new_from_array(squads_mpl::PROGRAM_ID),
            SQUADS_V3_PROGRAM_ID
        );
    }

    #[tokio::test]
    async fn proposal_decodes_nested_v3_config_ix_only() {
        let (multisig, tx, member) = (
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
        );
        let watched = HashSet::from([multisig]);

        let parsed = propose(
            multisig,
            multisig,
            tx,
            SQUADS_V3_PROGRAM_ID,
            &add_member(&member),
        )
        .await;
        let change = watched_config_change(&parsed, &watched, pick_member).unwrap();
        assert_eq!(change.path, "proposal");
        assert_eq!(change.transaction, Some(tx));
        assert_eq!(change.values, [member.to_string()]);

        let other_program = propose(
            multisig,
            multisig,
            tx,
            Pubkey::new_unique(),
            &add_member(&member),
        )
        .await;
        assert!(watched_config_change(&other_program, &watched, pick_member).is_none());

        let other_multisig = propose(
            multisig,
            Pubkey::new_unique(),
            tx,
            SQUADS_V3_PROGRAM_ID,
            &add_member(&member),
        )
        .await;
        assert!(watched_config_change(&other_multisig, &watched, pick_member).is_none());

        let truncated = propose(multisig, multisig, tx, SQUADS_V3_PROGRAM_ID, &ADD_MEMBER).await;
        assert!(watched_config_change(&truncated, &watched, pick_member).is_none());
    }

    #[tokio::test]
    async fn executed_change_has_its_own_dedup_key() {
        let (multisig, member) = (Pubkey::new_unique(), Pubkey::new_unique());
        let watched = HashSet::from([multisig]);
        let accounts = [multisig, Pubkey::default(), Pubkey::default()];
        let executed = parse(
            &SquadsV3IxParser {},
            &ix_update(SQUADS_V3_PROGRAM_ID, &accounts, add_member(&member)),
        )
        .await;
        let executed = watched_config_change(&executed, &watched, pick_member).unwrap();
        assert_eq!(executed.path, "executed");
        assert_eq!(executed.transaction, None);

        let proposed = propose(
            multisig,
            multisig,
            Pubkey::new_unique(),
            SQUADS_V3_PROGRAM_ID,
            &add_member(&member),
        )
        .await;
        let proposed = watched_config_change(&proposed, &watched, pick_member).unwrap();

        let id = RuleId::new(PROGRAM, "test");
        let ctx = stream_ctx(1, Some("sig"));
        let alert = |c: &ConfigChange<String>| {
            config_change_alert(id, c, "member", &member.to_string(), &ctx).build()
        };
        let (executed, proposed) = (alert(&executed), alert(&proposed));
        assert_ne!(executed.local_gate_key, proposed.local_gate_key);
        assert!(!proposed.metadata.contains_key("squads_url"));
    }
}
