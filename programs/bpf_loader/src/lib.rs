pub mod parser;
pub mod rules;

use crate::parser::BPFLoaderParser;
use interface::{Program, ProgramSpec, StreamRules};
use rules::program_upgrade::ProgramUpgrade;
use rules::program_upgrade_auth_change::ProgramUpgradeAuthChange;

/// Snake_case identifier this program is known by in config and logs.
pub const PROGRAM: Program = Program::new("bpf_loader");

pub fn spec() -> ProgramSpec {
    ProgramSpec::builder(PROGRAM)
        .account(
            BPFLoaderParser,
            StreamRules::new()
                .register::<ProgramUpgrade>()
                .register::<ProgramUpgradeAuthChange>(),
        )
        .finish()
}
