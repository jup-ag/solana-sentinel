//! Step 2 of startup: resolve every configured program against the registry
//! and type-check its rule configs. No network connection is open yet, so a
//! typo in `rules.toml` fails here with the program, rule and field named.

use crate::config::{ProgramRules, SentinelConfig};
use crate::wiring::WiringError;
use solana_sentinel::{ProgramSpec, RawRuleMap, ValidatedProgram};
use std::collections::HashSet;

pub fn validate_programs(
    config: &SentinelConfig,
    registry: &[fn() -> ProgramSpec],
) -> Result<Vec<ValidatedProgram>, WiringError> {
    let programs = config.programs_with_rules();
    let mut out = Vec::with_capacity(programs.len());
    let mut seen: HashSet<String> = HashSet::with_capacity(programs.len());

    for p in programs {
        let spec = registry
            .iter()
            .map(|spec| spec())
            .find(|spec| spec.program().as_str() == p.program)
            .ok_or_else(|| WiringError::UnknownProgram(p.program.clone()))?;
        if !seen.insert(p.program.clone()) {
            return Err(WiringError::DuplicateProgram(spec.program()));
        }
        let raw = raw_rules(p, spec.program())?;
        out.push(spec.validate(&raw)?);
    }
    Ok(out)
}

fn raw_rules(
    p: &ProgramRules,
    program: domain::programs::Program,
) -> Result<RawRuleMap, WiringError> {
    let mut raw = RawRuleMap::with_capacity(p.rules.len());
    for r in &p.rules {
        if raw.insert(r.name.clone(), r.config.clone()).is_some() {
            return Err(WiringError::DuplicateRule {
                program,
                rule: r.name.clone(),
            });
        }
    }
    Ok(raw)
}
