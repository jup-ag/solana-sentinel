//! Program registry: every program the service can run, one entry each.
//! Adding a program is one line in [`PROGRAMS`].

use interface::ProgramSpec;

pub const PROGRAMS: &[fn() -> ProgramSpec] = &[
    squads_v4::spec,
    spl_token::spec,
    sentinel::spec,
    pyth::spec,
    jup_usd::spec,
    bpf_loader::spec,
    jup_lend::spec,
    system::spec,
];

/// The spec registered under `name`, if any. Building a spec is cheap: no I/O.
pub fn find(name: &str) -> Option<ProgramSpec> {
    PROGRAMS
        .iter()
        .map(|spec| spec())
        .find(|spec| spec.program().as_str() == name)
}

pub fn all() -> Vec<ProgramSpec> {
    PROGRAMS.iter().map(|spec| spec()).collect()
}
