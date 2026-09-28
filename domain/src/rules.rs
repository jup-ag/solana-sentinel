use crate::programs::Program;
use std::fmt::{self, Display};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RuleId {
    pub program: Program,
    pub name: &'static str,
}

impl RuleId {
    pub const fn new(program: Program, name: &'static str) -> Self {
        Self { program, name }
    }

    pub const fn as_str(self) -> &'static str {
        self.name
    }
}

impl Display for RuleId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name)
    }
}
