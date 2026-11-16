use crate::programs::Program;
use std::fmt::{self, Display};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RuleId {
    pub program: Program,
    pub name: &'static str,
    /// Tells apart several configured copies of one rule (`instance` in `rules.toml`).
    pub instance: Option<&'static str>,
}

impl RuleId {
    pub const fn new(program: Program, name: &'static str) -> Self {
        Self {
            program,
            name,
            instance: None,
        }
    }

    #[must_use]
    pub const fn with_instance(self, instance: Option<&'static str>) -> Self {
        Self { instance, ..self }
    }

    pub const fn as_str(self) -> &'static str {
        self.name
    }
}

impl Display for RuleId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name)?;
        if let Some(instance) = self.instance {
            write!(f, ":{instance}")?;
        }
        Ok(())
    }
}
