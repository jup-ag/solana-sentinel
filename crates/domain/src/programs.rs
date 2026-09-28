use std::fmt::{self, Display};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Program(&'static str);

impl Program {
    pub const fn new(name: &'static str) -> Self {
        Self(name)
    }

    pub const fn as_str(self) -> &'static str {
        self.0
    }
}

impl Display for Program {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}
