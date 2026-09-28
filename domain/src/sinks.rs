use serde::Deserialize;
use std::fmt::{self, Display};

#[derive(Debug, Clone, Deserialize, PartialEq, Eq, Hash)]
#[serde(transparent)]
pub struct SinkId(String);

impl SinkId {
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl Display for SinkId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<&str> for SinkId {
    fn from(s: &str) -> Self {
        Self(s.to_owned())
    }
}

impl From<String> for SinkId {
    fn from(s: String) -> Self {
        Self(s)
    }
}
