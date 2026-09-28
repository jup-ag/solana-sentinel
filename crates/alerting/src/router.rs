use crate::sinks::AlertSink;
use domain::rules::RuleId;
use domain::sinks::SinkId;
use std::collections::HashMap;

/// Resolves a [`RuleId`] to the set of sinks that should receive its
/// alerts, and owns the sink instances themselves. The alerting service
/// borrows the router for the lifetime of the run.
///
/// Construction is intentionally primitive: callers build the routes
/// table and the sinks map themselves and hand both to [`Router::new`].
/// This keeps the alerting library independent of any particular config
/// format.
pub struct Router {
    routes: HashMap<RuleId, Vec<SinkId>>,
    sinks: HashMap<SinkId, Box<dyn AlertSink>>,
}

impl Router {
    pub fn new(
        routes: HashMap<RuleId, Vec<SinkId>>,
        sinks: HashMap<SinkId, Box<dyn AlertSink>>,
    ) -> Self {
        Self { routes, sinks }
    }

    pub fn route(&self, rule: RuleId) -> &[SinkId] {
        self.routes.get(&rule).map(Vec::as_slice).unwrap_or(&[])
    }

    pub fn sink(&self, id: &SinkId) -> Option<&dyn AlertSink> {
        self.sinks.get(id).map(std::convert::AsRef::as_ref)
    }

    pub fn sink_count(&self) -> usize {
        self.sinks.len()
    }
}
