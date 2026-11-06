//! Program shared state: caches every rule of a program reads, built once
//! before any rule and the only place a chain-discovered watch set lives.

use crate::dynamic::DynamicSubscription;
use crate::subscription::Subscription;
use crate::BootstrapCtx;
use async_trait::async_trait;
use std::any::{type_name, Any};
use std::sync::Arc;
use std::time::Duration;

/// Default [`ProgramDeps::min_push_interval`]: every push resends the whole filter set.
pub const DEFAULT_MIN_PUSH_INTERVAL: Duration = Duration::from_secs(10);

/// Object-safe apart from `bootstrap`, so the framework holds `Arc<dyn ProgramDeps>`.
#[async_trait]
pub trait ProgramDeps: Any + Send + Sync {
    async fn bootstrap(ctx: &BootstrapCtx) -> anyhow::Result<Self>
    where
        Self: Sized;

    /// Accounts discovered on chain (vaults, PDAs) that every pipeline of the program subscribes to,
    /// unless the pipeline narrowed its [`DynamicScope`](crate::DynamicScope).
    fn dynamic_subscription(&self) -> Subscription {
        Subscription::default()
    }

    /// The dynamic subscription split by purpose; each stream pipeline takes the tags its
    /// [`DynamicScope`](crate::DynamicScope) names. The default is `dynamic_subscription`, untagged,
    /// which every pipeline with the default scope takes. When overridden, the framework no longer
    /// calls `dynamic_subscription`.
    fn tagged_dynamic_subscription(&self) -> DynamicSubscription {
        DynamicSubscription::untagged(self.dynamic_subscription())
    }

    /// How often to `refresh` and push a changed dynamic subscription to the live stream.
    fn refresh_interval(&self) -> Option<Duration> {
        None
    }

    /// Least time between two filter pushes of this program, whatever `refresh_interval` is.
    /// A change seen sooner waits for the gap, then goes out in one push.
    fn min_push_interval(&self) -> Duration {
        DEFAULT_MIN_PUSH_INTERVAL
    }

    /// Re-read chain state so the next `dynamic_subscription` picks up new accounts.
    async fn refresh(&self, _ctx: &BootstrapCtx) -> anyhow::Result<()> {
        Ok(())
    }
}

#[async_trait]
impl ProgramDeps for () {
    async fn bootstrap(_ctx: &BootstrapCtx) -> anyhow::Result<Self> {
        Ok(())
    }
}

/// The program's bootstrapped deps as rules receive them in `build`.
/// `get::<Cache>()` fails if the program registered a different type.
#[derive(Clone)]
pub struct Deps(pub(crate) Arc<dyn ProgramDeps>);

impl Default for Deps {
    fn default() -> Self {
        Self(Arc::new(()))
    }
}

impl Deps {
    pub fn get<D: ProgramDeps>(&self) -> anyhow::Result<&D> {
        (&*self.0 as &dyn Any).downcast_ref::<D>().ok_or_else(|| {
            anyhow::anyhow!(
                "rule asked for deps `{}` but the program registered none of that type",
                type_name::<D>()
            )
        })
    }
}
