//! Program shared state: caches every rule of a program reads, built once
//! before any rule and the only place a chain-discovered watch set lives.

use crate::subscription::Subscription;
use crate::BootstrapCtx;
use async_trait::async_trait;
use std::time::Duration;

#[async_trait]
pub trait ProgramDeps: Clone + Send + Sync + 'static {
    async fn bootstrap(ctx: &BootstrapCtx) -> anyhow::Result<Self>;

    /// Accounts discovered on chain (vaults, PDAs) that every pipeline of the program subscribes to.
    fn dynamic_subscription(&self) -> Subscription {
        Subscription::default()
    }

    /// How often to `refresh` and push a changed `dynamic_subscription` to the live stream.
    fn refresh_interval(&self) -> Option<Duration> {
        None
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
