//! Gate keys are claimed before delivery (so racing nodes send once) and settled after it.
//! A claim holds its owner until confirmed; a failed delivery releases it, and a restarted
//! process takes over its own unconfirmed claims, which it never finished delivering.

use parking_lot::Mutex;
use redis::aio::ConnectionLike;
use std::collections::HashSet;
use std::sync::Arc;
use tracing::info;

const DELIVERED: &str = "sent";

/// Claims this process holds and has not settled yet.
#[derive(Clone, Default)]
pub(crate) struct OpenClaims(Arc<Mutex<HashSet<String>>>);

/// `SET key <owner> NX EXAT ts`. `true` means this process should send.
pub(crate) async fn claim(
    conn: &mut impl ConnectionLike,
    open: &OpenClaims,
    key: &str,
    owner: &str,
    ttl_timestamp: i64,
) -> anyhow::Result<bool> {
    // Reserved first, so a concurrent check in this process can't take it over too.
    if !open.0.lock().insert(key.to_string()) {
        return Ok(false);
    }
    let result = try_claim(conn, key, owner, ttl_timestamp).await;
    if !matches!(result, Ok(true)) {
        open.0.lock().remove(key);
    }
    result
}

async fn try_claim(
    conn: &mut impl ConnectionLike,
    key: &str,
    owner: &str,
    ttl_timestamp: i64,
) -> anyhow::Result<bool> {
    let set: Option<()> = redis::cmd("SET")
        .arg(key)
        .arg(owner)
        .arg("NX")
        .arg("EXAT")
        .arg(ttl_timestamp)
        .query_async(conn)
        .await?;
    if set.is_some() {
        return Ok(true);
    }
    // Ours but not open in this process: an earlier run claimed it and died before settling.
    let holder: Option<String> = redis::cmd("GET").arg(key).query_async(conn).await?;
    if holder.as_deref() == Some(owner) {
        info!(key, "taking over an unconfirmed claim from a previous run");
        return Ok(true);
    }
    Ok(false)
}

/// Delivered: keep the key for its cooldown, no longer tied to this owner.
pub(crate) async fn confirm(
    conn: &mut impl ConnectionLike,
    open: &OpenClaims,
    key: &str,
) -> anyhow::Result<()> {
    if !open.0.lock().remove(key) {
        return Ok(());
    }
    redis::cmd("SET")
        .arg(key)
        .arg(DELIVERED)
        .arg("XX")
        .arg("KEEPTTL")
        .query_async::<Option<()>>(conn)
        .await?;
    Ok(())
}

/// Not delivered anywhere: drop the claim so the next occurrence can send.
pub(crate) async fn release(
    conn: &mut impl ConnectionLike,
    open: &OpenClaims,
    key: &str,
    owner: &str,
) -> anyhow::Result<()> {
    if !open.0.lock().remove(key) {
        return Ok(());
    }
    let holder: Option<String> = redis::cmd("GET").arg(key).query_async(conn).await?;
    if holder.as_deref() == Some(owner) {
        redis::cmd("DEL").arg(key).query_async::<()>(conn).await?;
    }
    Ok(())
}
