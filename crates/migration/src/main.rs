#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    solana_sentinel_migration::migrate().await?;
    Ok(())
}
