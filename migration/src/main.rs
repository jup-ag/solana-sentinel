pub mod clickhouse;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    clickhouse::migrate().await?;
    Ok(())
}
