//! Record an incident's slot window into a replay file for `[data_source] type = "replay"`.
//! See `solana_sentinel_data_source::replay::record` for what is fetched and how.

use clap::Parser;
use solana_sentinel_data_source::replay::record::{record, RecordArgs};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .init();
    let header = record(RecordArgs::parse()).await?;
    for note in &header.notes {
        eprintln!("note: {note}");
    }
    println!(
        "{} slots {}..={} ({} notes)",
        header.name,
        header.first_slot,
        header.last_slot,
        header.notes.len()
    );
    Ok(())
}
