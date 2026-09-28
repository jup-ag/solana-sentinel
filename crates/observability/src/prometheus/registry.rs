use prometheus::{Encoder, Registry, TextEncoder};
use std::sync::LazyLock;

/// Global registry. All metrics below register themselves here on first access.
pub static REGISTRY: LazyLock<Registry> = LazyLock::new(Registry::new);

/// Encode the current registry snapshot as the Prometheus text exposition
/// format, for the `/metrics` HTTP handler.
pub fn gather() -> Result<(String, Vec<u8>), prometheus::Error> {
    let encoder = TextEncoder::new();
    let mut buffer = Vec::new();
    encoder.encode(&REGISTRY.gather(), &mut buffer)?;
    Ok((encoder.format_type().to_string(), buffer))
}
