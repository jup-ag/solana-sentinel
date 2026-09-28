use klickhouse::{Client as KlickhouseClient, ClientOptions};
use refinery::embed_migrations;
use std::env;
use std::sync::Arc;
use tokio_rustls::rustls::{ClientConfig, RootCertStore};
use tokio_rustls::TlsConnector;
use tracing::info;

pub async fn migrate() -> anyhow::Result<()> {
    embed_migrations!("clickhouse");
    setup_crypto_provider();

    let use_tls = env::var("CLICKHOUSE_TLS")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false); // default to false if not set

    let host_with_port = env::var("CLICKHOUSE_HOST").expect("CLICKHOUSE_HOST must be set");
    let database = env::var("CLICKHOUSE_DATABASE").expect("CLICKHOUSE_DATABASE must be set");
    let username = env::var("CLICKHOUSE_USERNAME").expect("CLICKHOUSE_USERNAME must be set");
    let password = env::var("CLICKHOUSE_PASSWORD").expect("CLICKHOUSE_PASSWORD must be set");

    let opts = ClientOptions {
        username,
        password,
        default_database: database,
        tcp_nodelay: true,
    };

    let mut client = if use_tls {
        info!(
            "Connecting to ClickHouse (native TLS) at {}",
            host_with_port
        );
        // Root certs from the system trust store (webpki-roots also works)
        let mut roots = RootCertStore::empty();
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

        let config = ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();

        let connector = TlsConnector::from(Arc::new(config));

        // SNI name = hostname only, strip the :port
        let hostname = host_with_port
            .split(':')
            .next()
            .expect("CLICKHOUSE_HOST must contain a hostname")
            .to_owned();
        let server_name = rustls_pki_types::ServerName::try_from(hostname)?; // owns the string -> 'static

        KlickhouseClient::connect_tls(host_with_port, opts, server_name, &connector).await?
    } else {
        info!("Connecting to ClickHouse (plaintext) at {}", host_with_port);
        KlickhouseClient::connect(host_with_port, opts).await?
    };

    let report = migrations::runner().run_async(&mut client).await?;

    info!(
        "Applied {} ClickHouse migrations",
        report.applied_migrations().len()
    );
    for migration in report.applied_migrations() {
        info!(
            "Migration Applied - Version: {}, Name: {}",
            migration.version(),
            migration.name()
        );
    }
    Ok(())
}

fn setup_crypto_provider() {
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("failed to install rustls crypto provider");
}
