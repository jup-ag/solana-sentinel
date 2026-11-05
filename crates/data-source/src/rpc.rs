//! RPC client construction shared by the service and the websocket source, so
//! every request carries the configured `[rpc.headers]`.

use anyhow::Context;
use reqwest_rpc::header::{HeaderName, HeaderValue};
use solana_rpc_client::http_sender::HttpSender;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_rpc_client::rpc_client::RpcClientConfig;
use std::collections::HashMap;
use std::time::Duration;

/// RPC client sending `headers` on every request. Keeps the 30s timeouts
/// `RpcClient::new` uses.
pub fn build_rpc_client(url: &str, headers: &HashMap<String, String>) -> anyhow::Result<RpcClient> {
    if headers.is_empty() {
        return Ok(RpcClient::new(url.to_owned()));
    }
    let mut map = HttpSender::default_headers();
    for (name, value) in headers {
        let name = HeaderName::from_bytes(name.as_bytes())
            .with_context(|| format!("rpc header name {name:?} is invalid"))?;
        let value = HeaderValue::from_str(value)
            .with_context(|| format!("rpc header {name} has an invalid value"))?;
        map.insert(name, value);
    }
    let timeout = Duration::from_secs(30);
    let http = reqwest_rpc::Client::builder()
        .default_headers(map)
        .timeout(timeout)
        .pool_idle_timeout(timeout)
        .build()
        .context("failed to build rpc http client")?;
    let sender = HttpSender::new_with_client(url.to_owned(), http);
    Ok(RpcClient::new_sender(sender, RpcClientConfig::default()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn rpc_client_sends_configured_headers() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        // Minimal HTTP server: capture the raw request, reply with a getSlot result.
        let server = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = vec![0u8; 8192];
            let n = sock.read(&mut buf).await.unwrap();
            let body = r#"{"jsonrpc":"2.0","result":42,"id":1}"#;
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: \
                 {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            sock.write_all(resp.as_bytes()).await.unwrap();
            String::from_utf8_lossy(&buf[..n]).to_string()
        });

        let headers = HashMap::from([("x-identity".to_owned(), "sentinel-test".to_owned())]);
        let rpc = build_rpc_client(&format!("http://{addr}/"), &headers).unwrap();
        assert_eq!(rpc.get_slot().await.unwrap(), 42);

        let request = server.await.unwrap();
        assert!(
            request.to_lowercase().contains("x-identity: sentinel-test"),
            "{request}"
        );
    }
}
