//! Strip secrets from URLs before they reach logs or error messages.
//!
//! RPC and PubSub providers put API keys in the path (`https://host/<key>`), the query
//! (`?api-key=…`) or the userinfo (`https://user:pass@host`). Errors from HTTP clients quote
//! the full request URL, so anything logged from them must go through [`redact_urls`].

const SCHEMES: [&str; 4] = ["https://", "http://", "wss://", "ws://"];

/// `scheme://host[:port]`, plus `/…` when anything followed the authority.
pub fn redact_url(url: &str) -> String {
    let Some(scheme) = SCHEMES.iter().find(|s| url.starts_with(**s)) else {
        return "<redacted url>".to_string();
    };
    let rest = &url[scheme.len()..];
    let end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    let authority = &rest[..end];
    let host = authority.rsplit_once('@').map_or(authority, |(_, h)| h);
    let tail = if end < rest.len() { "/…" } else { "" };
    format!("{scheme}{host}{tail}")
}

/// Redact every URL found in free text (error messages, debug output).
pub fn redact_urls(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut i = 0;
    while i < text.len() {
        let next = SCHEMES
            .iter()
            .filter_map(|s| text[i..].find(s).map(|p| i + p))
            .min();
        let Some(start) = next else {
            out.push_str(&text[i..]);
            break;
        };
        out.push_str(&text[i..start]);
        let end = text[start..]
            .find(|c: char| c.is_whitespace() || matches!(c, ')' | '"' | '\'' | '>' | ',' | ']'))
            .map_or(text.len(), |p| start + p);
        out.push_str(&redact_url(&text[start..end]));
        i = end;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redacts_path_query_and_userinfo() {
        assert_eq!(redact_url("https://mainnet.rpcpool.com/abc123"), "https://mainnet.rpcpool.com/…");
        assert_eq!(redact_url("https://x.helius-rpc.com/?api-key=k"), "https://x.helius-rpc.com/…");
        assert_eq!(redact_url("wss://u:p@ws.example.com:8900"), "wss://ws.example.com:8900");
        assert_eq!(redact_url("https://host"), "https://host");
    }

    #[test]
    fn redacts_urls_inside_error_text() {
        let e = "error sending request for url (https://mainnet.rpcpool.com/secret-key): timed out";
        assert_eq!(
            redact_urls(e),
            "error sending request for url (https://mainnet.rpcpool.com/…): timed out"
        );
        let dbg = r#"Reqwest(reqwest::Error { url: "https://a.b/key?x=1", source: hyper }) and http://c.d/e"#;
        let r = redact_urls(dbg);
        assert!(!r.contains("key") && !r.contains("/e"), "{r}");
        assert_eq!(redact_urls("no urls here"), "no urls here");
    }
}
