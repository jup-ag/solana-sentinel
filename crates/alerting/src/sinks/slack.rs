use crate::sinks::{AlertSink, AlertSinkError};
use async_trait::async_trait;
use chrono::SecondsFormat;
use domain::alert::{Alert, Severity};
use domain::sinks::SinkId;
use reqwest::header::{HeaderMap, RETRY_AFTER};
use reqwest::{Client as HttpClient, StatusCode};
use serde::Serialize;
use std::time::Duration;
use tracing::warn;

const SLACK_POST_MESSAGE_URL: &str = "https://slack.com/api/chat.postMessage";

// Slack Block Kit hard limits. Exceeding any of these makes chat.postMessage
// reject the *entire* message, so we clamp defensively.
const SECTION_TEXT_LIMIT: usize = 3000;
const HEADER_TEXT_LIMIT: usize = 150;
const TABLE_ROW_LIMIT: usize = 100;

// Headroom left when clamping the message before styling, so the markers added
// by highlighting can't push the section past SECTION_TEXT_LIMIT.
const STYLE_MARGIN: usize = 256;

// Table cells get roughly half the message width, and `is_wrapped` only breaks
// at whitespace — a 44-char base58 key contains none, so there is no break
// opportunity and the client clips the tail. Elide keys longer than this inside
// cells; the section text has full width and keeps them verbatim.
const CELL_PUBKEY_ELIDE_OVER: usize = 24;
const PUBKEY_HEAD: usize = 6;
const PUBKEY_TAIL: usize = 6;

pub struct SlackSink {
    pub id: SinkId,
    pub http_client: HttpClient,
    pub bot_token: String,
    pub channel: String,
}

impl SlackSink {
    pub fn build(
        id: SinkId,
        bot_token: String,
        channel: String,
        timeout_seconds: u64,
    ) -> Result<Self, AlertSinkError> {
        let http_client = HttpClient::builder()
            .timeout(Duration::from_secs(timeout_seconds))
            .build()
            .map_err(|e| AlertSinkError::SlackSinkError(Box::new(e)))?;

        Ok(Self {
            id,
            http_client,
            bot_token,
            channel,
        })
    }

    async fn post(&self, alert: &Alert) -> Result<(), AlertSinkError> {
        const MAX_ATTEMPTS: u32 = 3;
        const BASE_BACKOFF: Duration = Duration::from_millis(100);

        let payload = SlackPayload::from_alert(&self.channel, alert);
        let mut attempt = 0;

        loop {
            attempt += 1;

            let result = async {
                let resp = self
                    .http_client
                    .post(SLACK_POST_MESSAGE_URL)
                    .bearer_auth(&self.bot_token)
                    .json(&payload)
                    .send()
                    .await
                    .map_err(|e| (AlertSinkError::SlackSinkError(Box::new(e)), true, None))?;

                // HTTP-level failures: 5xx and 429 are retryable and won't carry a
                // valid SlackApiResponse body, so handle them before parsing.
                let status = resp.status();
                if status.is_server_error() || status == StatusCode::TOO_MANY_REQUESTS {
                    let retry_after = parse_retry_after(resp.headers());
                    return Err((
                        AlertSinkError::SlackSinkError(format!("slack http {status}").into()),
                        true,
                        retry_after,
                    ));
                }

                // chat.postMessage returns HTTP 200 even on logical failure; the real
                // status is in the JSON body `ok` field. By this point the message has
                // already been accepted by Slack, so a body-read failure is NOT retried
                // — retrying would risk double-posting a non-idempotent message.
                let body: SlackApiResponse = resp
                    .json()
                    .await
                    .map_err(|e| (AlertSinkError::SlackSinkError(Box::new(e)), false, None))?;

                if !body.ok {
                    let err = body
                        .error
                        .unwrap_or_else(|| "unknown slack error".to_string());
                    let retryable = matches!(
                        err.as_str(),
                        "ratelimited" | "internal_error" | "service_unavailable" | "fatal_error"
                    );
                    return Err((AlertSinkError::SlackSinkError(err.into()), retryable, None));
                }

                Ok(())
            }
            .await;

            match result {
                Ok(()) => return Ok(()),
                Err((e, retryable, retry_after)) => {
                    if !retryable || attempt >= MAX_ATTEMPTS {
                        return Err(e);
                    }
                    let backoff =
                        retry_after.unwrap_or_else(|| BASE_BACKOFF * 2u32.pow(attempt - 1));
                    warn!(
                        attempt,
                        max_attempts = MAX_ATTEMPTS,
                        error = %e,
                        backoff_ms = backoff.as_millis(),
                        "Slack post failed, retrying"
                    );
                    tokio::time::sleep(backoff).await;
                }
            }
        }
    }
}

/// Parse the `Retry-After` header (delta-seconds form, per Slack's rate limiting).
fn parse_retry_after(headers: &HeaderMap) -> Option<Duration> {
    headers
        .get(RETRY_AFTER)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.trim().parse::<u64>().ok())
        .map(Duration::from_secs)
}

#[async_trait]
impl AlertSink for SlackSink {
    fn id(&self) -> &SinkId {
        &self.id
    }

    async fn send(&self, alert: &Alert) -> Result<(), AlertSinkError> {
        self.post(alert).await
    }

    async fn send_batch(&self, alerts: &[Alert]) -> Result<(), AlertSinkError> {
        // No grouping: one message per alert.
        for alert in alerts {
            self.post(alert).await?;
        }
        Ok(())
    }
}

#[derive(serde::Deserialize)]
struct SlackApiResponse {
    ok: bool,
    #[serde(default)]
    error: Option<String>,
}

// All content lives inside a single attachment: the colored severity bar is an
// attachment-only feature and only spans the height of the attachment's own
// content, so an empty attachment renders no bar at all. Top-level `text` stays
// outside it as the notification fallback and renders above the bar.
#[derive(Serialize)]
struct SlackPayload<'a> {
    channel: &'a str,
    attachments: Vec<SlackAttachment>,
}

#[derive(Serialize)]
struct SlackAttachment {
    color: &'static str,
    blocks: Vec<SlackBlock>,
}

#[derive(Serialize)]
#[allow(dead_code)]
#[serde(tag = "type", rename_all = "snake_case")]
enum SlackBlock {
    Header {
        text: PlainText,
    },
    Section {
        text: Mrkdwn,
    },
    Table {
        rows: Vec<Vec<TableCell>>,
        #[serde(skip_serializing_if = "Vec::is_empty")]
        column_settings: Vec<ColumnSetting>,
    },
    Context {
        elements: Vec<Mrkdwn>,
    },
    Actions {
        elements: Vec<ButtonElement>,
    },
    Divider,
}

#[derive(Serialize)]
struct PlainText {
    #[serde(rename = "type")]
    kind: &'static str, // "plain_text"
    text: String,
    emoji: bool,
}

impl PlainText {
    fn new(text: impl Into<String>) -> Self {
        Self {
            kind: "plain_text",
            text: text.into(),
            emoji: true,
        }
    }
}

#[derive(Serialize)]
struct Mrkdwn {
    #[serde(rename = "type")]
    kind: &'static str, // "mrkdwn"
    text: String,
}

impl Mrkdwn {
    fn new(text: impl Into<String>) -> Self {
        Self {
            kind: "mrkdwn",
            text: text.into(),
        }
    }
}

#[derive(Serialize)]
struct ColumnSetting {
    #[serde(skip_serializing_if = "Option::is_none")]
    align: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    is_wrapped: Option<bool>,
}

// Table cells: raw_text for plain labels, rich_text for styled values.
// Numeric values render bold (short, punchy); pubkeys and signatures render as
// code/monospace (aligns chars, flags "this is an identifier", and gives
// click-to-copy on desktop clients). A cell can mix both, e.g. a compound
// value like `<pubkey>:8819/1, <pubkey>:8539/1`.
#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum TableCell {
    RawText { text: String },
    RichText { elements: Vec<RichTextSection> },
}

impl TableCell {
    fn raw(text: impl Into<String>) -> Self {
        Self::RawText { text: text.into() }
    }

    fn styled(text: impl Into<String>, style: TextStyle) -> Self {
        Self::RichText {
            elements: vec![RichTextSection {
                kind: "rich_text_section",
                elements: vec![RichTextElement::Text {
                    text: text.into(),
                    style: Some(style),
                }],
            }],
        }
    }

    /// Wrap rich-text elements as a single-section cell.
    fn rich(elements: Vec<RichTextElement>) -> Self {
        Self::RichText {
            elements: vec![RichTextSection {
                kind: "rich_text_section",
                elements,
            }],
        }
    }

    /// Cell holding a single pubkey or signature and nothing else.
    fn identifier(key: &str) -> Self {
        Self::rich(vec![pubkey_or_tx_element(key)])
    }

    /// Cell for `text` with every embedded pubkey/signature styled as an
    /// identifier. The spans between them go to `push_plain`, which is what
    /// separates a label (left alone) from a value (numbers bolded). Falls back
    /// to a raw_text cell when nothing ended up styled.
    fn scanned(text: &str, push_plain: fn(&str, &mut Vec<RichTextElement>)) -> Self {
        let mut elements = Vec::new();
        for segment in scan_pubkeys(text) {
            match segment {
                Segment::Pubkey(p) => elements.push(pubkey_or_tx_element(p)),
                Segment::Plain(p) => push_plain(p, &mut elements),
            }
        }

        if matches!(
            elements.as_slice(),
            [] | [RichTextElement::Text { style: None, .. }]
        ) {
            return Self::raw(text.to_string());
        }

        Self::rich(elements)
    }

    /// Label cell. Some rules key their metadata by pubkey (per-custodian
    /// reserves, per-account balances), so a label can itself be an identifier:
    /// render it as one and never run it through `humanize_key`, which
    /// capitalizes the first character and would silently corrupt a
    /// case-sensitive base58 key into a different key.
    fn for_label(k: &str) -> Self {
        let trimmed = k.trim();

        // Whole label is an identifier.
        if is_pubkey_or_tx_like(trimmed) {
            return Self::identifier(trimmed);
        }

        // Or a prefix plus one, e.g. `custodian:4APzEJ…twATk77`.
        Self::scanned(k, push_plain_text)
    }

    /// Cell styled according to the value's shape: every embedded
    /// pubkey/signature renders as code (or elided + linked), and every numeric
    /// token renders bold — including inside a compound value such as
    /// `mint : <pubkey> reserves : 1.6M`, where only part of the cell is a
    /// number. Mirrors `highlight_tokens`, which does the same for the mrkdwn
    /// section, so a value reads identically in either place.
    fn for_value(v: &str) -> Self {
        let trimmed = v.trim();

        if is_numeric_like(trimmed) {
            return Self::styled(trimmed, TextStyle::bold());
        }
        if is_pubkey_or_tx_like(trimmed) {
            return Self::identifier(trimmed);
        }

        Self::scanned(v, push_number_elements)
    }
}

/// Plain-span handler for labels: no styling, text passes through as-is.
fn push_plain_text(text: &str, out: &mut Vec<RichTextElement>) {
    out.push(RichTextElement::Text {
        text: text.to_string(),
        style: None,
    });
}

/// Render a base58 key for a table cell: verbatim monospace if it fits the
/// column, otherwise elided and linked to the explorer so the full key stays
/// reachable despite not being displayed.
fn pubkey_or_tx_element(key: &str) -> RichTextElement {
    if key.len() <= CELL_PUBKEY_ELIDE_OVER {
        return RichTextElement::Text {
            text: key.to_string(),
            style: Some(TextStyle::code()),
        };
    }

    // Signatures are 86-88 chars, addresses 32-44 (see `is_pubkey_or_tx_like`).
    let url = if key.len() >= 86 {
        explorer_tx_url(key)
    } else {
        explorer_address_url(key)
    };

    RichTextElement::Link {
        url,
        text: elide_middle(key),
    }
}

/// Append a plain (non-pubkey) span to a cell as rich-text elements, with the
/// numeric tokens bolded. Adjacent non-numeric stretches are coalesced so a
/// long prose value doesn't turn into dozens of one-word elements.
fn push_number_elements(text: &str, out: &mut Vec<RichTextElement>) {
    let mut plain = String::new();

    scan_numbers(text, |chunk, numeric| {
        if !numeric {
            plain.push_str(chunk);
            return;
        }
        if !plain.is_empty() {
            out.push(RichTextElement::Text {
                text: std::mem::take(&mut plain),
                style: None,
            });
        }
        out.push(RichTextElement::Text {
            text: chunk.to_string(),
            style: Some(TextStyle::bold()),
        });
    });

    if !plain.is_empty() {
        out.push(RichTextElement::Text {
            text: plain,
            style: None,
        });
    }
}

/// Keep the head and tail, which is what a human actually eyeball-matches
/// against. Base58 is ASCII, so byte slicing is char-boundary safe here.
fn elide_middle(s: &str) -> String {
    if s.len() <= PUBKEY_HEAD + PUBKEY_TAIL {
        return s.to_string();
    }
    format!(
        "{}\u{2026}{}",
        &s[..PUBKEY_HEAD],
        &s[s.len() - PUBKEY_TAIL..]
    )
}

#[derive(Serialize)]
struct RichTextSection {
    #[serde(rename = "type")]
    kind: &'static str, // "rich_text_section"
    elements: Vec<RichTextElement>,
}

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
#[allow(dead_code)]
enum RichTextElement {
    Text {
        text: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        style: Option<TextStyle>,
    },
    Link {
        url: String,
        text: String,
    },
}

// Only serialize the flags that are set, so a bold cell emits {"bold":true}
// and a code cell emits {"code":true} rather than both every time.
#[derive(Serialize)]
struct TextStyle {
    #[serde(skip_serializing_if = "Option::is_none")]
    bold: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    code: Option<bool>,
}

impl TextStyle {
    fn bold() -> Self {
        Self {
            bold: Some(true),
            code: None,
        }
    }

    fn code() -> Self {
        Self {
            bold: None,
            code: Some(true),
        }
    }
}

#[derive(Serialize)]
struct ButtonElement {
    #[serde(rename = "type")]
    kind: &'static str, // "button"
    text: PlainText,
    #[serde(skip_serializing_if = "Option::is_none")]
    url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    style: Option<&'static str>, // "primary" | "danger"
}

impl<'a> SlackPayload<'a> {
    fn from_alert(channel: &'a str, a: &Alert) -> Self {
        let header = truncate(&format!("Rule : {}", a.rule.name), HEADER_TEXT_LIMIT);
        let emoji = severity_emoji(a.severity);

        // Clamp before styling so the clamp can't slice a code fence in half.
        let message = highlight_tokens(&truncate(
            &a.message,
            SECTION_TEXT_LIMIT.saturating_sub(STYLE_MARGIN),
        ));

        let mut blocks = vec![
            SlackBlock::Header {
                text: PlainText::new(header),
            },
            SlackBlock::Section {
                text: Mrkdwn::new(truncate(&format!("{emoji} {message}"), SECTION_TEXT_LIMIT)),
            },
        ];

        if !a.metadata.is_empty() {
            blocks.push(metadata_table(a));
        }

        let actions = build_actions(a);
        if !actions.is_empty() {
            blocks.push(SlackBlock::Actions { elements: actions });
        }

        blocks.push(SlackBlock::Context {
            elements: vec![Mrkdwn::new(
                a.fired_at.to_rfc3339_opts(SecondsFormat::Secs, true),
            )],
        });

        Self {
            channel,
            attachments: vec![SlackAttachment {
                color: severity_color(a.severity),
                blocks,
            }],
        }
    }
}

/// Build a two-column (label / value) table block from an alert's metadata.
///
/// Slack renders a table's *first* row as a bold header and offers no flag to
/// disable it, so we emit an explicit one. Without it the first metadata entry
/// is promoted to the header and rendered bold — and if `metadata` is a
/// `HashMap` that is a different, arbitrary entry on every alert.
///
/// Slack caps tables at 100 rows; the header consumes one of them.
fn metadata_table(a: &Alert) -> SlackBlock {
    let mut rows: Vec<Vec<TableCell>> = Vec::with_capacity(a.metadata.len() + 1);

    rows.push(vec![TableCell::raw("Field"), TableCell::raw("Value")]);
    rows.extend(
        a.metadata
            .iter()
            .take(TABLE_ROW_LIMIT.saturating_sub(1))
            // skip tx_sig as it is already linked in the action buttons
            .filter(|(k, _)| *k != "tx_sig")
            .map(|(k, v)| vec![TableCell::for_label(k), TableCell::for_value(v)]),
    );

    SlackBlock::Table {
        rows,
        column_settings: vec![
            ColumnSetting {
                align: Some("left"),
                is_wrapped: Some(false),
            },
            ColumnSetting {
                align: Some("left"),
                is_wrapped: Some(true),
            },
        ],
    }
}

const SOLSCAN_BASE: &str = "https://solscan.io";

/// Solscan calls this path `/account/`, not `/address/` — a `/address/` URL
/// 404s there.
fn explorer_address_url(addr: &str) -> String {
    format!("{SOLSCAN_BASE}/account/{addr}")
}

fn explorer_tx_url(sig: &str) -> String {
    format!("{SOLSCAN_BASE}/tx/{sig}")
}

/// Build action buttons from known metadata keys, if present.
/// Adjust the keys to match what your `Alert` actually carries.
fn build_actions(a: &Alert) -> Vec<ButtonElement> {
    let mut out = Vec::new();

    if let Some(addr) = a.metadata.get("wallet_address") {
        out.push(ButtonElement {
            kind: "button",
            text: PlainText::new(":file_folder: Wallet"),
            url: Some(explorer_address_url(addr)),
            style: None,
        });
    }

    // Pyth / oracle alerts carry the feed account under "oracle".
    if let Some(oracle) = a.metadata.get("oracle") {
        out.push(ButtonElement {
            kind: "button",
            text: PlainText::new(":satellite_antenna: Oracle"),
            url: Some(explorer_address_url(oracle)),
            style: None,
        });
    }

    if let Some(sig) = a.metadata.get("tx_sig") {
        out.push(ButtonElement {
            kind: "button",
            text: PlainText::new(":link: Transaction"),
            url: Some(explorer_tx_url(sig)),
            style: None,
        });
    }

    out
}

/// A span of a message: either an embedded pubkey/signature or everything else.
enum Segment<'a> {
    Plain(&'a str),
    Pubkey(&'a str),
}

/// Split a string into base58 pubkey/signature spans and the plain text around
/// them. Unlike whole-token matching, this catches keys embedded in compound
/// values such as `<pubkey>:8819/1` or comma-joined lists.
fn scan_pubkeys(s: &str) -> Vec<Segment<'_>> {
    let bytes = s.as_bytes();
    let mut out = Vec::new();
    let mut plain_start = 0;
    let mut i = 0;

    // Base58 characters are ASCII, so every run boundary is a char boundary and
    // the slices below are always valid UTF-8.
    while i < bytes.len() {
        if !is_base58_byte(bytes[i]) {
            i += 1;
            continue;
        }

        let start = i;
        while i < bytes.len() && is_base58_byte(bytes[i]) {
            i += 1;
        }

        if is_pubkey_or_tx_like(&s[start..i]) {
            if start > plain_start {
                out.push(Segment::Plain(&s[plain_start..start]));
            }
            out.push(Segment::Pubkey(&s[start..i]));
            plain_start = i;
        }
    }

    if plain_start < s.len() {
        out.push(Segment::Plain(&s[plain_start..]));
    }

    out
}

/// Re-style a message for Slack mrkdwn: pubkeys and signatures become
/// `` `code` ``, numbers become `*bold*`. Original whitespace (including
/// newlines and runs of spaces) is preserved verbatim.
fn highlight_tokens(msg: &str) -> String {
    let mut out = String::with_capacity(msg.len() + 16);

    for segment in scan_pubkeys(msg) {
        match segment {
            Segment::Pubkey(p) => {
                out.push('`');
                out.push_str(p);
                out.push('`');
            }
            Segment::Plain(p) => out.push_str(&highlight_numbers(p)),
        }
    }

    out
}

/// Bold the numeric tokens in a plain span, leaving everything else untouched.
fn highlight_numbers(text: &str) -> String {
    let mut out = String::with_capacity(text.len() + 8);

    scan_numbers(text, |chunk, numeric| {
        if numeric {
            out.push('*');
            out.push_str(chunk);
            out.push('*');
        } else {
            out.push_str(chunk);
        }
    });

    out
}

/// Walk a plain span and hand every stretch to `emit`, with `numeric` set for
/// the ones that should be highlighted. Concatenating the chunks reproduces the
/// input verbatim, whitespace included, which is what lets the mrkdwn and
/// rich-text renderers share one definition of "this is a number".
fn scan_numbers<'a>(text: &'a str, mut emit: impl FnMut(&'a str, bool)) {
    let mut rest = text;

    while !rest.is_empty() {
        // Leading whitespace is never part of a token.
        let ws_end = rest
            .find(|c: char| !c.is_whitespace())
            .unwrap_or(rest.len());
        if ws_end > 0 {
            emit(&rest[..ws_end], false);
            rest = &rest[ws_end..];
            if rest.is_empty() {
                break;
            }
        }

        // Take the next run of non-whitespace as one token.
        let tok_end = rest.find(char::is_whitespace).unwrap_or(rest.len());
        let tok = &rest[..tok_end];
        rest = &rest[tok_end..];

        // Split off leading and trailing punctuation and emit it unstyled, so
        // "(8.05%)," still matches on the "8.05%" core and the brackets stay
        // outside the bold markers.
        let lead_end = tok
            .find(|c: char| !matches!(c, '(' | '[' | '{'))
            .unwrap_or(tok.len());
        let prefix = &tok[..lead_end];
        let core_and_suffix = &tok[lead_end..];
        let core = core_and_suffix.trim_end_matches(&[',', '.', ';', ':', ')', ']', '}'][..]);
        let suffix = &core_and_suffix[core.len()..];

        if !core.is_empty() && is_numeric_like(core) {
            if !prefix.is_empty() {
                emit(prefix, false);
            }
            emit(core, true);
            if !suffix.is_empty() {
                emit(suffix, false);
            }
        } else {
            emit(tok, false);
        }
    }
}

/// Base58 (Bitcoin alphabet) omits `0`, `O`, `I` and `l`. Two useful
/// consequences: a genuine pubkey can never be split by those characters, and
/// ordinary prose — which is full of them — gets broken into runs far too short
/// to reach the length floor in `is_pubkey_or_tx_like`.
fn is_base58_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() && !matches!(b, b'0' | b'O' | b'I' | b'l')
}

/// Pubkeys are 32-44 base58 chars, signatures 86-88. Requires at least one
/// letter so a long digit run (a slot, a lamport amount) isn't misclassified.
fn is_pubkey_or_tx_like(s: &str) -> bool {
    matches!(s.len(), 32..=44 | 86..=88)
        && s.bytes().all(is_base58_byte)
        && s.bytes().any(|b| b.is_ascii_alphabetic())
}

/// Numbers, prices, and percentages: must contain a digit and consist only of
/// digits and the usual numeric punctuation (so "BTC/USD" is NOT caught, but
/// "-8.0590%", "61600.45", "1.0000%", "67000", "1.6M" are).
fn is_numeric_like(v: &str) -> bool {
    if is_pubkey_or_tx_like(v) {
        return false;
    }
    let v = strip_scale_suffix(v.trim());
    if v.is_empty() {
        return false;
    }
    let has_digit = v.chars().any(|c| c.is_ascii_digit());
    let only_numeric = v
        .chars()
        .all(|c| c.is_ascii_digit() || matches!(c, '.' | ',' | '-' | '+' | '%' | '_'));
    has_digit && only_numeric
}

/// Drop the magnitude suffix the compact amount formatter appends, so "1.6M"
/// and "53.14M" are recognized as numbers. A digit is required immediately
/// before the suffix, which keeps a bare "M" or a word ending in one out.
fn strip_scale_suffix(v: &str) -> &str {
    let head = match v.as_bytes().last().copied() {
        Some(b'K' | b'M' | b'B' | b'T' | b'k' | b'm' | b'b' | b't') => &v[..v.len() - 1],
        _ => return v,
    };

    if head.as_bytes().last().is_some_and(u8::is_ascii_digit) {
        head
    } else {
        v
    }
}

/// Clamp to a char-boundary-safe length, appending an ellipsis if truncated.
fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        return s.to_string();
    }
    let budget = max.saturating_sub(1); // room for the ellipsis
    let mut end = budget;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}\u{2026}", &s[..end])
}

fn severity_emoji(s: Severity) -> &'static str {
    match s {
        Severity::Info => ":information_source:",
        Severity::Warn => ":warning:",
        Severity::Critical => ":rotating_light:",
    }
}

fn severity_color(s: Severity) -> &'static str {
    match s {
        Severity::Info => "#36a64f",     // green
        Severity::Warn => "#daa038",     // amber
        Severity::Critical => "#d50200", // red
    }
}
