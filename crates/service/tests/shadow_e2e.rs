//! End to end: two rules raise the same `L1` on the same account update; one is in shadow
//! (`shadow = true` in its `[[rules]]` block). A pause reactor shaped like the Jup Lend pause
//! simulator (acts on `action = L1|L2` with `vaults`) must act on the live rule only, while
//! both alerts still reach the sinks, the shadowed one as `action = alert` with the would-be
//! action and the vaults kept.

use async_trait::async_trait;
use solana_pubkey::Pubkey;
use solana_sentinel::raw::{RawAccount, RawAccountParser};
use solana_sentinel::reactor::{ReactCtx, ReactorRule};
use solana_sentinel::{
    Alert, Deps, NoConfig, Program, ProgramSpec, RuleId, Severity, StreamCtx, StreamRule,
    StreamRules, Subscription,
};
use solana_sentinel_service::testing::{rules_from_toml, FakeUpdate, Harness};
use std::time::Duration;
use tokio::time::sleep;

const PROGRAM: Program = Program::new("fake");
const WATCHED: Pubkey = Pubkey::new_from_array([7; 32]);
const OWNER: Pubkey = Pubkey::new_from_array([9; 32]);

fn l1(id: RuleId, ctx: &StreamCtx) -> Alert {
    Alert::for_rule(id)
        .severity(Severity::Critical)
        .message(format!("{} drain", id.name))
        .meta("action", "L1")
        .meta("vaults", "3,7")
        .key([ctx.slot])
        .build()
}

struct LiveRule;

impl StreamRule for LiveRule {
    const ID: RuleId = RuleId::new(PROGRAM, "live_rule");
    type Config = NoConfig;
    type Event = RawAccount;

    fn subscription(_: &NoConfig) -> Subscription {
        Subscription::new().accounts([WATCHED])
    }

    fn build(_: NoConfig, _: &Deps) -> anyhow::Result<Self> {
        Ok(Self)
    }

    fn evaluate(&self, _: &RawAccount, ctx: &StreamCtx) -> Option<Alert> {
        Some(l1(Self::ID, ctx))
    }
}

struct ShadowRule;

impl StreamRule for ShadowRule {
    const ID: RuleId = RuleId::new(PROGRAM, "shadow_rule");
    type Config = NoConfig;
    type Event = RawAccount;

    fn subscription(_: &NoConfig) -> Subscription {
        Subscription::new().accounts([WATCHED])
    }

    fn build(_: NoConfig, _: &Deps) -> anyhow::Result<Self> {
        Ok(Self)
    }

    fn evaluate(&self, _: &RawAccount, ctx: &StreamCtx) -> Option<Alert> {
        Some(l1(Self::ID, ctx))
    }
}

/// Same acceptance as the Jup Lend pause simulator's `pause_request`.
struct PauseReactor;

#[async_trait]
impl ReactorRule for PauseReactor {
    const ID: RuleId = RuleId::new(PROGRAM, "pause_simulator");
    type Config = NoConfig;

    fn build(_: NoConfig, _: &Deps) -> anyhow::Result<Self> {
        Ok(Self)
    }

    fn accepts(&self, alert: &Alert) -> bool {
        matches!(
            alert.metadata.get("action").map(String::as_str),
            Some("L1" | "L2")
        ) && alert.metadata.get("vaults").is_some_and(|v| !v.is_empty())
    }

    async fn react(&self, alert: Alert, _: ReactCtx) -> Vec<Alert> {
        vec![Alert::for_rule(Self::ID)
            .message(format!("would pause for {}", alert.rule.name))
            .meta("source_rule", alert.rule.name)
            .build()]
    }
}

fn spec() -> ProgramSpec {
    ProgramSpec::new(PROGRAM)
        .account(
            RawAccountParser,
            StreamRules::new()
                .register::<LiveRule>()
                .register::<ShadowRule>(),
        )
        .reactor::<PauseReactor>()
}

const RULES: &str = r#"
[[rules]]
program = "fake"
name = "live_rule"
sinks = ["chat"]
shadow = false

[[rules]]
program = "fake"
name = "shadow_rule"
sinks = ["chat"]
shadow = true

[[rules]]
program = "fake"
name = "pause_simulator"
sinks = ["chat"]
"#;

#[tokio::test(start_paused = true)]
async fn shadowed_l1_never_reaches_the_pause_reactor() {
    let h = Harness::start(rules_from_toml(RULES).unwrap(), &[spec])
        .await
        .unwrap();
    let chat = h.sink("chat");
    h.send(FakeUpdate::account(WATCHED, OWNER, vec![1], 1))
        .await;
    sleep(Duration::from_secs(5)).await;

    let got = chat.incidents();
    let pauses: Vec<_> = got
        .iter()
        .filter(|a| a.rule == PauseReactor::ID)
        .map(|a| a.metadata["source_rule"].as_str())
        .collect();
    assert_eq!(pauses, ["live_rule"], "{got:#?}");

    let live = got.iter().find(|a| a.rule == LiveRule::ID).unwrap();
    assert_eq!(live.metadata["action"], "L1");
    assert!(!live.metadata.contains_key("shadow"));

    // Shadowed: delivered like any alert, same severity / message / vaults, no pause.
    let shadowed = got.iter().find(|a| a.rule == ShadowRule::ID).unwrap();
    assert_eq!(shadowed.severity, Severity::Critical);
    assert_eq!(shadowed.message, "shadow_rule drain");
    assert_eq!(shadowed.metadata["action"], "alert");
    assert_eq!(shadowed.metadata["shadow"], "true");
    assert_eq!(shadowed.metadata["would_action"], "L1");
    assert_eq!(shadowed.metadata["vaults"], "3,7");
}
