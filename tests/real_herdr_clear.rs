//! Opt-in proof that one backend's clear command actually rotates its session.
//!
//! This is the half of renew that documentation cannot settle. The clear command
//! is read from what a backend ships, but whether clearing produces a *new
//! backend-native session reference that reaches Kelpie through Herdr* is a
//! property of that backend's hook and Herdr's adapter for it. Renew's phase two
//! waits on exactly that signal, so a backend where it never arrives leaves an
//! incarnation cleared and holding no instructions.
//!
//! Run once per backend against a disposable session — see
//! `docs/real-herdr-test.md`:
//!
//! ```sh
//! KELPIE_TEST_AGENT_KIND=pi cargo test --test real_herdr_clear -- --ignored --nocapture
//! ```

use std::env;
use std::path::PathBuf;
use std::thread;
use std::time::{Duration, Instant};

use kelpie::domain::{
    InitialMessageIntent, InitialMessageKind, OperationOutcome, Parent, StartIntent,
};
use kelpie::herdr::HerdrClient;
use kelpie::slice::{Kelpie, RotationTiming, clear_protocol_for};
use kelpie::store::Store;

fn required(name: &str) -> String {
    env::var(name).unwrap_or_else(|_| panic!("{name} is required for the ignored real-Herdr test"))
}

/// Generous next to a clear, which rotates within seconds. This is the live
/// equivalent of the stall deadline, and reaching it is the finding.
const ROTATION_LIMIT: Duration = Duration::from_mins(1);

#[test]
#[ignore = "requires an explicitly disposable live Herdr shell pane"]
#[allow(clippy::too_many_lines)]
fn real_herdr_clear_rotates_the_backend_native_session() {
    let socket = PathBuf::from(required("KELPIE_TEST_HERDR_SOCKET"));
    let pane_id = required("KELPIE_TEST_PANE_ID");
    let terminal_id = required("KELPIE_TEST_TERMINAL_ID");
    let working_directory = required("KELPIE_TEST_CWD");
    let public_name = required("KELPIE_TEST_AGENT_NAME");
    let backend_kind = required("KELPIE_TEST_AGENT_KIND");
    let backend_args = env::var("KELPIE_TEST_AGENT_ARGS_JSON")
        .map(|value| serde_json::from_str(&value).expect("KELPIE_TEST_AGENT_ARGS_JSON is JSON"))
        .unwrap_or_default();

    // The shipped table, not a copy of it: a test with its own mapping would
    // pass while the command renew actually sends is wrong.
    let protocol = clear_protocol_for(&backend_kind)
        .unwrap_or_else(|| panic!("{backend_kind} has no verified clear protocol to test"));
    let clear_command = protocol.command;
    println!(
        "backend {backend_kind}: clear command is {clear_command}, rotation expected {:?}",
        protocol.rotation
    );

    let state = tempfile::tempdir().expect("state directory");
    let store = Store::open(state.path().join("kelpie.sqlite3")).expect("durable store");
    let probe = HerdrClient::new(&socket, Duration::from_secs(5));
    let mut kelpie = Kelpie::new(store, HerdrClient::new(&socket, Duration::from_secs(5)));
    let started = kelpie
        .launch(&StartIntent {
            public_name,
            logical_agent_id: None,
            parent: Parent::Parentless,
            herdr_session: "real-clear-integration".into(),
            pane_id: pane_id.clone(),
            expected_terminal_id: terminal_id,
            backend_kind: backend_kind.clone(),
            backend_args,
            initial_message: InitialMessageIntent {
                sender: None,
                kind: InitialMessageKind::Tell,
                body: "say ready and wait".into(),
            },
            working_directory,
            idempotency_key: format!("real-clear-start-{}", uuid::Uuid::now_v7()),
            readiness_timeout_ms: 60_000,
            keep_open: true,
            supersedes: None,
            requested_model: None,
            requested_provider: None,
            requested_effort: None,
        })
        .expect("real start reaches exact readiness");
    assert_eq!(started.start_outcome, OperationOutcome::Succeeded);

    // Renew only ever clears an agent that has just answered a prepare ask, so
    // warm the conversation first. A backend that assigns its session lazily
    // reports nothing until something is sent, and clearing a pane that never
    // held a conversation would not be the situation renew faces.
    probe
        .connect()
        .expect("connect for the warm-up")
        .prompt_agent(
            &format!("real-clear-warmup-{}", uuid::Uuid::now_v7()),
            &pane_id,
            "reply with the single word ready",
        )
        .expect("warm-up prompt is accepted");

    // A backend that never reports a session reference at all can never have a
    // clear proven, whatever its clear command is.
    let before = wait_for_session(&probe, &pane_id).unwrap_or_else(|| {
        panic!(
            "{backend_kind} reported no backend-native session within {}s, so renew could never \
             prove a clear landed for it",
            ROTATION_LIMIT.as_secs()
        )
    });
    println!("pre-clear session: {before}");

    probe
        .connect()
        .expect("connect for the clear")
        .prompt_agent(
            &format!("real-clear-{}", uuid::Uuid::now_v7()),
            &pane_id,
            clear_command,
        )
        .expect("clear command is accepted");

    match protocol.rotation {
        // The rotation is the precondition of the injection, so it must arrive
        // with nothing further sent.
        RotationTiming::OnClear => {
            let rotated = wait_for_rotation(&probe, &pane_id, &before);
            let latest = rotated.unwrap_or_else(|| {
                panic!(
                    "{backend_kind} is recorded as rotating on the clear, and {clear_command} \
                     did not rotate its session within {}s. If the context was in fact cleared, \
                     this backend rotates on its next prompt instead and its table entry is \
                     wrong; renew would clear it and then wait forever.",
                    ROTATION_LIMIT.as_secs()
                )
            });
            println!("post-clear session: {latest}");
        }
        // The injection is what allocates the replacement conversation, so
        // nothing may rotate until the resume prompt is sent — and it must
        // rotate once it is.
        RotationTiming::OnNextPrompt => {
            thread::sleep(Duration::from_secs(5));
            let early = probe
                .agent(
                    &format!("real-clear-early-{}", uuid::Uuid::now_v7()),
                    &pane_id,
                )
                .expect("probe the pane")
                .agent_session
                .map(|value| value.to_string());
            assert_eq!(
                early.as_ref(),
                Some(&before),
                "{backend_kind} is recorded as rotating only on its next prompt, but it rotated \
                 on the clear alone. It should be moved to OnClear, which is the stronger \
                 barrier."
            );
            probe
                .connect()
                .expect("connect for the injection")
                .prompt_agent(
                    &format!("real-clear-inject-{}", uuid::Uuid::now_v7()),
                    &pane_id,
                    "you are a continuation; reply with the single word resumed",
                )
                .expect("resume prompt is accepted");
            let latest = wait_for_rotation(&probe, &pane_id, &before).unwrap_or_else(|| {
                panic!(
                    "{backend_kind}: the resume prompt was accepted and no new conversation \
                     appeared within {}s, so the clear never landed and that prompt went into \
                     the context it was meant to replace.",
                    ROTATION_LIMIT.as_secs()
                )
            });
            println!("post-injection session: {latest}");
        }
    }
    println!("{backend_kind}: renew's barrier is observable for this backend");
}

/// Poll until the session reference differs from `before`.
fn wait_for_rotation(probe: &HerdrClient, pane_id: &str, before: &str) -> Option<String> {
    let deadline = Instant::now() + ROTATION_LIMIT;
    while Instant::now() < deadline {
        thread::sleep(Duration::from_millis(500));
        let observed = probe
            .agent(
                &format!("real-clear-probe-{}", uuid::Uuid::now_v7()),
                pane_id,
            )
            .expect("probe the pane")
            .agent_session
            .map(|value| value.to_string());
        if let Some(current) = observed
            && current != before
        {
            return Some(current);
        }
    }
    None
}

/// Wait for the backend to report any session reference at all.
///
/// A backend writes its session on start through its own hook, so readiness and
/// a reported session are not the same instant.
fn wait_for_session(probe: &HerdrClient, pane_id: &str) -> Option<String> {
    let deadline = Instant::now() + ROTATION_LIMIT;
    while Instant::now() < deadline {
        let observed = probe
            .agent(&format!("real-clear-pre-{}", uuid::Uuid::now_v7()), pane_id)
            .expect("probe the pane")
            .agent_session
            .map(|value| value.to_string());
        if let Some(session) = observed {
            return Some(session);
        }
        thread::sleep(Duration::from_millis(500));
    }
    None
}
