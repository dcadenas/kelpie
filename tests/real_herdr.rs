//! Opt-in lifecycle coverage against a real, disposable Herdr session.

use std::env;
use std::path::PathBuf;
use std::time::Duration;

use kelpie::domain::{
    DeliveryOutcome, InitialMessageIntent, InitialMessageKind, ObligationState, OperationOutcome,
    Parent, ReplyDisposition, StartIntent,
};
use kelpie::herdr::HerdrClient;
use kelpie::slice::Kelpie;
use kelpie::store::Store;

fn required(name: &str) -> String {
    env::var(name).unwrap_or_else(|_| panic!("{name} is required for the ignored real-Herdr test"))
}

#[test]
#[ignore = "requires an explicitly disposable live Herdr shell pane"]
fn real_herdr_start_ask_and_correlated_reply() {
    let socket = PathBuf::from(required("KELPIE_TEST_HERDR_SOCKET"));
    let pane_id = required("KELPIE_TEST_PANE_ID");
    let terminal_id = required("KELPIE_TEST_TERMINAL_ID");
    let working_directory = required("KELPIE_TEST_CWD");
    let public_name = required("KELPIE_TEST_AGENT_NAME");
    let backend_kind = env::var("KELPIE_TEST_AGENT_KIND").unwrap_or_else(|_| "codex".into());
    let backend_args = env::var("KELPIE_TEST_AGENT_ARGS_JSON")
        .map(|value| serde_json::from_str(&value).expect("KELPIE_TEST_AGENT_ARGS_JSON is JSON"))
        .unwrap_or_default();
    let state = tempfile::tempdir().expect("state directory");
    let store = Store::open(state.path().join("kelpie.sqlite3")).expect("durable store");
    let herdr = HerdrClient::new(socket, Duration::from_secs(5));
    let mut kelpie = Kelpie::new(store, herdr);
    let started = kelpie
        .launch(&StartIntent {
            public_name,
            logical_agent_id: None,
            parent: Parent::Parentless,
            herdr_session: "real-integration".into(),
            pane_id,
            expected_terminal_id: terminal_id,
            backend_kind,
            backend_args,
            initial_message: InitialMessageIntent {
                sender: None,
                kind: InitialMessageKind::Tell,
                body: "real Herdr integration fixture".into(),
            },
            working_directory,
            idempotency_key: format!("real-start-{}", uuid::Uuid::now_v7()),
            readiness_timeout_ms: 30_000,
            keep_open: true,
            supersedes: None,
            requested_model: None,
            requested_provider: None,
            requested_effort: None,
        })
        .expect("real start reaches exact readiness");
    assert_eq!(started.start_outcome, OperationOutcome::Succeeded);
    assert_eq!(started.initial_message_outcome, DeliveryOutcome::Accepted);
    let ask = kelpie
        .ask(
            started.logical_agent_id,
            started.logical_agent_id,
            started.incarnation_id,
            "Reply through Kelpie is tested locally; no model response is inferred here.",
            &format!("real-ask-{}", uuid::Uuid::now_v7()),
            None,
            None,
        )
        .expect("real Herdr prompt accepts delivery");
    kelpie
        .reply(
            ask.message_id,
            "explicit integration completion",
            ReplyDisposition::Final,
            &format!("real-final-{}", uuid::Uuid::now_v7()),
        )
        .expect("correlated final reply");
    assert_eq!(
        kelpie
            .store_mut()
            .obligation_state(ask.message_id)
            .expect("obligation"),
        ObligationState::Resolved
    );
}
