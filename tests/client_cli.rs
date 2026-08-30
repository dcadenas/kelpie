//! Typed `kelpie` CLI: bodies stay bytes and ordinary commands do not need jq.

use std::fs::{self, File};
use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixListener;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use kelpie::daemon::Daemon;
use kelpie::domain::{InitialMessageKind, Parent, StartIntent};
use kelpie::envelope;
use kelpie::herdr::HerdrClient;
use kelpie::slice::Kelpie;
use kelpie::store::{AdoptEvidence, Store};
use serde_json::{Value, json};

#[allow(clippy::too_many_lines)]
fn spawn_named_pair(
    directory: &Path,
) -> (
    PathBuf,
    thread::JoinHandle<()>,
    thread::JoinHandle<()>,
    mpsc::Receiver<String>,
) {
    let database = directory.join("kelpie.sqlite3");
    let kelpie_socket = directory.join("kelpie.sock");
    let herdr_socket = directory.join("herdr.sock");
    let listener = UnixListener::bind(&herdr_socket).expect("herdr");
    let (prompt_tx, prompt_rx) = mpsc::channel();
    let herdr = thread::spawn(move || {
        let exchanges = [
            "ping",
            "session.snapshot",
            "ping",
            "session.snapshot",
            "agent.prompt",
        ];
        for method in exchanges {
            let (mut stream, _) = listener.accept().expect("accept");
            let mut line = String::new();
            BufReader::new(stream.try_clone().expect("clone"))
                .read_line(&mut line)
                .expect("read");
            let request: Value = serde_json::from_str(&line).expect("json");
            assert_eq!(request["method"], method);
            let result = match method {
                "ping" => serde_json::json!({"type":"pong","version":"test","protocol":20}),
                "session.snapshot" => serde_json::json!({
                    "type":"session_snapshot",
                    "snapshot":{
                        "protocol":20,
                        "panes":[
                            {"pane_id":"w1:p1","terminal_id":"term-a","cwd":"/tmp/a"},
                            {"pane_id":"w1:p2","terminal_id":"term-b","cwd":"/tmp/b"}
                        ],
                        "agents":[
                            {"terminal_id":"term-a","pane_id":"w1:p1","name":"alice","agent":"grok","launch_pending":false},
                            {"terminal_id":"term-b","pane_id":"w1:p2","name":"bob","agent":"codex","launch_pending":false}
                        ]
                    }
                }),
                _ => {
                    prompt_tx
                        .send(
                            request["params"]["text"]
                                .as_str()
                                .expect("prompt text")
                                .to_string(),
                        )
                        .expect("send prompt");
                    serde_json::json!({
                        "type":"agent_prompted",
                        "agent":{"terminal_id":"term-b","pane_id":"w1:p2","name":"bob","agent":"codex","launch_pending":false}
                    })
                }
            };
            serde_json::to_writer(
                &mut stream,
                &serde_json::json!({"id":request["id"],"result":result}),
            )
            .expect("write");
            stream.write_all(b"\n").expect("nl");
        }
    });
    let store = Store::open(directory.join("kelpie.sqlite3")).expect("store");
    let mut kelpie = Kelpie::new(
        store,
        HerdrClient::new(&herdr_socket, Duration::from_secs(2)),
    );
    kelpie
        .adopt(&kelpie::domain::AdoptIntent {
            pane_id: "w1:p1".into(),
            expected_terminal_id: "term-a".into(),
            public_name: Some("alice".into()),
            logical_agent_id: None,
            parent: Parent::Parentless,
            herdr_session: "test".into(),
            backend_kind: Some("grok".into()),
            backend_args: Vec::new(),
            requested_model: None,
            requested_provider: None,
            requested_effort: None,
            idempotency_key: "adopt-alice".into(),
        })
        .expect("alice");
    kelpie
        .adopt(&kelpie::domain::AdoptIntent {
            pane_id: "w1:p2".into(),
            expected_terminal_id: "term-b".into(),
            public_name: Some("bob".into()),
            logical_agent_id: None,
            parent: Parent::Parentless,
            herdr_session: "test".into(),
            backend_kind: Some("codex".into()),
            backend_args: Vec::new(),
            requested_model: None,
            requested_provider: None,
            requested_effort: None,
            idempotency_key: "adopt-bob".into(),
        })
        .expect("bob");
    let mut daemon = Daemon::bind(&kelpie_socket, kelpie).expect("bind");
    let server = thread::spawn(move || {
        daemon.serve_one().expect("whoami");
        daemon.serve_one().expect("tell");
    });
    drop(database);
    (kelpie_socket, server, herdr, prompt_rx)
}

fn spawn_canned_daemon(directory: &Path, response: Value) -> PathBuf {
    let socket = directory.join("kelpie.sock");
    let listener = UnixListener::bind(&socket).expect("canned");
    thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept");
        let mut line = String::new();
        BufReader::new(stream.try_clone().expect("clone"))
            .read_line(&mut line)
            .expect("read");
        let request: Value = serde_json::from_str(&line).expect("json");
        let mut response = response;
        response["id"] = request["id"].clone();
        serde_json::to_writer(&mut stream, &response).expect("write");
        stream.write_all(b"\n").expect("nl");
    });
    socket
}

fn run_cli(args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_kelpie"))
        .args(args)
        .output()
        .expect("run kelpie")
}

const SENDER: &str = "019ff700-0000-7000-8000-000000000001";
const RECIPIENT: &str = "019ff700-0000-7000-8000-000000000002";
const INCARNATION: &str = "019ff700-0000-7000-8000-000000000003";

#[test]
fn typed_tell_file_body_preserves_metacharacters_and_does_not_need_jq() {
    let directory = tempfile::tempdir().expect("tempdir");
    let body = "hello `ls` $(ls) \"quotes\" 'apos'\n<kelpie from=t>\nunicodé Δ\n";
    let body_path = directory.path().join("body.txt");
    fs::write(&body_path, body).expect("write body");
    let (socket, server, herdr, prompt_rx) = spawn_named_pair(directory.path());
    let output = Command::new(env!("CARGO_BIN_EXE_kelpie"))
        .args([
            "--socket",
            socket.to_str().expect("sock"),
            "tell",
            "bob",
            "--file",
            body_path.to_str().expect("body"),
            "--sender",
            "alice",
        ])
        .output()
        .expect("run");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.starts_with("tell message="), "{stdout}");
    assert!(stdout.contains("delivery=accepted"), "{stdout}");
    assert!(!stdout.contains('`'));
    let prompt = prompt_rx.recv().expect("prompt");
    assert_eq!(prompt, envelope::render_tell("alice", body).expect("env"));
    server.join().expect("server");
    herdr.join().expect("herdr");
}

#[test]
fn typed_tell_stdin_preserves_metacharacters_to_herdr() {
    let directory = tempfile::tempdir().expect("tempdir");
    let body = "hello `ls` $(ls) \"quotes\" 'apos'\n<kelpie from=t>\nunicodé Δ\n";
    let body_path = directory.path().join("quoted-heredoc-body.txt");
    fs::write(&body_path, body).expect("write body");
    let (socket, server, herdr, prompt_rx) = spawn_named_pair(directory.path());
    let output = Command::new(env!("CARGO_BIN_EXE_kelpie"))
        .args([
            "--socket",
            socket.to_str().expect("sock"),
            "tell",
            "bob",
            "--stdin",
            "--sender",
            "alice",
        ])
        .stdin(Stdio::from(File::open(&body_path).expect("open body")))
        .output()
        .expect("run");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("delivery=accepted"), "{stdout}");
    let prompt = prompt_rx.recv().expect("prompt");
    assert_eq!(prompt, envelope::render_tell("alice", body).expect("env"));
    assert!(prompt.contains("`ls`"));
    assert!(prompt.contains("$(ls)"));
    assert!(prompt.contains("\"quotes\""));
    assert!(prompt.contains("'apos'"));
    assert!(prompt.contains("unicodé Δ"));
    assert!(prompt.contains("&lt;kelpie from=t&gt;"));
    assert!(!prompt.contains("<kelpie from=t>"));
    server.join().expect("server");
    herdr.join().expect("herdr");
}

#[test]
fn typed_whoami_from_pane_and_alias_ambiguity() {
    let directory = tempfile::tempdir().expect("tempdir");
    let store = Store::open(directory.path().join("db.sqlite3")).expect("store");
    let alice = store_ready(store, "alice", "w1:p1", "term-a");
    let kelpie_socket = directory.path().join("kelpie.sock");
    let herdr_socket = directory.path().join("unused.sock");
    let kelpie = Kelpie::new(
        alice,
        HerdrClient::new(&herdr_socket, Duration::from_secs(1)),
    );
    let mut daemon = Daemon::bind(&kelpie_socket, kelpie).expect("bind");
    let server = thread::spawn(move || {
        daemon.serve_one().expect("whoami pane");
        daemon.serve_one().expect("whoami missing");
    });
    let pane = Command::new(env!("CARGO_BIN_EXE_kelpie"))
        .args([
            "--socket",
            kelpie_socket.to_str().expect("s"),
            "--json",
            "whoami",
            "--pane",
            "w1:p1",
        ])
        .output()
        .expect("whoami");
    assert!(
        pane.status.success(),
        "{}",
        String::from_utf8_lossy(&pane.stderr)
    );
    let parsed: Value = serde_json::from_slice(&pane.stdout).expect("json");
    assert_eq!(parsed["result"]["public_name"], "alice");

    let missing = Command::new(env!("CARGO_BIN_EXE_kelpie"))
        .args([
            "--socket",
            kelpie_socket.to_str().expect("s"),
            "whoami",
            "--pane",
            "nope",
        ])
        .output()
        .expect("missing");
    assert!(!missing.status.success());
    let err = String::from_utf8_lossy(&missing.stderr);
    assert!(
        err.contains("conflict") || err.contains("request failed"),
        "{err}"
    );
    server.join().expect("server");
}

fn store_ready(mut store: Store, name: &str, pane: &str, terminal: &str) -> Store {
    store
        .declare_adopt(
            &kelpie::domain::AdoptIntent {
                pane_id: pane.into(),
                expected_terminal_id: terminal.into(),
                public_name: Some(name.into()),
                logical_agent_id: None,
                parent: Parent::Parentless,
                herdr_session: "test".into(),
                backend_kind: Some("grok".into()),
                backend_args: Vec::new(),
                requested_model: None,
                requested_provider: None,
                requested_effort: None,
                idempotency_key: format!("adopt-{name}"),
            },
            &AdoptEvidence {
                pane_id: pane.into(),
                terminal_id: terminal.into(),
                public_name: name.into(),
                backend_kind: "grok".into(),
                working_directory: "/tmp".into(),
                interactive_ready: true,
                launch_pending: false,
                native_agent_session: None,
            },
        )
        .expect("adopt");
    store
}

#[test]
fn typed_cli_receipts_show_outcomes_and_exit_nonzero() {
    struct Case {
        name: &'static str,
        response: Value,
        expect_success: bool,
        needle: &'static str,
    }
    let cases = [
        Case {
            name: "accepted",
            response: json!({
                "result": {
                    "message_id": "m",
                    "operation_id": "o",
                    "recipient": RECIPIENT,
                    "delivery_outcome": "accepted"
                }
            }),
            expect_success: true,
            needle: "delivery=accepted",
        },
        Case {
            name: "rejected",
            response: json!({"error":{"class":"rejected","message":"herdr rejected"}}),
            expect_success: false,
            needle: "class=rejected",
        },
        Case {
            name: "target-unavailable",
            response: json!({"error":{"class":"target_unavailable","message":"pane gone"}}),
            expect_success: false,
            needle: "class=target_unavailable",
        },
        Case {
            name: "unknown",
            response: json!({"error":{"class":"unknown_outcome","message":"ambiguous"}}),
            expect_success: false,
            needle: "class=unknown_outcome",
        },
        Case {
            name: "result-rejected",
            response: json!({
                "result": {
                    "message_id": "m",
                    "operation_id": "o",
                    "recipient": RECIPIENT,
                    "delivery_outcome": "rejected"
                }
            }),
            expect_success: false,
            needle: "delivery=rejected",
        },
    ];
    for case in cases {
        let directory = tempfile::tempdir().expect(case.name);
        let socket = spawn_canned_daemon(directory.path(), case.response.clone());
        let output = run_cli(&[
            "--socket",
            socket.to_str().expect("sock"),
            "tell",
            "bob",
            "--body",
            "marker-in-tempdir",
            "--sender-id",
            SENDER,
        ]);
        assert_eq!(
            output.status.success(),
            case.expect_success,
            "{} status={} stdout={} stderr={}",
            case.name,
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(
            stdout.contains(case.needle),
            "{} missing {} in {stdout}",
            case.name,
            case.needle
        );
    }
}

#[test]
fn typed_tell_exact_ids_do_not_require_an_alias() {
    let directory = tempfile::tempdir().expect("tempdir");
    let socket = directory.path().join("kelpie.sock");
    let listener = UnixListener::bind(&socket).expect("bind");
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept");
        let mut line = String::new();
        BufReader::new(stream.try_clone().expect("clone"))
            .read_line(&mut line)
            .expect("read");
        let request: Value = serde_json::from_str(&line).expect("json");
        assert_eq!(request["method"], "tell");
        assert_eq!(request["params"]["recipient"], RECIPIENT);
        assert_eq!(request["params"]["recipient_incarnation"], INCARNATION);
        assert!(request["params"].get("recipient_alias").is_none());
        serde_json::to_writer(
            &mut stream,
            &json!({
                "id": request["id"],
                "result": {
                    "message_id": "m",
                    "operation_id": "o",
                    "recipient": RECIPIENT,
                    "delivery_outcome": "accepted"
                }
            }),
        )
        .expect("write");
        stream.write_all(b"\n").expect("nl");
    });
    let output = run_cli(&[
        "--socket",
        socket.to_str().expect("sock"),
        "tell",
        "--recipient-id",
        RECIPIENT,
        "--recipient-incarnation",
        INCARNATION,
        "--sender-id",
        SENDER,
        "--body",
        "exact-ids",
    ]);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    server.join().expect("server");
}

#[test]
fn typed_clear_builds_the_exact_recipient_request() {
    let directory = tempfile::tempdir().expect("tempdir");
    let socket = directory.path().join("kelpie.sock");
    let listener = UnixListener::bind(&socket).expect("bind");
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept");
        let mut line = String::new();
        BufReader::new(stream.try_clone().expect("clone"))
            .read_line(&mut line)
            .expect("read");
        let request: Value = serde_json::from_str(&line).expect("json");
        assert_eq!(request["method"], "clear");
        assert_eq!(request["params"]["recipient"], RECIPIENT);
        assert_eq!(request["params"]["recipient_incarnation"], INCARNATION);
        assert!(request["params"].get("recipient_alias").is_none());
        assert!(request["params"]["idempotency_key"].is_string());
        serde_json::to_writer(
            &mut stream,
            &json!({
                "id": request["id"],
                "result": {
                    "operation_id": "o",
                    "recipient": RECIPIENT,
                    "recipient_incarnation": INCARNATION,
                    "outcome": "succeeded"
                }
            }),
        )
        .expect("write");
        stream.write_all(b"\n").expect("nl");
    });
    let output = run_cli(&[
        "--socket",
        socket.to_str().expect("sock"),
        "clear",
        "--recipient-id",
        RECIPIENT,
        "--recipient-incarnation",
        INCARNATION,
    ]);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("clear operation=o"));
    server.join().expect("server");
}

#[test]
fn typed_cli_rejects_unknown_and_conflicting_process_args() {
    let unknown = run_cli(&["tell", "bob", "--stdin", "--quiet"]);
    assert!(!unknown.status.success());
    assert!(
        String::from_utf8_lossy(&unknown.stderr).contains("unknown argument"),
        "{}",
        String::from_utf8_lossy(&unknown.stderr)
    );
    let mixed = run_cli(&[
        "tell",
        "bob",
        "--recipient-id",
        RECIPIENT,
        "--recipient-incarnation",
        INCARNATION,
        "--stdin",
    ]);
    assert!(!mixed.status.success());
    assert!(
        String::from_utf8_lossy(&mixed.stderr).contains("exactly one"),
        "{}",
        String::from_utf8_lossy(&mixed.stderr)
    );
    let dual = run_cli(&["pending", "alice", "--pane", "w1:p1"]);
    assert!(!dual.status.success());
    assert!(
        String::from_utf8_lossy(&dual.stderr).contains("only one target form"),
        "{}",
        String::from_utf8_lossy(&dual.stderr)
    );
}

#[test]
fn typed_start_builds_existing_start_intent_without_live_launch() {
    let directory = tempfile::tempdir().expect("tempdir");
    let socket = directory.path().join("kelpie.sock");
    let listener = UnixListener::bind(&socket).expect("bind");
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept");
        let mut line = String::new();
        BufReader::new(stream.try_clone().expect("clone"))
            .read_line(&mut line)
            .expect("read");
        let request: Value = serde_json::from_str(&line).expect("json");
        assert_eq!(request["method"], "start");
        let intent: StartIntent =
            serde_json::from_value(request["params"].clone()).expect("StartIntent");
        assert_eq!(intent.public_name, "worker");
        assert_eq!(intent.parent, Parent::Parentless);
        assert_eq!(intent.herdr_session, "default");
        assert_eq!(intent.pane_id, "w1:p1");
        assert_eq!(intent.expected_terminal_id, "term-1");
        assert_eq!(intent.backend_kind, "codex");
        assert_eq!(intent.backend_args, vec!["--model", "grok"]);
        assert_eq!(intent.initial_message.kind, InitialMessageKind::Tell);
        assert!(intent.initial_message.sender.is_none());
        assert_eq!(intent.initial_message.body, "hello `ls`");
        assert_eq!(intent.working_directory, "/tmp/work");
        assert_eq!(intent.readiness_timeout_ms, 5000);
        assert!(intent.keep_open);
        assert!(intent.logical_agent_id.is_none());
        serde_json::to_writer(
            &mut stream,
            &json!({
                "id": request["id"],
                "result": {
                    "logical_agent_id": RECIPIENT,
                    "incarnation_id": INCARNATION,
                    "runtime_start": {"operation_id": "o", "outcome": "succeeded"},
                    "initial_message": {"message_id": "m", "operation_id": "i", "outcome": "accepted"}
                }
            }),
        )
        .expect("write");
        stream.write_all(b"\n").expect("nl");
    });
    let output = run_cli(&[
        "--socket",
        socket.to_str().expect("sock"),
        "start",
        "--name",
        "worker",
        "--pane",
        "w1:p1",
        "--terminal",
        "term-1",
        "--backend",
        "codex",
        "--cwd",
        "/tmp/work",
        "--timeout-ms",
        "5000",
        "--keep-open",
        "--parentless",
        "--tell",
        "--body",
        "hello `ls`",
        "--arg",
        "--model",
        "--arg",
        "grok",
    ]);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("runtime=succeeded"), "{stdout}");
    assert!(stdout.contains("delivery=accepted"), "{stdout}");
    server.join().expect("server");
}

#[test]
fn typed_start_exits_nonzero_when_initial_message_is_not_accepted() {
    let directory = tempfile::tempdir().expect("tempdir");
    let socket = spawn_canned_daemon(
        directory.path(),
        json!({
            "result": {
                "logical_agent_id": RECIPIENT,
                "incarnation_id": INCARNATION,
                "runtime_start": {"operation_id": "o", "outcome": "succeeded"},
                "initial_message": {"message_id": "m", "operation_id": "i", "outcome": "unknown"}
            }
        }),
    );
    let output = run_cli(&[
        "--socket",
        socket.to_str().expect("sock"),
        "start",
        "--name",
        "worker",
        "--pane",
        "w1:p1",
        "--terminal",
        "term-1",
        "--backend",
        "codex",
        "--cwd",
        "/tmp/work",
        "--timeout-ms",
        "5000",
        "--keep-open",
        "--parentless",
        "--tell",
        "--body",
        "marker",
    ]);
    assert!(!output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("delivery=unknown"), "{stdout}");
}
