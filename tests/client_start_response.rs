//! Real `kelpie` client regression for composed start response delivery.
//!
//! Proves a successful start prints exactly one correlated NDJSON response with
//! the launch receipt fields before EOF, even when readiness takes multiple
//! seconds (the live Grok start shape).

use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixListener;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;
use std::time::Duration;

use kelpie::daemon::Daemon;
use kelpie::domain::{InitialMessageIntent, InitialMessageKind, Parent, StartIntent};
use kelpie::herdr::HerdrClient;
use kelpie::slice::Kelpie;
use kelpie::store::Store;
use serde_json::Value;

fn readiness_delay_ms() -> u64 {
    2_500
}

fn spawn_slow_ready_herdr(socket: &Path) -> thread::JoinHandle<()> {
    let listener = UnixListener::bind(socket).expect("bind fake Herdr");
    let delay = Duration::from_millis(readiness_delay_ms());
    thread::spawn(move || {
        // negotiate + pre-start snapshot + start + post-start readiness snapshot
        // + initial-message prompt. Delay after agent.start so launch waits.
        let exchanges: Vec<(&str, Value)> = vec![
            (
                "ping",
                serde_json::json!({"type":"pong","version":"test","protocol":20}),
            ),
            (
                "session.snapshot",
                serde_json::json!({
                    "type":"session_snapshot",
                    "snapshot":{
                        "protocol":20,
                        "panes":[{"pane_id":"w1:p1","terminal_id":"term-1","cwd":"/tmp/work"}],
                        "agents":[]
                    }
                }),
            ),
            (
                "agent.start",
                serde_json::json!({
                    "type":"agent_started",
                    "agent":{
                        "terminal_id":"term-1","pane_id":"w1:p1","name":"worker",
                        "agent":"codex","interactive_ready":false,"launch_pending":true
                    },
                    "argv":["codex"]
                }),
            ),
        ];
        for (expected, result) in exchanges {
            let (mut stream, _) = listener.accept().expect("accept Herdr request");
            let mut line = String::new();
            BufReader::new(stream.try_clone().expect("clone"))
                .read_line(&mut line)
                .expect("read");
            let request: Value = serde_json::from_str(&line).expect("JSON");
            assert_eq!(request["method"], expected);
            serde_json::to_writer(
                &mut stream,
                &serde_json::json!({"id":request["id"],"result":result}),
            )
            .expect("write");
            stream.write_all(b"\n").expect("newline");
        }
        thread::sleep(delay);
        // readiness poll: agent.get, which reconciles the managed start
        let (mut stream, _) = listener.accept().expect("accept readiness poll");
        let mut line = String::new();
        BufReader::new(stream.try_clone().expect("clone"))
            .read_line(&mut line)
            .expect("read");
        let request: Value = serde_json::from_str(&line).expect("JSON");
        assert_eq!(request["method"], "agent.get");
        serde_json::to_writer(
            &mut stream,
            &serde_json::json!({
                "id": request["id"],
                "result": {
                    "type":"agent_info",
                    "agent":{
                        "terminal_id":"term-1","pane_id":"w1:p1","name":"worker",
                        "agent":"codex","interactive_ready":true,"launch_pending":false
                    }
                }
            }),
        )
        .expect("write");
        stream.write_all(b"\n").expect("newline");
        // initial message prompt
        let (mut stream, _) = listener.accept().expect("accept initial prompt");
        let mut line = String::new();
        BufReader::new(stream.try_clone().expect("clone"))
            .read_line(&mut line)
            .expect("read");
        let request: Value = serde_json::from_str(&line).expect("JSON");
        assert_eq!(request["method"], "agent.prompt");
        let text = request["params"]["text"].as_str().expect("text");
        assert!(text.starts_with("<kelpie from="), "envelope: {text}");
        serde_json::to_writer(
            &mut stream,
            &serde_json::json!({
                "id": request["id"],
                "result": {
                    "type":"agent_prompted",
                    "agent":{
                        "terminal_id":"term-1","pane_id":"w1:p1","name":"worker",
                        "agent":"codex","interactive_ready":true,"launch_pending":false
                    }
                }
            }),
        )
        .expect("write");
        stream.write_all(b"\n").expect("newline");
    })
}

fn start_request(id: &str) -> Value {
    serde_json::json!({
        "id": id,
        "method": "start",
        "params": StartIntent {
            public_name: "worker".into(),
            logical_agent_id: None,
            parent: Parent::Parentless,
            herdr_session: "test".into(),
            pane_id: "w1:p1".into(),
            expected_terminal_id: "term-1".into(),
            backend_kind: "codex".into(),
            backend_args: vec![],
            initial_message: InitialMessageIntent {
                sender: None,
                kind: InitialMessageKind::Tell,
                body: "initial work".into(),
            },
            working_directory: "/tmp/work".into(),
            idempotency_key: format!("{id}-key"),
            readiness_timeout_ms: 15_000,
            keep_open: true,
            supersedes: None,
            requested_model: None,
            requested_provider: None,
            requested_effort: None,
        }
    })
}

fn assert_start_receipt(response: &Value, request_id: &str) {
    assert_eq!(response["id"], request_id);
    assert!(response.get("error").is_none_or(Value::is_null));
    let result = response["result"].as_object().expect("result object");
    assert!(result.contains_key("logical_agent_id"), "{result:?}");
    assert!(result.contains_key("incarnation_id"), "{result:?}");
    assert_eq!(result["runtime_start"]["outcome"], "succeeded");
    assert_eq!(result["initial_message"]["outcome"], "accepted");
    assert!(result["initial_message"]["message_id"].is_string());
    assert!(result["runtime_start"]["operation_id"].is_string());
}

fn spawn_start_daemon(
    directory: &Path,
) -> (PathBuf, thread::JoinHandle<()>, thread::JoinHandle<()>) {
    let database = directory.join("kelpie.sqlite3");
    let kelpie_socket = directory.join("kelpie.sock");
    let herdr_socket = directory.join("herdr.sock");
    let herdr = spawn_slow_ready_herdr(&herdr_socket);
    let store = Store::open(&database).expect("store");
    let client = HerdrClient::new(&herdr_socket, Duration::from_secs(5));
    let kelpie = Kelpie::new(store, client);
    let mut daemon = Daemon::bind(&kelpie_socket, kelpie).expect("bind");
    let server = thread::spawn(move || {
        daemon.serve_one().expect("serve start");
    });
    (kelpie_socket, server, herdr)
}

#[test]
fn real_client_prints_one_correlated_start_receipt_before_eof() {
    let directory = tempfile::tempdir().expect("tempdir");
    let runtime = directory.path().join("runtime");
    fs::create_dir_all(&runtime).expect("runtime");
    let receipt = directory.path().join("receipt.ndjson");
    let (kelpie_socket, server, herdr) = spawn_start_daemon(directory.path());

    let request = start_request("live-start-receipt-1");
    let started = std::time::Instant::now();
    let mut child = Command::new(env!("CARGO_BIN_EXE_kelpie"))
        .arg(&kelpie_socket)
        .env("XDG_RUNTIME_DIR", &runtime)
        .env("KELPIE_RECEIPT_PATH", &receipt)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn kelpie client");
    {
        let mut stdin = child.stdin.take().expect("stdin");
        serde_json::to_writer(&mut stdin, &request).expect("write request");
        stdin.write_all(b"\n").expect("newline");
    }
    let output = child.wait_with_output().expect("wait client");
    let elapsed = started.elapsed();

    assert!(
        output.status.success(),
        "client exit {:?}; stderr={}",
        output.status.code(),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        elapsed >= Duration::from_millis(readiness_delay_ms()),
        "expected slow readiness path, elapsed {elapsed:?}"
    );
    assert!(
        elapsed < Duration::from_secs(12),
        "client hung after start completed: {elapsed:?}"
    );

    let stdout = String::from_utf8(output.stdout).expect("stdout utf8");
    assert!(
        !stdout.trim().is_empty(),
        "empty stdout; stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    let lines: Vec<&str> = stdout.lines().collect();
    assert_eq!(
        lines.len(),
        1,
        "expected exactly one NDJSON response line, got {stdout:?}"
    );
    let response: Value = serde_json::from_str(lines[0]).expect("response JSON");
    assert_start_receipt(&response, "live-start-receipt-1");
    assert!(!stdout.ends_with("\n\n"), "unexpected trailing blank lines");

    let file_body = fs::read_to_string(&receipt).expect("receipt file");
    let file_response: Value = serde_json::from_str(file_body.trim()).expect("receipt JSON");
    assert_eq!(file_response, response);

    server.join().expect("daemon thread");
    herdr.join().expect("herdr thread");
}

#[test]
fn real_client_preserves_receipt_when_stdout_pipe_is_torn_down() {
    // Models the live failure mode where tool capture yields zero bytes even
    // though the daemon completed start: the caller's stdout pipe is gone
    // (EPIPE) while the Unix-socket response still arrives. Herdr agent spawn
    // cannot close the kelpie client's FDs (setsid/close_random_fds run only in
    // the new agent child), so the product must not treat stdout as the sole
    // delivery channel.
    let directory = tempfile::tempdir().expect("tempdir");
    let runtime = directory.path().join("runtime");
    fs::create_dir_all(&runtime).expect("runtime");
    let receipt = directory.path().join("receipt.ndjson");
    let (kelpie_socket, server, herdr) = spawn_start_daemon(directory.path());

    let request = start_request("live-start-receipt-torn-stdout");
    let mut child = Command::new(env!("CARGO_BIN_EXE_kelpie"))
        .arg(&kelpie_socket)
        .env("XDG_RUNTIME_DIR", &runtime)
        .env("KELPIE_RECEIPT_PATH", &receipt)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn kelpie client");
    {
        let mut stdin = child.stdin.take().expect("stdin");
        serde_json::to_writer(&mut stdin, &request).expect("write request");
        stdin.write_all(b"\n").expect("newline");
    }
    // Drop the parent's read end of stdout immediately so any later client write
    // hits a broken pipe — equivalent to a capture channel that vanished mid-start.
    drop(child.stdout.take());

    let status = child.wait().expect("wait client");
    assert!(
        status.success(),
        "client must still exit 0 when stdout is gone after a successful RPC; status={status:?}"
    );
    let file_body = fs::read_to_string(&receipt).expect("receipt file must exist");
    assert!(
        !file_body.trim().is_empty(),
        "receipt file empty despite successful start"
    );
    let response: Value = serde_json::from_str(file_body.trim()).expect("receipt JSON");
    assert_start_receipt(&response, "live-start-receipt-torn-stdout");

    let trace_path = runtime.join("kelpie/last-client-trace.json");
    let trace: Value =
        serde_json::from_str(&fs::read_to_string(&trace_path).expect("trace")).expect("trace JSON");
    assert_eq!(trace["request_id"], "live-start-receipt-torn-stdout");
    assert_eq!(trace["method"], "start");
    assert_eq!(trace["stdout_written"], false);
    assert!(
        !trace["stdout_error"].as_str().unwrap_or("").is_empty(),
        "trace should record the stdout failure: {trace}"
    );

    server.join().expect("daemon thread");
    herdr.join().expect("herdr thread");
}

#[test]
fn real_client_exit_nonzero_on_empty_daemon_close() {
    let directory = tempfile::tempdir().expect("tempdir");
    let runtime = directory.path().join("runtime");
    fs::create_dir_all(&runtime).expect("runtime");
    let kelpie_socket = directory.path().join("kelpie.sock");
    let listener = UnixListener::bind(&kelpie_socket).expect("bind");
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().expect("accept");
        // Read request then close without writing a response.
        let mut line = String::new();
        BufReader::new(&stream)
            .read_line(&mut line)
            .expect("read request");
        drop(stream);
    });

    let mut child = Command::new(env!("CARGO_BIN_EXE_kelpie"))
        .arg(&kelpie_socket)
        .env("XDG_RUNTIME_DIR", &runtime)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn");
    {
        let mut stdin = child.stdin.take().expect("stdin");
        stdin
            .write_all(br#"{"id":"empty-1","method":"notice.list","params":{}}"#)
            .expect("write");
        stdin.write_all(b"\n").expect("nl");
    }
    let output = child.wait_with_output().expect("wait");
    assert!(!output.status.success(), "expected nonzero exit");
    assert!(
        output.stdout.is_empty(),
        "stdout should stay empty on missing response"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("without a correlated response"),
        "stderr={stderr}"
    );
    server.join().expect("server");
}
