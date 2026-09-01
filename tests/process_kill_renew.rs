//! Real-`kelpied` process-kill coverage across a renew's two external effects.
//!
//! The boundary that matters is the one between the clear and the resume
//! prompt. An incarnation killed there has lost its context, and nothing inside
//! it survives to notice. Recovery must finish that injection rather than start
//! the renew again.

use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixListener;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use kelpie::domain::{
    InitialMessageIntent, InitialMessageKind, Parent, RenewId, RenewIntent, RenewTimeout,
    StartIntent,
};
use kelpie::herdr::AgentObservation;
use kelpie::store::{DeclaredStart, Store, store_clock_ms};
use rusqlite::Connection;
use serde_json::Value;

const DAEMON_BOUND: &str = "daemon_bound";
const BEFORE_CLEAR: &str = "renew_after_ready_before_clear";
const BEFORE_INJECT: &str = "renew_after_clear_before_inject";

const PRE_CLEAR_SESSION: &str = "\"sess-before\"";

fn intent(name: &str, pane: &str, terminal: &str, key: &str) -> StartIntent {
    StartIntent {
        public_name: name.into(),
        logical_agent_id: None,
        parent: Parent::Parentless,
        herdr_session: "renew-fault-test".into(),
        pane_id: pane.into(),
        expected_terminal_id: terminal.into(),
        backend_kind: "claude".into(),
        backend_args: vec![],
        initial_message: InitialMessageIntent {
            sender: None,
            kind: InitialMessageKind::Tell,
            body: "seed only".into(),
        },
        working_directory: "/tmp/work".into(),
        idempotency_key: key.into(),
        readiness_timeout_ms: 5_000,
        keep_open: true,
        supersedes: None,
        requested_model: None,
        requested_provider: None,
        requested_effort: None,
    }
}

fn observation(session: &str) -> AgentObservation {
    AgentObservation {
        terminal_id: "term-worker".into(),
        pane_id: "w1:p1".into(),
        name: Some("worker".into()),
        agent: Some("claude".into()),
        interactive_ready: true,
        launch_pending: false,
        agent_session: Some(Value::String(session.into())),
    }
}

fn agent_json(session: &str) -> Value {
    serde_json::json!({
        "terminal_id":"term-worker",
        "pane_id":"w1:p1",
        "name":"worker",
        "agent":"claude",
        "agent_status":"idle",
        "interactive_ready":true,
        "launch_pending":false,
        "agent_session":session
    })
}

/// A fake Herdr that answers by method instead of by a fixed script, so a
/// daemon that reconciles more or less on startup does not deadlock the test.
struct FakeHerdr {
    prompts: Arc<Mutex<Vec<Value>>>,
    stop: Arc<AtomicBool>,
}

impl FakeHerdr {
    fn start(socket: &Path, session: &str) -> Self {
        let listener = UnixListener::bind(socket).expect("bind fake Herdr");
        listener
            .set_nonblocking(true)
            .expect("nonblocking fake Herdr");
        let prompts = Arc::new(Mutex::new(Vec::new()));
        let session_value = Arc::new(Mutex::new(session.to_string()));
        let stop = Arc::new(AtomicBool::new(false));
        let thread_prompts = Arc::clone(&prompts);
        let thread_session = Arc::clone(&session_value);
        let thread_stop = Arc::clone(&stop);
        thread::spawn(move || {
            while !thread_stop.load(Ordering::Relaxed) {
                let Ok((mut stream, _)) = listener.accept() else {
                    thread::sleep(Duration::from_millis(5));
                    continue;
                };
                stream.set_nonblocking(false).expect("blocking stream");
                let mut line = String::new();
                if BufReader::new(stream.try_clone().expect("clone"))
                    .read_line(&mut line)
                    .is_err()
                    || line.trim().is_empty()
                {
                    continue;
                }
                let Ok(request) = serde_json::from_str::<Value>(&line) else {
                    continue;
                };
                let session = thread_session.lock().expect("session lock").clone();
                let result = match request["method"].as_str() {
                    Some("ping") => {
                        serde_json::json!({"type":"pong","version":"test","protocol":20})
                    }
                    Some("session.snapshot") => serde_json::json!({
                        "type":"session_snapshot",
                        "snapshot":{"protocol":20,"panes":[],"agents":[agent_json(&session)]}
                    }),
                    Some("agent.get") => {
                        serde_json::json!({"type":"agent_info","agent":agent_json(&session)})
                    }
                    Some("agent.prompt") => {
                        thread_prompts
                            .lock()
                            .expect("prompts lock")
                            .push(request.clone());
                        serde_json::json!({
                            "type":"agent_prompted","agent":agent_json(&session)
                        })
                    }
                    _ => serde_json::json!({"type":"ok"}),
                };
                let _ = serde_json::to_writer(
                    &mut stream,
                    &serde_json::json!({"id":request["id"],"result":result}),
                );
                let _ = stream.write_all(b"\n");
            }
        });
        Self { prompts, stop }
    }

    fn prompts(&self) -> Vec<Value> {
        self.prompts.lock().expect("prompts lock").clone()
    }

    fn stop(&self) {
        self.stop.store(true, Ordering::Relaxed);
    }
}

/// Seed one Ready claude agent plus a renew already past its prepare phase.
fn seed_renew(database: &Path, clearing: bool) -> RenewId {
    let mut store = Store::open(database).expect("open seed store");
    let worker: DeclaredStart = store
        .declare_start(&intent("worker", "w1:p1", "term-worker", "worker-start"))
        .expect("declare worker");
    store
        .begin_attempt(worker.operation_id, worker.incarnation_id, "seed-start")
        .expect("begin seed attempt");
    store
        .accept_start_ready(
            worker.operation_id,
            worker.incarnation_id,
            &observation("sess-before"),
            None,
        )
        .expect("accept seed readiness");

    let renew_id = store
        .create_renew(&RenewIntent {
            logical_agent_id: worker.logical_agent_id,
            incarnation_id: worker.incarnation_id,
            requester_agent_id: worker.logical_agent_id,
            prepare_prompt: "save progress to progress.md".into(),
            resume_prompt: "read progress.md and continue".into(),
            on_timeout: RenewTimeout::Abort,
            prepare_timeout_ms: 600_000,
            every_ms: None,
            scheduled_at_ms: store_clock_ms().expect("clock"),
        })
        .expect("create renew");
    let ask = store
        .create_ask_with_schedule(
            worker.logical_agent_id,
            worker.logical_agent_id,
            worker.incarnation_id,
            "save progress to progress.md",
            "renew-prepare",
            None,
            None,
            false,
        )
        .expect("prepare ask");
    store
        .mark_renew_preparing(
            renew_id,
            ask.message_id,
            store_clock_ms().expect("clock") + 600_000,
        )
        .expect("preparing");
    store.mark_renew_ready(renew_id).expect("ready");
    if clearing {
        store
            .mark_renew_clearing(
                renew_id,
                PRE_CLEAR_SESSION,
                store_clock_ms().expect("clock") + 3_600_000,
                None,
            )
            .expect("clearing");
    }
    renew_id
}

fn spawn_kelpied(
    database: &Path,
    kelpie_socket: &Path,
    herdr_socket: &Path,
    fault_socket: &Path,
    points: &str,
) -> Child {
    Command::new(env!("CARGO_BIN_EXE_kelpied"))
        .arg("--database")
        .arg(database)
        .arg("--socket")
        .arg(kelpie_socket)
        .arg("--herdr-socket")
        .arg(herdr_socket)
        .env("KELPIE_TEST_FAULT_POINTS", points)
        .env("KELPIE_TEST_FAULT_SOCKET", fault_socket)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn real kelpied")
}

/// Wait for one fault rendezvous, failing with the daemon's own notices rather
/// than hanging when the point is never reached.
fn accept_point(listener: &UnixListener, expected: &str) -> std::os::unix::net::UnixStream {
    accept_point_inner(listener, expected, None)
}

fn accept_point_for(
    listener: &UnixListener,
    expected: &str,
    database: &Path,
) -> std::os::unix::net::UnixStream {
    accept_point_inner(listener, expected, Some(database))
}

fn accept_point_inner(
    listener: &UnixListener,
    expected: &str,
    database: Option<&Path>,
) -> std::os::unix::net::UnixStream {
    listener.set_nonblocking(true).expect("nonblocking fault");
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    let stream = loop {
        match listener.accept() {
            Ok((stream, _)) => break stream,
            Err(_) if std::time::Instant::now() < deadline => {
                thread::sleep(Duration::from_millis(10));
            }
            Err(error) => {
                let notices = database.map(operator_notices).unwrap_or_default();
                let rows = database.map(renew_rows).unwrap_or_default();
                panic!(
                    "fault point {expected} was never reached: {error};\n\
                     notices: {notices:?}\n  renews: {rows:?}"
                )
            }
        }
    };
    listener.set_nonblocking(false).expect("blocking fault");
    stream
        .set_nonblocking(false)
        .expect("blocking fault stream");
    let mut line = String::new();
    BufReader::new(stream.try_clone().expect("clone fault stream"))
        .read_line(&mut line)
        .expect("read fault point");
    assert_eq!(line.trim_end(), expected);
    stream
}

fn renew_rows(database: &Path) -> Vec<String> {
    let connection = Connection::open(database).expect("open state database");
    let mut statement = connection
        .prepare(
            "SELECT r.phase, r.pre_clear_session_json, i.state, i.observed_pane_id
             FROM renews r JOIN incarnations i ON i.id = r.incarnation_id",
        )
        .expect("prepare renews");
    let rows = statement
        .query_map([], |row| {
            Ok(format!(
                "phase={} pre_clear={:?} incarnation={} pane={}",
                row.get::<_, String>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?
            ))
        })
        .expect("query renews");
    rows.map(|row| row.expect("renew row")).collect()
}

/// Whatever the daemon reported while failing to make progress.
fn operator_notices(database: &Path) -> Vec<String> {
    let connection = Connection::open(database).expect("open state database");
    let mut statement = connection
        .prepare("SELECT body FROM operator_notices ORDER BY created_at_ms")
        .expect("prepare notices");
    let rows = statement
        .query_map([], |row| row.get::<_, String>(0))
        .expect("query notices");
    rows.map(|row| row.expect("notice body")).collect()
}

fn renew_phase(database: &Path, renew_id: RenewId) -> String {
    Connection::open(database)
        .expect("open state database")
        .query_row(
            "SELECT phase FROM renews WHERE id = ?1",
            [renew_id.to_string()],
            |row| row.get(0),
        )
        .expect("renew phase")
}

fn attempt_phases(database: &Path, renew_id: RenewId, step: &str) -> Vec<String> {
    let connection = Connection::open(database).expect("open state database");
    let mut statement = connection
        .prepare(
            "SELECT phase FROM renew_attempts WHERE renew_id = ?1 AND step = ?2
             ORDER BY started_at_ms, id",
        )
        .expect("prepare attempts");
    let rows = statement
        .query_map(rusqlite::params![renew_id.to_string(), step], |row| {
            row.get::<_, String>(0)
        })
        .expect("query attempts");
    rows.map(|row| row.expect("attempt phase")).collect()
}

fn observed_session(database: &Path) -> Option<String> {
    Connection::open(database)
        .expect("open state database")
        .query_row(
            "SELECT observed_native_session_json FROM incarnations LIMIT 1",
            [],
            |row| row.get(0),
        )
        .expect("observed session")
}

#[test]
fn kill_before_the_clear_leaves_the_context_intact_and_the_renew_resumable() {
    let directory = tempfile::tempdir().expect("tempdir");
    let database = directory.path().join("kelpie.sqlite3");
    let kelpie_socket = directory.path().join("kelpie.sock");
    let herdr_socket = directory.path().join("herdr.sock");
    let fault_socket = directory.path().join("fault.sock");
    let renew_id = seed_renew(&database, false);
    let fault_listener = UnixListener::bind(&fault_socket).expect("bind fault harness");
    let herdr = FakeHerdr::start(&herdr_socket, "sess-before");

    let mut daemon = spawn_kelpied(
        &database,
        &kelpie_socket,
        &herdr_socket,
        &fault_socket,
        &format!("{DAEMON_BOUND},{BEFORE_CLEAR}"),
    );
    let mut bound = accept_point(&fault_listener, DAEMON_BOUND);
    bound.write_all(b"x").expect("release daemon startup");
    let paused = accept_point(&fault_listener, BEFORE_CLEAR);
    daemon.kill().expect("kill kelpied");
    daemon.wait().expect("reap kelpied");
    drop(paused);
    herdr.stop();

    assert!(
        herdr.prompts().is_empty(),
        "the clear must not have been written before the fault point"
    );
    // The pre-clear reference and the submitted attempt are both durable before
    // the write, so recovery can tell that a clear may have escaped and must
    // not send a second one.
    assert_eq!(renew_phase(&database, renew_id), "clearing");
    assert_eq!(
        attempt_phases(&database, renew_id, "clear"),
        vec!["submitted"]
    );
    assert!(attempt_phases(&database, renew_id, "inject").is_empty());
}

#[test]
fn kill_between_the_clear_and_the_resume_prompt_is_completed_not_restarted() {
    let directory = tempfile::tempdir().expect("tempdir");
    let database = directory.path().join("kelpie.sqlite3");
    let kelpie_socket = directory.path().join("kelpie.sock");
    let herdr_socket = directory.path().join("herdr.sock");
    let fault_socket = directory.path().join("fault.sock");
    // Seeded mid-clear: this incarnation's context is already gone.
    let renew_id = seed_renew(&database, true);
    let fault_listener = UnixListener::bind(&fault_socket).expect("bind fault harness");
    // Rotated: the clear landed, so the resume prompt is owed.
    let herdr = FakeHerdr::start(&herdr_socket, "sess-after");

    let mut daemon = spawn_kelpied(
        &database,
        &kelpie_socket,
        &herdr_socket,
        &fault_socket,
        &format!("{DAEMON_BOUND},{BEFORE_INJECT}"),
    );
    let mut bound = accept_point(&fault_listener, DAEMON_BOUND);
    bound.write_all(b"x").expect("release daemon startup");
    let paused = accept_point_for(&fault_listener, BEFORE_INJECT, &database);
    daemon.kill().expect("kill kelpied");
    daemon.wait().expect("reap kelpied");
    drop(paused);
    herdr.stop();

    assert!(
        herdr.prompts().is_empty(),
        "no resume prompt may cross before the fault point"
    );
    // The worst durable state in the whole feature: cleared, not yet re-seeded.
    assert_eq!(renew_phase(&database, renew_id), "clearing");
    assert_eq!(
        attempt_phases(&database, renew_id, "inject"),
        vec!["submitted"]
    );

    fs::remove_file(&kelpie_socket).expect("remove killed daemon socket");
    fs::remove_file(&herdr_socket).expect("remove killed Herdr socket");

    let recovery = FakeHerdr::start(&herdr_socket, "sess-after");
    let mut recovered = spawn_kelpied(
        &database,
        &kelpie_socket,
        &herdr_socket,
        &fault_socket,
        DAEMON_BOUND,
    );
    let mut bound = accept_point(&fault_listener, DAEMON_BOUND);
    bound.write_all(b"x").expect("release recovered startup");

    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while recovery.prompts().is_empty() && std::time::Instant::now() < deadline {
        thread::sleep(Duration::from_millis(20));
    }
    let prompts = recovery.prompts();
    recovered.kill().expect("kill recovered kelpied");
    recovered.wait().expect("reap recovered kelpied");
    recovery.stop();

    assert_eq!(
        prompts.len(),
        1,
        "recovery owes exactly the injection, not a second clear"
    );
    assert_eq!(prompts[0]["params"]["target"], "w1:p1");
    let text = prompts[0]["params"]["text"]
        .as_str()
        .expect("resume prompt text");
    assert!(
        text.contains("You are a continuation."),
        "a re-seeded agent must be told it is resuming: {text}"
    );
    assert!(text.contains("read progress.md and continue"));
    assert!(
        text.contains("resumed cycle=1"),
        "the resume envelope carries its cycle: {text}"
    );
    assert!(
        !text.contains("/clear"),
        "recovery must not resend the clear: {text}"
    );
    // A submitted-but-unresolved injection is retried, not abandoned: a
    // duplicate resume prompt is recoverable, a missing one is not.
    assert_eq!(
        attempt_phases(&database, renew_id, "inject"),
        vec!["submitted", "accepted"]
    );
    assert!(
        attempt_phases(&database, renew_id, "clear").is_empty(),
        "recovery must not send a second clear"
    );
    assert_eq!(
        observed_session(&database).as_deref(),
        Some("\"sess-after\""),
        "attribution must follow the renewed conversation, not the dead one"
    );
}
