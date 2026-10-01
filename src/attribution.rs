//! Requested launch configuration vs adapter-observed execution metadata.

use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Requested model, provider, or effort. Absence means nobody wrote the field.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct RequestedAttribution {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effort: Option<String>,
}

/// One observed field. `Undetermined` is explicit and is not an omitted request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", content = "value", rename_all = "snake_case")]
pub enum ObservedField {
    Undetermined,
    Reported(String),
}

impl ObservedField {
    fn reported(value: Option<String>) -> Self {
        match value {
            Some(value) if !value.is_empty() => Self::Reported(value),
            _ => Self::Undetermined,
        }
    }
}

/// One append-only observation from a named adapter or an explicit non-adapter.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObservedAttribution {
    pub adapter: String,
    pub model: ObservedField,
    pub provider: ObservedField,
    pub effort: ObservedField,
}

/// Roots used when a named adapter may read its own session artifact.
#[derive(Debug, Clone, Default)]
pub struct SessionRoots {
    pub claude: Option<PathBuf>,
    pub codex: Option<PathBuf>,
    pub opencode: Option<PathBuf>,
    /// `OpenCode` V2 command-line clients to ask, in order. V2 keeps sessions
    /// behind its background service rather than in a store this process can
    /// locate, so they are read through the service's own `api` command.
    pub opencode_clients: Vec<PathBuf>,
}

impl SessionRoots {
    /// Default live session roots under the user home directory.
    #[must_use]
    pub fn from_home() -> Self {
        let home = std::env::var_os("HOME").map(PathBuf::from);
        Self {
            claude: home.as_ref().map(|home| home.join(".claude/projects")),
            codex: home.as_ref().map(|home| home.join(".codex/sessions")),
            opencode: home.as_ref().map(|home| home.join(".local/share/opencode")),
            // `opencode` is V2 once the machine is migrated; until then V1 has
            // no `api` command and the side-by-side `opencode2` answers.
            opencode_clients: vec![PathBuf::from("opencode"), PathBuf::from("opencode2")],
        }
    }
}

/// Observe execution metadata. Never uses requested fields, pane titles, or Herdr `AgentInfo`.
#[must_use]
pub fn observe(
    backend_kind: &str,
    native_session: Option<&Value>,
    roots: &SessionRoots,
) -> ObservedAttribution {
    observe_detailed(backend_kind, native_session, roots).0
}

/// Observe, and say why when nothing could be determined.
///
/// The reason is diagnostic, not evidence: it explains whether an adapter found
/// no session at all or found one that has not run a turn yet. Those need
/// opposite responses from a caller — look elsewhere, or ask again later — and
/// `undetermined` alone cannot tell them apart. It is deliberately not durable;
/// only the observation itself is.
#[must_use]
pub fn observe_detailed(
    backend_kind: &str,
    native_session: Option<&Value>,
    roots: &SessionRoots,
) -> (ObservedAttribution, Option<String>) {
    let session_id = native_session_id(native_session);
    match backend_kind {
        "claude" => (
            observe_claude(session_id.as_deref(), roots.claude.as_deref()),
            None,
        ),
        "codex" => (
            observe_codex(session_id.as_deref(), roots.codex.as_deref()),
            None,
        ),
        "opencode" => observe_opencode(
            session_id.as_deref(),
            roots.opencode.as_deref(),
            &roots.opencode_clients,
        ),
        other => (undetermined(other), None),
    }
}

fn undetermined(adapter: &str) -> ObservedAttribution {
    ObservedAttribution {
        adapter: adapter.to_string(),
        model: ObservedField::Undetermined,
        provider: ObservedField::Undetermined,
        effort: ObservedField::Undetermined,
    }
}

fn native_session_id(native_session: Option<&Value>) -> Option<String> {
    let value = native_session?.get("value")?.as_str()?;
    let value = value.trim();
    (!value.is_empty()).then(|| value.to_string())
}

fn observe_claude(session_id: Option<&str>, root: Option<&Path>) -> ObservedAttribution {
    let Some(session_id) = session_id else {
        return undetermined("claude");
    };
    let Some(root) = root else {
        return undetermined("claude");
    };
    let path = find_named_jsonl(root, session_id);
    let Some(path) = path else {
        return undetermined("claude");
    };
    let Ok(text) = fs::read_to_string(&path) else {
        return undetermined("claude");
    };
    let mut model = None;
    let mut provider = None;
    for line in text.lines().rev() {
        let Ok(value) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        if value.get("type").and_then(Value::as_str) != Some("assistant") {
            continue;
        }
        let message = value.get("message");
        model = nonempty_str(message.and_then(|message| message.get("model")));
        provider = nonempty_str(message.and_then(|message| message.get("provider")));
        if model.is_some() || provider.is_some() {
            break;
        }
    }
    ObservedAttribution {
        adapter: "claude".into(),
        model: ObservedField::reported(model),
        provider: ObservedField::reported(provider),
        effort: ObservedField::Undetermined,
    }
}

fn observe_codex(session_id: Option<&str>, root: Option<&Path>) -> ObservedAttribution {
    let Some(session_id) = session_id else {
        return undetermined("codex");
    };
    let Some(root) = root else {
        return undetermined("codex");
    };
    let Some(path) = find_containing_jsonl(root, session_id) else {
        return undetermined("codex");
    };
    let Ok(text) = fs::read_to_string(&path) else {
        return undetermined("codex");
    };
    for line in text.lines() {
        let Ok(value) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        if value.get("type").and_then(Value::as_str) != Some("session_meta") {
            continue;
        }
        let payload = value.get("payload");
        return ObservedAttribution {
            adapter: "codex".into(),
            model: ObservedField::reported(nonempty_str(
                payload.and_then(|payload| payload.get("model")),
            )),
            provider: ObservedField::reported(nonempty_str(
                payload.and_then(|payload| payload.get("model_provider")),
            )),
            effort: ObservedField::reported(nonempty_str(
                payload.and_then(|payload| payload.get("effort")),
            )),
        };
    }
    undetermined("codex")
}

/// Read `OpenCode`'s own session store.
///
/// `OpenCode` keeps messages in `SQLite`, and one directory holds several stores
/// (`opencode.db`, `opencode-local.db`, per-workspace files, …), so the session
/// is searched for rather than assumed to sit in a default file. Model identity
/// is written on assistant rows only, so a session that has not produced a turn
/// has nothing to observe yet — genuinely undetermined, and resolved by
/// observing again later rather than by guessing.
fn observe_opencode(
    session_id: Option<&str>,
    root: Option<&Path>,
    clients: &[PathBuf],
) -> (ObservedAttribution, Option<String>) {
    let Some(session_id) = session_id else {
        return (
            undetermined("opencode"),
            Some("no native session is recorded for this incarnation".into()),
        );
    };
    let mut stores: Vec<PathBuf> = match root {
        Some(root) => match fs::read_dir(root) {
            Ok(entries) => entries
                .flatten()
                .map(|entry| entry.path())
                .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("db"))
                .collect(),
            Err(_) => Vec::new(),
        },
        None => Vec::new(),
    };
    stores.sort();

    let mut session_seen = false;
    let lookups = stores
        .iter()
        .map(|store| read_opencode_session(store, session_id))
        .chain(
            clients
                .iter()
                .map(|client| read_opencode_v2_session(client, session_id)),
        );
    for lookup in lookups {
        match lookup {
            OpencodeLookup::Reported {
                model,
                provider,
                effort,
            } => {
                return (
                    ObservedAttribution {
                        adapter: "opencode".into(),
                        model: ObservedField::reported(model),
                        provider: ObservedField::reported(provider),
                        effort: ObservedField::reported(effort),
                    },
                    None,
                );
            }
            OpencodeLookup::SessionWithoutTurn => session_seen = true,
            OpencodeLookup::Absent => {}
        }
    }
    let reason = if session_seen {
        "session has produced no assistant turn yet; observe again after its first reply"
    } else if stores.is_empty() && clients.is_empty() {
        "no OpenCode store or client is configured"
    } else {
        "session was not found in any OpenCode store or V2 service"
    };
    (undetermined("opencode"), Some(reason.into()))
}

/// How long one V2 client may take to answer before it counts as absent.
const OPENCODE_CLIENT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(3);

/// Ask an `OpenCode` V2 client for the session's newest assistant message.
///
/// The service records model, provider, and variant on every assistant
/// message, so the newest one is what is serving the session now. A client
/// that is missing, is V1 (no `api` command), times out, or answers with
/// anything but a message page counts as not knowing the session, never as
/// evidence about it.
fn read_opencode_v2_session(client: &Path, session_id: &str) -> OpencodeLookup {
    if session_id.is_empty()
        || !session_id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    {
        return OpencodeLookup::Absent;
    }
    let page = |query: &str| {
        run_bounded(
            client,
            &[
                "api",
                "GET",
                &format!("/api/session/{session_id}/message?{query}"),
            ],
        )
        .and_then(|stdout| serde_json::from_slice::<Value>(&stdout).ok())
        .and_then(|page| page.get("data").and_then(Value::as_array).cloned())
    };
    let Some(assistants) = page("type=assistant&order=desc&limit=1") else {
        return OpencodeLookup::Absent;
    };
    if let Some(model) = assistants.first().and_then(|message| message.get("model")) {
        return OpencodeLookup::Reported {
            model: nonempty_str(model.get("id")),
            provider: nonempty_str(model.get("providerID")),
            effort: nonempty_str(model.get("variant")),
        };
    }
    // The service answered for this session, so it exists; it has just not
    // produced an assistant turn yet.
    OpencodeLookup::SessionWithoutTurn
}

/// Run a short read-only command and return its stdout when it exits zero in time.
fn run_bounded(program: &Path, args: &[&str]) -> Option<Vec<u8>> {
    use std::io::Read;
    use std::process::{Command, Stdio};
    let mut child = Command::new(program)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .ok()?;
    let deadline = std::time::Instant::now() + OPENCODE_CLIENT_TIMEOUT;
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) if std::time::Instant::now() < deadline => {
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
            _ => {
                let _ = child.kill();
                let _ = child.wait();
                return None;
            }
        }
    };
    if !status.success() {
        return None;
    }
    let mut stdout = Vec::new();
    child.stdout.take()?.read_to_end(&mut stdout).ok()?;
    Some(stdout)
}

enum OpencodeLookup {
    Reported {
        model: Option<String>,
        provider: Option<String>,
        effort: Option<String>,
    },
    SessionWithoutTurn,
    Absent,
}

/// Read one store read-only so a live `OpenCode` process is never disturbed.
fn read_opencode_session(store: &Path, session_id: &str) -> OpencodeLookup {
    let Ok(connection) =
        rusqlite::Connection::open_with_flags(store, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
    else {
        return OpencodeLookup::Absent;
    };
    // Newest assistant row: a session can change model mid-run, and the current
    // answer is what a caller verifying a running agent is asking about.
    let latest: Result<(Option<String>, Option<String>, Option<String>), _> = connection.query_row(
        "SELECT json_extract(data, '$.modelID'),
                json_extract(data, '$.providerID'),
                json_extract(data, '$.variant')
         FROM message
         WHERE session_id = ?1 AND json_extract(data, '$.role') = 'assistant'
         ORDER BY time_created DESC, id DESC
         LIMIT 1",
        [session_id],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    );
    if let Ok((model, provider, effort)) = latest {
        return OpencodeLookup::Reported {
            model,
            provider,
            effort,
        };
    }
    let present: Result<i64, _> = connection.query_row(
        "SELECT COUNT(*) FROM message WHERE session_id = ?1",
        [session_id],
        |row| row.get(0),
    );
    match present {
        Ok(count) if count > 0 => OpencodeLookup::SessionWithoutTurn,
        _ => OpencodeLookup::Absent,
    }
}

fn nonempty_str(value: Option<&Value>) -> Option<String> {
    value
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn find_named_jsonl(root: &Path, session_id: &str) -> Option<PathBuf> {
    let direct = root.join(format!("{session_id}.jsonl"));
    if direct.is_file() {
        return Some(direct);
    }
    visit_files(root, &mut |path| {
        (path.file_stem().and_then(|stem| stem.to_str()) == Some(session_id)
            && path.extension().and_then(|ext| ext.to_str()) == Some("jsonl"))
        .then(|| path.to_path_buf())
    })
}

fn find_containing_jsonl(root: &Path, session_id: &str) -> Option<PathBuf> {
    visit_files(root, &mut |path| {
        if path.extension().and_then(|ext| ext.to_str()) != Some("jsonl") {
            return None;
        }
        let Ok(text) = fs::read_to_string(path) else {
            return None;
        };
        text.contains(session_id).then(|| path.to_path_buf())
    })
}

fn visit_files<T>(root: &Path, find: &mut impl FnMut(&Path) -> Option<T>) -> Option<T> {
    let entries = fs::read_dir(root).ok()?;
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            if let Some(found) = visit_files(&path, find) {
                return Some(found);
            }
        } else if let Some(found) = find(&path) {
            return Some(found);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grok_and_empty_sessions_are_undetermined() {
        let roots = SessionRoots::default();
        let grok = observe("grok", None, &roots);
        assert_eq!(grok.adapter, "grok");
        assert_eq!(grok.model, ObservedField::Undetermined);
        let empty = observe("codex", None, &roots);
        assert_eq!(empty.model, ObservedField::Undetermined);
        let unreadable = observe(
            "claude",
            Some(&serde_json::json!({"value":"missing"})),
            &roots,
        );
        assert_eq!(unreadable.model, ObservedField::Undetermined);
    }

    #[test]
    fn requested_json_omission_is_not_undetermined() {
        let requested = serde_json::to_value(RequestedAttribution::default()).expect("json");
        assert_eq!(requested, serde_json::json!({}));
        let observed = serde_json::to_value(ObservedField::Undetermined).expect("json");
        assert_eq!(observed, serde_json::json!({"status":"undetermined"}));
    }

    /// Build an `OpenCode` store the way `OpenCode` does: messages in `SQLite`, with
    /// model identity written on assistant rows only.
    fn opencode_store(path: &Path, rows: &[(&str, &str, Option<&str>, Option<&str>)]) {
        let connection = rusqlite::Connection::open(path).expect("open store");
        connection
            .execute_batch(
                "CREATE TABLE message (
                    id TEXT PRIMARY KEY,
                    session_id TEXT NOT NULL,
                    time_created INTEGER NOT NULL,
                    time_updated INTEGER NOT NULL,
                    data TEXT NOT NULL
                );",
            )
            .expect("schema");
        for (index, (session, role, model, provider)) in rows.iter().enumerate() {
            let data = serde_json::json!({
                "role": role,
                "modelID": model,
                "providerID": provider,
                "variant": Value::Null,
            });
            connection
                .execute(
                    "INSERT INTO message (id, session_id, time_created, time_updated, data)
                     VALUES (?1, ?2, ?3, ?3, ?4)",
                    rusqlite::params![
                        format!("msg-{index}"),
                        session,
                        i64::try_from(index).expect("index"),
                        data.to_string()
                    ],
                )
                .expect("insert");
        }
    }

    #[test]
    fn opencode_reads_the_store_that_holds_the_session() {
        let directory = tempfile::tempdir().expect("temp");
        let root = directory.path().join("opencode");
        fs::create_dir_all(&root).expect("dir");
        // The session lives in a non-default store, beside a decoy default one.
        opencode_store(
            &root.join("opencode.db"),
            &[("ses_other", "assistant", Some("wrong"), Some("wrong"))],
        );
        opencode_store(
            &root.join("opencode-stream-contribute.db"),
            &[
                ("ses_1", "user", None, None),
                ("ses_1", "assistant", Some("gpt-5.6-sol"), Some("openai")),
            ],
        );
        let roots = SessionRoots {
            opencode: Some(root),
            ..SessionRoots::default()
        };
        let (observed, reason) = observe_detailed(
            "opencode",
            Some(&serde_json::json!({"value":"ses_1"})),
            &roots,
        );
        assert_eq!(
            observed.model,
            ObservedField::Reported("gpt-5.6-sol".into())
        );
        assert_eq!(observed.provider, ObservedField::Reported("openai".into()));
        // variant is null, so effort is honestly undetermined rather than guessed.
        assert_eq!(observed.effort, ObservedField::Undetermined);
        assert_eq!(reason, None);
    }

    /// Write an executable fake `OpenCode` client that answers `api GET` for
    /// one session from canned pages, and fails like V1 for anything else.
    fn fake_opencode_client(dir: &Path, name: &str, assistant_page: &str) -> PathBuf {
        use std::os::unix::fs::PermissionsExt;
        let path = dir.join(name);
        fs::write(
            &path,
            format!(
                "#!/bin/sh\n\
                 [ \"$1\" = api ] || exit 1\n\
                 case \"$3\" in\n\
                 /api/session/ses_v2/message\\?type=assistant*) printf '%s' '{assistant_page}' ;;\n\
                 *) exit 1 ;;\n\
                 esac\n"
            ),
        )
        .expect("write fake client");
        fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).expect("chmod");
        path
    }

    /// A V2 session lives behind the service, not in a store under the root,
    /// so the client's newest assistant message is the observation.
    #[test]
    fn opencode_v2_session_is_read_through_the_client_after_v1_misses() {
        let directory = tempfile::tempdir().expect("temp");
        let root = directory.path().join("opencode");
        fs::create_dir_all(&root).expect("dir");
        opencode_store(&root.join("opencode.db"), &[("ses_v1", "user", None, None)]);
        let v1 = directory.path().join("v1-opencode");
        fs::write(&v1, "#!/bin/sh\nexit 1\n").expect("v1");
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&v1, fs::Permissions::from_mode(0o755)).expect("chmod");
        }
        let v2 = fake_opencode_client(
            directory.path(),
            "opencode2",
            r#"{"data":[{"type":"assistant","model":{"id":"gpt-6.1-sol","providerID":"openai","variant":"low"}}],"cursor":{}}"#,
        );
        let roots = SessionRoots {
            opencode: Some(root),
            opencode_clients: vec![directory.path().join("missing-client"), v1, v2],
            ..SessionRoots::default()
        };
        let (observed, reason) = observe_detailed(
            "opencode",
            Some(&serde_json::json!({"value":"ses_v2"})),
            &roots,
        );
        assert_eq!(
            observed.model,
            ObservedField::Reported("gpt-6.1-sol".into())
        );
        assert_eq!(observed.provider, ObservedField::Reported("openai".into()));
        assert_eq!(observed.effort, ObservedField::Reported("low".into()));
        assert_eq!(reason, None);
    }

    /// The service knowing a session that has not answered yet is the same
    /// "ask again later" as V1's store holding only the user turn.
    #[test]
    fn opencode_v2_session_without_a_turn_is_worth_asking_again() {
        let directory = tempfile::tempdir().expect("temp");
        let v2 = fake_opencode_client(directory.path(), "opencode", r#"{"data":[],"cursor":{}}"#);
        let roots = SessionRoots {
            opencode_clients: vec![v2],
            ..SessionRoots::default()
        };
        let (observed, reason) = observe_detailed(
            "opencode",
            Some(&serde_json::json!({"value":"ses_v2"})),
            &roots,
        );
        assert_eq!(observed.model, ObservedField::Undetermined);
        assert!(
            reason
                .as_deref()
                .is_some_and(|reason| reason.contains("no assistant turn")),
            "{reason:?}"
        );
        let (_, reason) = observe_detailed(
            "opencode",
            Some(&serde_json::json!({"value":"ses_other"})),
            &roots,
        );
        assert!(
            reason
                .as_deref()
                .is_some_and(|reason| reason.contains("not found")),
            "{reason:?}"
        );
    }

    #[test]
    fn opencode_separates_no_turn_yet_from_no_session() {
        let directory = tempfile::tempdir().expect("temp");
        let root = directory.path().join("opencode");
        fs::create_dir_all(&root).expect("dir");
        opencode_store(&root.join("opencode.db"), &[("ses_1", "user", None, None)]);
        let roots = SessionRoots {
            opencode: Some(root),
            ..SessionRoots::default()
        };

        // Present, but no assistant turn: undetermined, and worth asking again.
        let (observed, reason) = observe_detailed(
            "opencode",
            Some(&serde_json::json!({"value":"ses_1"})),
            &roots,
        );
        assert_eq!(observed.model, ObservedField::Undetermined);
        assert!(
            reason
                .as_deref()
                .expect("reason")
                .contains("no assistant turn"),
            "{reason:?}"
        );

        // Absent everywhere: also undetermined, but asking again will not help.
        let (_, missing) = observe_detailed(
            "opencode",
            Some(&serde_json::json!({"value":"ses_absent"})),
            &roots,
        );
        assert!(
            missing.as_deref().expect("reason").contains("not found"),
            "{missing:?}"
        );

        // No session recorded at all is a third, distinct reason.
        let (_, unbound) = observe_detailed("opencode", None, &roots);
        assert!(
            unbound
                .as_deref()
                .expect("reason")
                .contains("no native session"),
            "{unbound:?}"
        );
    }

    #[test]
    fn opencode_reports_the_newest_assistant_turn() {
        let directory = tempfile::tempdir().expect("temp");
        let root = directory.path().join("opencode");
        fs::create_dir_all(&root).expect("dir");
        opencode_store(
            &root.join("opencode.db"),
            &[
                ("ses_1", "assistant", Some("first-model"), Some("openai")),
                ("ses_1", "assistant", Some("second-model"), Some("openai")),
            ],
        );
        let roots = SessionRoots {
            opencode: Some(root),
            ..SessionRoots::default()
        };
        let observed = observe(
            "opencode",
            Some(&serde_json::json!({"value":"ses_1"})),
            &roots,
        );
        assert_eq!(
            observed.model,
            ObservedField::Reported("second-model".into())
        );
    }

    #[test]
    fn named_adapters_report_only_session_fields() {
        let directory = tempfile::tempdir().expect("temp");
        let claude_root = directory.path().join("claude");
        fs::create_dir_all(&claude_root).expect("dir");
        fs::write(
            claude_root.join("sess-1.jsonl"),
            "{\"type\":\"assistant\",\"message\":{\"model\":\"claude-opus\",\"provider\":\"anthropic\"}}\n",
        )
        .expect("write");
        let codex_root = directory.path().join("codex");
        fs::create_dir_all(codex_root.join("2026/08/12")).expect("dir");
        fs::write(
            codex_root.join("2026/08/12/rollout.jsonl"),
            "{\"type\":\"session_meta\",\"payload\":{\"session_id\":\"codex-1\",\"model\":\"o3\",\"model_provider\":\"openai\",\"effort\":\"high\"}}\n",
        )
        .expect("write");
        let opencode_root = directory.path().join("opencode");
        fs::create_dir_all(&opencode_root).expect("dir");
        opencode_store(
            &opencode_root.join("opencode.db"),
            &[(
                "ses_1",
                "assistant",
                Some("opencode-model"),
                Some("opencode"),
            )],
        );
        let roots = SessionRoots {
            claude: Some(claude_root),
            codex: Some(codex_root),
            opencode: Some(opencode_root),
            opencode_clients: Vec::new(),
        };
        let claude = observe(
            "claude",
            Some(&serde_json::json!({"value":"sess-1"})),
            &roots,
        );
        assert_eq!(claude.model, ObservedField::Reported("claude-opus".into()));
        assert_eq!(claude.provider, ObservedField::Reported("anthropic".into()));
        let codex = observe(
            "codex",
            Some(&serde_json::json!({"kind":"id","value":"codex-1"})),
            &roots,
        );
        assert_eq!(codex.model, ObservedField::Reported("o3".into()));
        assert_eq!(codex.provider, ObservedField::Reported("openai".into()));
        assert_eq!(codex.effort, ObservedField::Reported("high".into()));
        let opencode = observe(
            "opencode",
            Some(&serde_json::json!({"value":"ses_1"})),
            &roots,
        );
        assert_eq!(
            opencode.model,
            ObservedField::Reported("opencode-model".into())
        );
        let requested = RequestedAttribution {
            model: Some("ignored".into()),
            provider: None,
            effort: None,
        };
        let grok = observe("grok", Some(&serde_json::json!({"value":"sess-1"})), &roots);
        assert_eq!(grok.model, ObservedField::Undetermined);
        assert_ne!(
            serde_json::to_value(&requested.model).expect("req"),
            serde_json::to_value(&grok.model).expect("obs")
        );
    }
}
