use std::env;
use std::fs;
use std::io::{self, BufRead, BufReader, Write};
use std::net::Shutdown;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::{SystemTime, UNIX_EPOCH};

use kelpie::cli::{
    AdoptArgs, AgentTarget, AttributionTarget, BodySource, Caller, Command, Due, ExactRecipient,
    Invocation, Recipient, StartCommand, StartParent, attribution_params, command_usage,
    env_caller, format_receipt, generated_id, message_params, parse_invocation, read_body,
    read_raw_request, typed_request, usage, whoami_params,
};
use kelpie::domain::Parent;
use serde_json::{Value, json};

fn main() -> ExitCode {
    ignore_sigpipe();
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("kelpie: {error}");
            ExitCode::FAILURE
        }
    }
}

fn ignore_sigpipe() {
    #[cfg(unix)]
    {
        const SIGPIPE: i32 = 13;
        const SIG_IGN: usize = 1;
        // SAFETY: SIG_IGN for SIGPIPE is a standard disposition with no ownership.
        unsafe {
            unsafe extern "C" {
                fn signal(sig: i32, handler: usize) -> usize;
            }
            let _ = signal(SIGPIPE, SIG_IGN);
        }
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().skip(1).collect();
    match parse_invocation(&args).map_err(io::Error::other)? {
        Invocation::Skill => {
            print!("{}", kelpie::SKILL);
            Ok(())
        }
        Invocation::Version => {
            println!("kelpie {}", env!("CARGO_PKG_VERSION"));
            Ok(())
        }
        Invocation::Help { command } => {
            println!(
                "{}",
                command
                    .as_deref()
                    .and_then(command_usage)
                    .unwrap_or_else(|| usage().to_string())
            );
            Ok(())
        }
        Invocation::Raw { socket } => exchange(&socket, &read_raw_request(&mut io::stdin())?, true),
        Invocation::Typed {
            socket,
            json,
            command,
        } => {
            let request_id = generated_id();
            let (method, params) = build_typed(&socket, command, &request_id)?;
            let request = typed_request(&request_id, &method, &params);
            exchange(&socket, &serde_json::to_string(&request)?, json)
        }
    }
}

#[allow(clippy::too_many_lines)]
fn build_typed(
    socket: &Path,
    command: Command,
    request_id: &str,
) -> Result<(String, Value), Box<dyn std::error::Error>> {
    match command {
        Command::Tell {
            recipient,
            body,
            sender,
            idempotency_key,
            due,
            every_ms,
        } => Ok(("tell".into(), {
            let mut params = message_command(
                socket,
                recipient,
                &body,
                sender,
                idempotency_key,
                resolve_due(due)?,
                request_id,
            )?;
            if let Some(every_ms) = every_ms {
                params["every_ms"] = json!(every_ms);
            }
            params
        })),
        Command::Clear {
            recipient,
            idempotency_key,
        } => {
            let mut params = json!({
                "idempotency_key": idempotency_key.unwrap_or_else(generated_id),
            });
            match recipient {
                Recipient::Alias(name) => params["recipient_alias"] = json!(name),
                Recipient::Exact(exact) => {
                    params["recipient"] = json!(exact.recipient);
                    params["recipient_incarnation"] = json!(exact.incarnation);
                }
                Recipient::Agent(_) => {
                    return Err("clear requires an alias or exact incarnation".into());
                }
            }
            Ok(("clear".into(), params))
        }
        Command::Renew(renew) => {
            let key = renew.idempotency_key.unwrap_or_else(generated_id);
            let caller = resolve_caller(
                socket,
                renew.requester,
                &format!("{request_id}:{key}:requester"),
            )?;
            let requester = caller.0.clone();
            let prepare_prompt = read_body(&renew.prepare_prompt, &mut io::stdin())?;
            let resume_prompt = read_body(&renew.prompt, &mut io::stdin())?;
            let mut params = json!({
                "requester": requester,
                "prepare_prompt": prepare_prompt,
                "prompt": resume_prompt,
                "on_timeout": renew.on_timeout,
                "prepare_timeout_ms": renew.prepare_timeout_ms,
            });
            // No recipient means the caller renews itself, resolved from the
            // same whoami that named the requester. A renew never resolves an
            // alias: the target is fixed here, before anything is armed.
            let exact = if let Some(exact) = renew.recipient {
                exact
            } else {
                if caller.1.is_empty() {
                    return Err(
                        "cannot renew yourself when the caller is given as a bare agent \
                                id: a policy binds to one incarnation, so pass --recipient-id \
                                with --recipient-incarnation"
                            .into(),
                    );
                }
                ExactRecipient {
                    recipient: caller.0,
                    incarnation: caller.1,
                }
            };
            params["recipient"] = json!(exact.recipient);
            params["recipient_incarnation"] = json!(exact.incarnation);
            if let Some(due_at_ms) = resolve_due(renew.due)? {
                params["due_at_ms"] = json!(due_at_ms);
            }
            if let Some(every_ms) = renew.every_ms {
                params["every_ms"] = json!(every_ms);
            }
            Ok(("renew".into(), params))
        }
        Command::Ask {
            recipient,
            body,
            sender,
            idempotency_key,
            due,
            remind_after_ms,
            no_remind,
            from_operator,
        } => Ok(("ask".into(), {
            let mut params = message_command(
                socket,
                recipient,
                &body,
                sender,
                idempotency_key,
                resolve_due(due)?,
                request_id,
            )?;
            if let Some(interval) = remind_after_ms {
                params["remind_after_ms"] = json!(interval);
            }
            if no_remind {
                params["no_remind"] = json!(true);
            }
            if from_operator {
                params["from_operator"] = json!(true);
            }
            params
        })),
        Command::Reply {
            reply_to,
            requester,
            body,
            disposition,
            idempotency_key,
        } => {
            let agent = resolve_caller(socket, requester, request_id)?.0;
            Ok((
                "reply".into(),
                json!({
                    "reply_to": reply_to,
                    "requester_agent_id": agent,
                    "body": read_body(&body, &mut io::stdin())?,
                    "disposition": disposition,
                    "idempotency_key": idempotency_key.unwrap_or_else(generated_id),
                }),
            ))
        }
        Command::Pending { target } => {
            let agent = resolve_caller(socket, target, request_id)?.0;
            Ok(("pending".into(), json!({"agent_id": agent})))
        }
        Command::Recover => Ok(("recover".into(), json!({}))),
        Command::NameInfo { name } => Ok(("name.info".into(), json!({ "name": name }))),
        Command::AskInfo { ask_id } => Ok(("ask.info".into(), json!({ "ask_message_id": ask_id }))),
        Command::Whoami { target } => {
            let caller = target
                .or_else(env_caller)
                .ok_or("cannot resolve caller; set HERDR_PANE_ID or pass --sender/--pane")?;
            if let Caller::Id(id) = caller {
                return Err(format!("whoami needs a pane or alias, not {id}").into());
            }
            let mut params = whoami_params(&caller);
            params["lazy_adopt_key"] = json!(format!("{request_id}:lazy-adopt:self"));
            Ok(("whoami".into(), params))
        }
        Command::Who {
            target,
            adopt_caller,
            history,
            refresh,
        } => {
            let target = match target {
                Some(target) => target,
                None => match env_caller() {
                    Some(Caller::Pane(pane)) => AttributionTarget::Pane(pane),
                    _ => {
                        return Err(
                            "cannot resolve caller; set HERDR_PANE_ID or pass a target".into()
                        );
                    }
                },
            };
            let mut params = attribution_params(&target);
            params["history"] = json!(history);
            params["refresh"] = json!(refresh);
            if adopt_caller {
                params["lazy_adopt_key"] = json!(format!("{request_id}:lazy-adopt:self"));
            }
            Ok(("who".into(), params))
        }
        Command::Rename { target, name } => {
            let mut params = json!({"name": name});
            match target {
                Some(Caller::Id(id)) => params["agent_id"] = json!(id),
                Some(Caller::Alias(alias)) => params["alias"] = json!(alias),
                _ => {
                    let agent = resolve_caller(socket, target, request_id)?.0;
                    params["agent_id"] = json!(agent);
                }
            }
            Ok(("rename".into(), params))
        }
        Command::Report { live, active } => {
            Ok(("report".into(), json!({"live": live, "active": active})))
        }
        Command::Attribution { target, refresh } => {
            let mut params = attribution_params(&target);
            if refresh {
                params["refresh"] = json!(true);
            }
            Ok(("attribution".into(), params))
        }
        Command::Start(start) => {
            let StartCommand {
                public_name,
                logical_agent_id,
                parent,
                herdr_session,
                pane_id,
                terminal_id,
                backend_kind,
                backend_args,
                initial_kind,
                initial_sender,
                body,
                working_directory,
                idempotency_key,
                readiness_timeout_ms,
                keep_open,
                requested_model,
                requested_provider,
                requested_effort,
                supersedes,
            } = *start;
            // An ask opens an obligation, so it needs an agent waiting identity.
            // Resolve the caller when none was given, the way tell and ask do; a
            // tell stays operator-attributed unless --sender-id says otherwise.
            let initial_sender = match (initial_sender, initial_kind) {
                (Some(sender), _) => Some(sender),
                (None, "ask") => Some(resolve_caller(socket, None, request_id)?.0),
                (None, _) => None,
            };
            let initial_message = json!({
                "sender": initial_sender,
                "kind": initial_kind,
                "body": read_body(&body, &mut io::stdin())?,
            });
            let mut params = json!({
                "public_name": public_name,
                "parent": match parent {
                    StartParent::Parentless => json!({"kind": "parentless"}),
                    StartParent::Agent(id) => json!({"kind": "agent", "agent_id": id}),
                },
                "herdr_session": herdr_session,
                "pane_id": pane_id,
                "expected_terminal_id": terminal_id,
                "backend_kind": backend_kind,
                "backend_args": backend_args,
                "initial_message": initial_message,
                "working_directory": working_directory,
                "idempotency_key": idempotency_key.unwrap_or_else(generated_id),
                "readiness_timeout_ms": readiness_timeout_ms,
                "keep_open": keep_open,
            });
            if let Some(id) = logical_agent_id {
                params["logical_agent_id"] = json!(id);
            }
            if let Some(model) = requested_model {
                params["requested_model"] = json!(model);
            }
            if let Some(provider) = requested_provider {
                params["requested_provider"] = json!(provider);
            }
            if let Some(effort) = requested_effort {
                params["requested_effort"] = json!(effort);
            }
            if let Some(predecessor) = supersedes {
                params["supersedes"] = json!(predecessor);
                return Ok(("handoff".into(), params));
            }
            Ok(("start".into(), params))
        }
        Command::Adopt(adopt) => {
            let AdoptArgs {
                pane_id,
                terminal_id,
                public_name,
                backend_kind,
                logical_agent_id,
                herdr_session,
                backend_args,
                requested_model,
                requested_provider,
                requested_effort,
                idempotency_key,
            } = *adopt;
            let mut params = json!({
                "pane_id": pane_id,
                "expected_terminal_id": terminal_id,
                "parent": Parent::Parentless,
                "herdr_session": herdr_session,
                "backend_args": backend_args,
                "idempotency_key": idempotency_key.unwrap_or_else(generated_id),
            });
            for (key, value) in [
                ("requested_model", requested_model),
                ("requested_provider", requested_provider),
                ("requested_effort", requested_effort),
            ] {
                if let Some(value) = value {
                    params[key] = json!(value);
                }
            }
            if let Some(name) = public_name {
                params["public_name"] = json!(name);
            }
            if let Some(kind) = backend_kind {
                params["backend_kind"] = json!(kind);
            }
            if let Some(id) = logical_agent_id {
                params["logical_agent_id"] = json!(id);
            }
            Ok(("adopt".into(), params))
        }
        Command::Notice { body } => Ok((
            "notice.create".into(),
            json!({"body": read_body(&body, &mut io::stdin())?}),
        )),
        Command::Notices => Ok(("notice.list".into(), json!({}))),
        Command::Cancel {
            ask_id,
            reason,
            requester,
        } => {
            let agent = resolve_caller(socket, requester, request_id)?.0;
            Ok((
                "cancel".into(),
                json!({
                    "requester_agent_id": agent,
                    "ask_message_id": ask_id,
                    "reason": reason
                }),
            ))
        }
        Command::RenewCancel {
            renew_id,
            reason,
            requester,
        } => {
            let agent = resolve_caller(socket, requester, request_id)?.0;
            Ok((
                "renew.cancel".into(),
                json!({
                    "requester_agent_id": agent,
                    "renew_id": renew_id,
                    "reason": reason
                }),
            ))
        }
        Command::ScheduleCancel {
            schedule_id,
            reason,
            requester,
        } => {
            let agent = resolve_caller(socket, requester, request_id)?.0;
            Ok((
                "schedule.cancel".into(),
                json!({
                    "requester_agent_id": agent,
                    "schedule_id": schedule_id,
                    "reason": reason
                }),
            ))
        }
        Command::Schedules { target } => {
            let agent = resolve_caller(socket, target, request_id)?.0;
            Ok(("schedule.list".into(), json!({"agent_id": agent})))
        }
        Command::ReminderSnooze {
            ask_id,
            until_ms,
            requester,
        } => {
            let agent = resolve_caller(socket, requester, request_id)?.0;
            Ok((
                "reminder.snooze".into(),
                json!({
                    "requester_agent_id": agent,
                    "ask_message_id": ask_id,
                    "until_ms": until_ms
                }),
            ))
        }
        Command::ReminderDisable { ask_id, requester } => {
            let agent = resolve_caller(socket, requester, request_id)?.0;
            Ok((
                "reminder.disable".into(),
                json!({"requester_agent_id": agent, "ask_message_id": ask_id}),
            ))
        }
        Command::Retire {
            incarnation_id,
            idempotency_key,
            close_pane,
        } => Ok((
            "retire".into(),
            json!({
                "incarnation_id": incarnation_id,
                "idempotency_key": idempotency_key.unwrap_or_else(generated_id),
                "close_pane": close_pane,
            }),
        )),
        Command::WaiterRegister {
            public_name,
            parent,
            idempotency_key,
        } => Ok((
            "waiter.register".into(),
            json!({
                "public_name": public_name,
                "parent": match parent {
                    StartParent::Parentless => json!({"kind": "parentless"}),
                    StartParent::Agent(id) => json!({"kind": "agent", "agent_id": id}),
                },
                "idempotency_key": idempotency_key.unwrap_or_else(generated_id),
            }),
        )),
        Command::WaiterRetire { target } => Ok((
            "waiter.retire".into(),
            match target {
                AgentTarget::Id(id) => json!({ "logical_agent_id": id }),
                AgentTarget::Alias(alias) => json!({ "alias": alias }),
            },
        )),
    }
}

/// Turn a requested delivery time into the epoch milliseconds the daemon takes.
///
/// The clock is consulted here rather than in the parser so parsing stays pure
/// and testable, and so a relative time is measured from the moment the request
/// is actually built.
fn resolve_due(due: Option<Due>) -> Result<Option<i64>, Box<dyn std::error::Error>> {
    match due {
        None => Ok(None),
        Some(Due::AtMs(at_ms)) => Ok(Some(at_ms)),
        Some(Due::InMs(offset)) => {
            let now = SystemTime::now().duration_since(UNIX_EPOCH)?;
            let now_ms = i64::try_from(now.as_millis()).map_err(|_| "clock is out of range")?;
            Ok(Some(
                now_ms.checked_add(offset).ok_or("--due-in overflowed")?,
            ))
        }
    }
}

fn message_command(
    socket: &Path,
    recipient: Recipient,
    body: &BodySource,
    sender: Option<Caller>,
    idempotency_key: Option<String>,
    due_at_ms: Option<i64>,
    request_id: &str,
) -> Result<Value, Box<dyn std::error::Error>> {
    let key = idempotency_key.unwrap_or_else(generated_id);
    let sender = resolve_caller(socket, sender, &format!("{request_id}:{key}:sender"))?.0;
    let body = read_body(body, &mut io::stdin())?;
    match recipient {
        Recipient::Alias(name) => Ok(message_params(
            &sender,
            Some(&name),
            None,
            &body,
            &key,
            due_at_ms,
        )),
        Recipient::Exact(exact) => Ok(message_params(
            &sender,
            None,
            Some(&exact),
            &body,
            &key,
            due_at_ms,
        )),
        Recipient::Agent(id) => {
            let mut params = json!({
                "sender": sender,
                "recipient": id,
                "body": body,
                "idempotency_key": key,
            });
            if let Some(due_at_ms) = due_at_ms {
                params["due_at_ms"] = json!(due_at_ms);
            }
            Ok(params)
        }
    }
}

fn resolve_caller(
    socket: &Path,
    caller: Option<Caller>,
    request_id: &str,
) -> Result<(String, String, String), Box<dyn std::error::Error>> {
    let caller = caller.or_else(env_caller).ok_or(
        "cannot resolve caller; set HERDR_PANE_ID or pass --sender / --sender-id / --pane",
    )?;
    if let Caller::Id(id) = caller {
        return Ok((id, String::new(), String::new()));
    }
    let mut params = whoami_params(&caller);
    params["lazy_adopt_key"] = json!(format!("{request_id}:lazy-adopt:self"));
    let request = typed_request(&generated_id(), "whoami", &params);
    let (_, response) = rpc(socket, &serde_json::to_string(&request)?, "whoami", false)?;
    if let Some(error) = response.get("error").filter(|error| !error.is_null()) {
        return Err(format!(
            "{}: {}",
            error
                .get("class")
                .and_then(Value::as_str)
                .unwrap_or("error"),
            error
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("whoami failed")
        )
        .into());
    }
    let result = &response["result"];
    Ok((
        field(result, "logical_agent_id"),
        field(result, "incarnation_id"),
        field(result, "public_name"),
    ))
}

fn exchange(
    socket: &Path,
    request: &str,
    json_mode: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let request_value: Value = serde_json::from_str(request)?;
    let method = request_value
        .get("method")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let (raw, response) = rpc(socket, request.trim_end(), &method, json_mode)?;
    if !json_mode {
        let _ = write_stdout(format_receipt(&method, &response).as_bytes());
    }
    let _ = raw;
    fail_if_error(&response)
}

fn rpc(
    socket: &Path,
    request: &str,
    method: &str,
    print_raw: bool,
) -> Result<(String, Value), Box<dyn std::error::Error>> {
    let mut trace = ClientTrace::new();
    trace.request_id = serde_json::from_str::<Value>(request)
        .ok()
        .and_then(|value| value.get("id")?.as_str().map(ToOwned::to_owned))
        .unwrap_or_default();
    trace.method = method.to_string();

    let mut stream = UnixStream::connect(socket)?;
    trace.mark("connected");
    stream.write_all(request.as_bytes())?;
    stream.write_all(b"\n")?;
    stream.flush()?;
    stream.shutdown(Shutdown::Write)?;
    trace.mark("request_sent");

    let mut response = String::new();
    BufReader::new(stream).read_line(&mut response)?;
    trace.mark("response_received");
    if response.trim().is_empty() {
        trace.note = "empty_response".into();
        trace.write_files();
        return Err("daemon closed the connection without a correlated response".into());
    }

    let response_bytes = response.as_bytes();
    let receipt_path = write_receipt(response_bytes)?;
    trace.receipt_path = receipt_path.display().to_string();
    trace.receipt_bytes = response_bytes.len();
    trace.mark("receipt_written");

    if print_raw {
        match write_stdout(response_bytes) {
            Ok(()) => {
                trace.stdout_written = true;
                trace.mark("stdout_written");
            }
            Err(error) => {
                trace.stdout_written = false;
                trace.stdout_error = error.to_string();
                trace.mark("stdout_failed");
            }
        }
    }
    trace.write_files();
    let parsed = serde_json::from_str(&response)?;
    Ok((response, parsed))
}

fn write_stdout(bytes: &[u8]) -> io::Result<()> {
    let mut stdout = io::stdout();
    stdout.write_all(bytes)?;
    stdout.flush()
}

fn write_receipt(bytes: &[u8]) -> io::Result<PathBuf> {
    if let Some(path) = env::var_os("KELPIE_RECEIPT_PATH") {
        let path = PathBuf::from(path);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        atomic_write(&path, bytes)?;
        return Ok(path);
    }
    let dir = client_state_dir();
    fs::create_dir_all(&dir)?;
    let path = dir.join("last-response.ndjson");
    atomic_write(&path, bytes)?;
    Ok(path)
}

fn client_state_dir() -> PathBuf {
    if let Some(dir) = env::var_os("XDG_RUNTIME_DIR") {
        return PathBuf::from(dir).join("kelpie");
    }
    env::temp_dir().join("kelpie-client")
}

fn atomic_write(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let tmp = path.with_extension(format!(
        "tmp-{}-{}",
        std::process::id(),
        now_millis().unwrap_or(0)
    ));
    {
        let mut file = fs::File::create(&tmp)?;
        file.write_all(bytes)?;
        if !bytes.ends_with(b"\n") {
            file.write_all(b"\n")?;
        }
        file.sync_all()?;
    }
    fs::rename(&tmp, path)?;
    Ok(())
}

fn now_millis() -> Option<u128> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .map(|duration| duration.as_millis())
}

fn fail_if_error(response: &Value) -> Result<(), Box<dyn std::error::Error>> {
    if response.get("error").is_some_and(|error| !error.is_null()) {
        return Err("request failed".into());
    }
    if let Some(outcome) = response
        .get("result")
        .and_then(|result| result.get("delivery_outcome"))
        .and_then(Value::as_str)
        && outcome != "accepted"
        && outcome != "queued"
    {
        return Err(format!("delivery {outcome}").into());
    }
    if let Some(outcome) = response
        .get("result")
        .and_then(|result| result.pointer("/runtime_start/outcome"))
        .and_then(Value::as_str)
        && outcome != "succeeded"
    {
        return Err(format!("runtime {outcome}").into());
    }
    if let Some(outcome) = response
        .get("result")
        .and_then(|result| result.pointer("/initial_message/outcome"))
        .and_then(Value::as_str)
        && outcome != "accepted"
    {
        return Err(format!("initial message {outcome}").into());
    }
    Ok(())
}

fn field(value: &Value, name: &str) -> String {
    value
        .get(name)
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string()
}

#[derive(Default)]
struct ClientTrace {
    request_id: String,
    method: String,
    marks: Vec<(String, u128)>,
    receipt_path: String,
    receipt_bytes: usize,
    stdout_written: bool,
    stdout_error: String,
    note: String,
}

impl ClientTrace {
    fn new() -> Self {
        Self::default()
    }

    fn mark(&mut self, name: &str) {
        if let Some(ms) = now_millis() {
            self.marks.push((name.to_string(), ms));
        }
    }

    fn write_files(&self) {
        let dir = client_state_dir();
        let _ = fs::create_dir_all(&dir);
        let path = dir.join("last-client-trace.json");
        let marks: Vec<Value> = self
            .marks
            .iter()
            .map(|(name, ms)| json!({"name": name, "at_ms": ms}))
            .collect();
        let body = json!({
            "request_id": self.request_id,
            "method": self.method,
            "marks": marks,
            "receipt_path": self.receipt_path,
            "receipt_bytes": self.receipt_bytes,
            "stdout_written": self.stdout_written,
            "stdout_error": self.stdout_error,
            "note": self.note,
        });
        if let Ok(bytes) = serde_json::to_vec_pretty(&body) {
            let _ = atomic_write(&path, &bytes);
        }
    }
}
