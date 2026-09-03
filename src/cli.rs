//! Typed `kelpie` command line. NDJSON remains the socket protocol.

use std::fmt::Write as _;
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};

use serde_json::{Value, json};

use crate::domain::format_duration_ms;
use uuid::Uuid;

/// Everything `adopt` needs. Boxed in [`Command`] because it is much larger
/// than every other variant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdoptArgs {
    pub pane_id: String,
    pub terminal_id: String,
    pub public_name: Option<String>,
    pub backend_kind: Option<String>,
    pub logical_agent_id: Option<String>,
    pub herdr_session: String,
    pub backend_args: Vec<String>,
    pub requested_model: Option<String>,
    pub requested_provider: Option<String>,
    pub requested_effort: Option<String>,
    pub idempotency_key: Option<String>,
}

/// How the client should speak to the daemon.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Invocation {
    /// Print the canonical skill text.
    Skill,
    /// Print the package version.
    Version,
    /// Print usage.
    Help,
    /// Legacy one-JSON-request mode against an explicit socket.
    Raw { socket: PathBuf },
    /// Schema-aware command that builds the NDJSON request.
    Typed {
        socket: PathBuf,
        json: bool,
        command: Command,
    },
}

/// One typed client command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    Tell {
        recipient: Recipient,
        body: BodySource,
        sender: Option<Caller>,
        idempotency_key: Option<String>,
        due: Option<Due>,
    },
    Ask {
        recipient: Recipient,
        body: BodySource,
        sender: Option<Caller>,
        idempotency_key: Option<String>,
        due: Option<Due>,
        remind_after_ms: Option<i64>,
        no_remind: bool,
        from_operator: bool,
    },
    Reply {
        reply_to: String,
        requester: Option<Caller>,
        body: BodySource,
        disposition: &'static str,
        idempotency_key: Option<String>,
    },
    Clear {
        recipient: Recipient,
        idempotency_key: Option<String>,
    },
    /// Boxed: a renew carries two prompts and a schedule, and inlining it would
    /// make every `Command` pay for the largest variant.
    Renew(Box<RenewArgs>),
    Pending {
        target: Option<Caller>,
    },
    Recover,
    Whoami {
        target: Option<Caller>,
    },
    Attribution {
        target: AttributionTarget,
        refresh: bool,
    },
    Report {
        live: bool,
        active: bool,
    },
    NameInfo {
        name: String,
    },
    AskInfo {
        ask_id: String,
    },
    Rename {
        target: Option<Caller>,
        name: String,
    },
    Adopt(Box<AdoptArgs>),
    Notice {
        body: BodySource,
    },
    Notices,
    Cancel {
        ask_id: String,
        reason: String,
        requester: Option<Caller>,
    },
    /// End a renew policy before its incarnation does.
    RenewCancel {
        renew_id: String,
        reason: String,
        requester: Option<Caller>,
    },
    ReminderSnooze {
        ask_id: String,
        until_ms: i64,
        requester: Option<Caller>,
    },
    ReminderDisable {
        ask_id: String,
        requester: Option<Caller>,
    },
    Retire {
        incarnation_id: String,
        idempotency_key: Option<String>,
        close_pane: bool,
    },
    /// Launch one incarnation using the existing start contract.
    Start(Box<StartCommand>),
    WaiterRegister {
        public_name: String,
        parent: StartParent,
        idempotency_key: Option<String>,
    },
    WaiterRetire {
        logical_agent_id: String,
    },
}

/// `StartIntent` parent: parentless or an exact parent agent ID.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StartParent {
    Parentless,
    Agent(String),
}

/// Parsed typed start. Fields match [`crate::domain::StartIntent`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartCommand {
    pub public_name: String,
    pub logical_agent_id: Option<String>,
    pub parent: StartParent,
    pub herdr_session: String,
    pub pane_id: String,
    pub terminal_id: String,
    pub backend_kind: String,
    pub backend_args: Vec<String>,
    pub initial_kind: &'static str,
    pub initial_sender: Option<String>,
    pub body: BodySource,
    pub working_directory: String,
    pub idempotency_key: Option<String>,
    pub readiness_timeout_ms: u64,
    pub keep_open: bool,
    pub requested_model: Option<String>,
    pub requested_provider: Option<String>,
    pub requested_effort: Option<String>,
    /// Incarnation this start replaces, for `handoff`.
    pub supersedes: Option<String>,
}

/// Caller identity supplied on the command line.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Caller {
    Alias(String),
    Id(String),
    Pane(String),
}

/// Which identity an attribution lookup names.
///
/// `Incarnation` is the exact form. `Agent` resolves to that agent's newest
/// incarnation. `Alias` requires a live Ready binding. `Pane` defaults to the
/// calling pane and never adopts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AttributionTarget {
    Incarnation(String),
    Agent(String),
    Alias(String),
    Pane(String),
}

/// Exact recipient IDs for recovery use.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExactRecipient {
    pub recipient: String,
    pub incarnation: String,
}

/// One allowed recipient addressing form.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Recipient {
    /// Live public name resolved at send time.
    Alias(String),
    /// Exact durable IDs.
    Exact(ExactRecipient),
}

/// Everything one renew needs: two prompts, a disposition, and a schedule.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RenewArgs {
    /// Exact IDs only, and `None` means the caller's own incarnation.
    ///
    /// A renew has no alias form. An alias is a live address another agent may
    /// hold, and aiming a policy at the wrong one is unrecoverable: it clears a
    /// stranger's conversation once a cycle. Exact IDs cannot drift onto a
    /// different agent between typing and arming.
    pub recipient: Option<ExactRecipient>,
    pub requester: Option<Caller>,
    /// Delivered as an ask; its final reply is the ready signal.
    pub prepare_prompt: BodySource,
    /// Injected after the clear. With `every_ms` it runs on every cycle for the
    /// life of the agent, so it must be reentrant.
    pub prompt: BodySource,
    pub on_timeout: &'static str,
    pub prepare_timeout_ms: i64,
    pub every_ms: Option<i64>,
    pub due: Option<Due>,
    pub idempotency_key: Option<String>,
}

/// Where a message body is read from. The path/stdin bytes are not re-parsed
/// by a shell after the CLI starts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BodySource {
    Stdin,
    File(PathBuf),
    Literal(String),
}

/// Parse `argv` after the program name.
///
/// # Errors
///
/// Returns a usage string when flags conflict or a required value is missing.
pub fn parse_invocation(args: &[String]) -> Result<Invocation, String> {
    if args.is_empty() {
        return Ok(Invocation::Help);
    }
    if args.len() == 1 && (args[0] == "--skill") {
        return Ok(Invocation::Skill);
    }
    if args.len() == 1 && (args[0] == "--version" || args[0] == "-V") {
        return Ok(Invocation::Version);
    }
    if args.iter().any(|arg| arg == "--help" || arg == "-h") {
        return Ok(Invocation::Help);
    }

    let mut socket = None;
    let mut json = false;
    let mut rest = Vec::new();
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--socket" => {
                if socket.is_some() {
                    return Err("--socket specified more than once".into());
                }
                socket = Some(required_value(args, &mut index, "--socket")?);
            }
            "--json" => {
                if json {
                    return Err("--json specified more than once".into());
                }
                json = true;
                index += 1;
            }
            other if other.starts_with('-') => {
                rest.push(args[index].clone());
                index += 1;
            }
            _ => {
                rest.extend(args[index..].iter().cloned());
                break;
            }
        }
    }
    if rest.is_empty() {
        return Ok(Invocation::Help);
    }
    if is_raw_socket_token(&rest[0]) && rest.len() == 1 {
        return Ok(Invocation::Raw {
            socket: PathBuf::from(&rest[0]),
        });
    }
    let command = parse_command(&rest)?;
    Ok(Invocation::Typed {
        socket: PathBuf::from(socket.unwrap_or_else(default_socket)),
        json,
        command,
    })
}

/// Read the selected body source as raw bytes decoded as UTF-8.
///
/// # Errors
///
/// Returns an I/O or UTF-8 error. Does not interpret shell syntax.
pub fn read_body(source: &BodySource, stdin: &mut impl Read) -> Result<String, String> {
    match source {
        BodySource::Stdin => {
            let mut bytes = Vec::new();
            stdin
                .read_to_end(&mut bytes)
                .map_err(|error| error.to_string())?;
            String::from_utf8(bytes).map_err(|error| error.to_string())
        }
        BodySource::File(path) => fs::read_to_string(path).map_err(|error| error.to_string()),
        BodySource::Literal(text) => Ok(text.clone()),
    }
}

/// Build one daemon NDJSON request for a typed command after identities resolve.
#[must_use]
pub fn typed_request(id: &str, method: &str, params: &Value) -> Value {
    json!({"id": id, "method": method, "params": params})
}

/// Generate a unique request or idempotency identifier.
#[must_use]
pub fn generated_id() -> String {
    Uuid::now_v7().to_string()
}

/// Default live socket under `$XDG_RUNTIME_DIR/kelpie`.
#[must_use]
pub fn default_socket() -> String {
    crate::paths::kelpie_socket_path().display().to_string()
}

/// Format a typed receipt. Unknown and rejected outcomes stay visible.
#[must_use]
#[allow(clippy::too_many_lines)]
pub fn format_receipt(method: &str, response: &Value) -> String {
    if let Some(error) = response.get("error").filter(|error| !error.is_null()) {
        return format!(
            "{method} error class={} message={}\n",
            error.get("class").and_then(Value::as_str).unwrap_or("?"),
            error
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("request failed")
        );
    }
    let result = response.get("result").cloned().unwrap_or(Value::Null);
    match method {
        "tell" | "ask" => {
            // A queued delivery is a deferral, not a dispatch. Epoch milliseconds
            // are unreadable at a glance, and a reader who skims one has no way
            // to notice the message has not been sent yet.
            let due = match result.get("due_at_ms") {
                Some(Value::Number(number)) => number.as_i64().map_or(String::new(), |due_at| {
                    format!(
                        " NOT-SENT-UNTIL={} due-at-ms={due_at}",
                        format_utc_ms(due_at)
                    )
                }),
                _ => String::new(),
            };
            let reminder = if method == "ask" {
                match result.get("remind_after_ms") {
                    Some(Value::Number(number)) => format!(" reminder={number}"),
                    _ => " reminder=disabled".into(),
                }
            } else {
                String::new()
            };
            format!(
                "{method} message={} operation={} recipient={} delivery={}{due}{reminder}\n",
                field(&result, "message_id"),
                field(&result, "operation_id"),
                field(&result, "recipient"),
                field(&result, "delivery_outcome")
            )
        }
        "renew" => format_renew_receipt(&result),
        "clear" => format_clear_receipt(&result),
        "reply" => format!(
            "reply message={} delivery={} obligation={}\n",
            field(&result, "message_id"),
            field(&result, "delivery_outcome"),
            field(&result, "obligation_state")
        ),
        "whoami" => format!(
            "whoami name={} agent={} incarnation={}\n",
            field(&result, "public_name"),
            field(&result, "logical_agent_id"),
            field(&result, "incarnation_id")
        ),
        "start" => format!(
            "start agent={} incarnation={} runtime={} message={} delivery={}\n",
            field(&result, "logical_agent_id"),
            field(&result, "incarnation_id"),
            nested_field(&result, "runtime_start", "outcome"),
            field(&result["initial_message"], "message_id"),
            nested_field(&result, "initial_message", "outcome")
        ),
        "adopt" => format!(
            "adopt agent={} incarnation={} outcome={}\n",
            field(&result, "logical_agent_id"),
            field(&result, "incarnation_id"),
            field(&result, "outcome")
        ),
        "recover" => format!(
            "recover lost={} unknown={} starts={}\n",
            field(&result, "incarnations_marked_lost"),
            field(&result, "outcomes_marked_unknown"),
            field(&result, "starts_recovered")
        ),
        "attribution" => render_attribution(&result),
        "report" => render_report(&result),
        "name.info" => render_name_info(&result),
        "ask.info" => render_ask_info(&result),
        "waiter.register" => format!(
            "waiter-register agent={} name={} transport=socket_inbox\n",
            field(&result, "logical_agent_id"),
            field(&result, "public_name")
        ),
        "waiter.retire" => format_waiter_retire_receipt(&result),
        "rename" => format!(
            "rename name={} agent={} incarnation={}\n",
            field(&result, "public_name"),
            field(&result, "logical_agent_id"),
            field(&result, "incarnation_id")
        ),
        "notice.create" => format!("notice {}\n", field(&result, "notice_id")),
        "cancel" => format!(
            "cancel state={} response={}{} owing-response={}{}\n",
            field(&result, "state"),
            field(&result, "response"),
            result
                .get("message_id")
                .and_then(Value::as_str)
                .map_or(String::new(), |id| format!(" message={id}")),
            field(&result, "owing_response"),
            result
                .get("owing_message_id")
                .and_then(Value::as_str)
                .map_or(String::new(), |id| format!(" owing-message={id}"))
        ),
        "retire" => format!(
            "retire operation={} pane-released={}\n",
            field(&result, "operation_id"),
            field(&result, "pane_released")
        ),
        _ => format!("{result}\n"),
    }
}

fn format_clear_receipt(result: &Value) -> String {
    format!(
        "clear operation={} recipient={} outcome={}\n",
        field(result, "operation_id"),
        field(result, "recipient"),
        field(result, "outcome")
    )
}

fn format_renew_receipt(result: &Value) -> String {
    let schedule = match result.get("every_ms") {
        Some(Value::Number(number)) => format!(" every={number}ms"),
        _ => format!(" due={}", field(result, "scheduled_at_ms")),
    };
    format!(
        "renew {} recipient={} phase={} on-timeout={}{schedule}\n",
        field(result, "renew_id"),
        field(result, "recipient"),
        field(result, "phase"),
        field(result, "on_timeout")
    )
}

/// Usage text for `--help`.
#[must_use]
pub fn usage() -> &'static str {
    "\
kelpie --skill | --version
kelpie SOCKET                         # raw NDJSON request on stdin
kelpie [--socket PATH] [--json] COMMAND

Commands:
  tell <recipient> | --recipient-id ID --recipient-incarnation ID
       (--stdin | --file PATH | --body TEXT)
       [--due-in 10m | --due-at RFC3339 | --due-at-ms MS]
  ask  <recipient> | --recipient-id ID --recipient-incarnation ID
       (--stdin | --file PATH | --body TEXT)
       [--remind-after-ms MS | --no-remind]
  reply <ask-id> (--progress | --final) (--stdin | --file PATH | --body TEXT)
  clear <recipient> | --recipient-id ID --recipient-incarnation ID
  renew [--recipient-id ID --recipient-incarnation ID]
       (--prepare-prompt TEXT | --prepare-prompt-file PATH)
       (--prompt TEXT | --prompt-file PATH)
       --on-timeout (abort | proceed)
       [--prepare-timeout 10m | --prepare-timeout-ms MS]
       [--due-in 45m | --due-at RFC3339 | --due-at-ms MS | --every 45m]
  renew-cancel <renew-id> --reason TEXT
  pending [alias]
  recover
  whoami [alias]
  name-info <alias>
  ask-info <ask-id>
  attribution [alias] | --pane ID | --agent-id ID | --incarnation-id ID
       [--refresh]
  report [--live] [--active]
  rename [alias] | --sender-id ID --name NEW-NAME
  handoff --replace INCARNATION-ID <all start arguments>
  start --name NAME --pane ID --terminal ID --backend KIND --cwd PATH
       --timeout-ms N (--keep-open | --no-keep-open)
       (--parentless | --parent-id ID) (--tell | --ask)
       (--stdin | --file PATH | --body TEXT)
       [--arg ARG]... [--session NAME] [--logical-id ID] [--sender-id ID]
       [--requested-model NAME] [--requested-provider NAME] [--requested-effort NAME]
  adopt --pane ID --terminal ID [--name NAME] [--backend KIND]
       [--logical-id ID] [--session NAME]
  notice (--stdin | --file PATH | --body TEXT)
  notices
  cancel <ask-id> --reason TEXT
  reminder-snooze <ask-id> --until-ms MS
  reminder-disable <ask-id>
  retire --incarnation ID [--close-pane]
  waiter-register --name NAME (--parentless | --parent-id ID)
  waiter-retire --logical-id ID

Unknown, duplicate, conflicting, or extra arguments fail closed. Ordinary
bodies should use --stdin or --file. --body is only for short trusted text.
Use exactly one recipient form: a live name, or both exact IDs. Do not
invent a fake alias when addressing by ID. renew is the exception: it takes
no alias, and with no recipient it renews the caller. Only renew-cancel ends
a policy, and only its requester or its target may call it. Caller defaults to the Ready
binding for $HERDR_PANE_ID. Default receipts show accepted, rejected,
target-unavailable, and unknown outcomes; any non-success exits nonzero."
}

fn parse_command(args: &[String]) -> Result<Command, String> {
    match args[0].as_str() {
        "tell" | "ask" => parse_message_command(args),
        "reply" => parse_reply(args),
        "clear" => parse_clear(args),
        "renew" => parse_renew(args),
        "renew-cancel" => parse_renew_cancel(args),
        "pending" => parse_pending(args),
        "recover" => {
            let tokens = Tokens::new(&args[1..]);
            tokens.finish("recover")?;
            Ok(Command::Recover)
        }
        "whoami" => parse_whoami(args),
        "name-info" => parse_name_info(args),
        "ask-info" => parse_ask_info(args),
        "attribution" => parse_attribution(args),
        "rename" => {
            let mut tokens = Tokens::new(&args[1..]);
            let name = tokens.take_value("--name")?.ok_or("missing --name")?;
            let target = take_single_target(&mut tokens, "rename")?;
            tokens.finish("rename")?;
            Ok(Command::Rename { target, name })
        }
        "report" => {
            let mut tokens = Tokens::new(&args[1..]);
            let live = tokens.take_bool("--live")?;
            let active = tokens.take_bool("--active")?;
            tokens.finish("report")?;
            Ok(Command::Report { live, active })
        }
        "start" => parse_start(&args[1..], false),
        "handoff" => parse_start(&args[1..], true),
        "adopt" => parse_adopt(&args[1..]),
        "waiter-register" => parse_waiter_register(&args[1..]),
        "waiter-retire" => parse_waiter_retire(&args[1..]),
        "notice" => {
            let mut tokens = Tokens::new(&args[1..]);
            let body = take_body(&mut tokens, "notice")?;
            tokens.finish("notice")?;
            Ok(Command::Notice { body })
        }
        "notices" => {
            let tokens = Tokens::new(&args[1..]);
            tokens.finish("notices")?;
            Ok(Command::Notices)
        }
        "cancel" => parse_cancel(args),
        "reminder-snooze" => parse_reminder_snooze(args),
        "reminder-disable" => parse_reminder_disable(args),
        "retire" => parse_retire(&args[1..]),
        other => Err(format!("unknown command {other}")),
    }
}

fn parse_waiter_register(args: &[String]) -> Result<Command, String> {
    let mut tokens = Tokens::new(args);
    let mut problems = Problems::default();
    let public_name = problems.required(&mut tokens, "--name");
    let parent = take_parent(&mut problems, &mut tokens, "waiter-register");
    let idempotency_key = problems.value(&mut tokens, "--idempotency-key");
    problems.resolve(&tokens, "waiter-register")?;
    Ok(Command::WaiterRegister {
        public_name: public_name.ok_or("missing --name")?,
        parent: parent.ok_or("missing --parentless or --parent-id")?,
        idempotency_key,
    })
}

fn parse_waiter_retire(args: &[String]) -> Result<Command, String> {
    let mut tokens = Tokens::new(args);
    let logical_agent_id = tokens
        .take_value("--logical-id")?
        .ok_or("waiter-retire requires --logical-id")?;
    tokens.finish("waiter-retire")?;
    Ok(Command::WaiterRetire { logical_agent_id })
}

fn parse_clear(args: &[String]) -> Result<Command, String> {
    let mut tokens = Tokens::new(&args[1..]);
    let recipient_id = tokens.take_value("--recipient-id")?;
    let recipient_incarnation = tokens.take_value("--recipient-incarnation")?;
    let idempotency_key = tokens.take_value("--idempotency-key")?;
    let positional = tokens.take_positional();
    tokens.finish("clear")?;
    let recipient = match (positional, recipient_id, recipient_incarnation) {
        (Some(name), None, None) => Recipient::Alias(name),
        (None, Some(recipient), Some(incarnation)) => Recipient::Exact(ExactRecipient {
            recipient,
            incarnation,
        }),
        _ => {
            return Err(
                "clear requires exactly one of <recipient> or --recipient-id plus \
                 --recipient-incarnation"
                    .into(),
            );
        }
    };
    Ok(Command::Clear {
        recipient,
        idempotency_key,
    })
}

fn parse_message_command(args: &[String]) -> Result<Command, String> {
    let verb = args[0].as_str();
    let mut tokens = Tokens::new(&args[1..]);
    let recipient_id = tokens.take_value("--recipient-id")?;
    let recipient_incarnation = tokens.take_value("--recipient-incarnation")?;
    let sender = take_caller(&mut tokens)?;
    let body = take_body(&mut tokens, verb)?;
    let idempotency_key = tokens.take_value("--idempotency-key")?;
    let due = take_due(&mut tokens, verb)?;
    let reminder_values = tokens.take_all_values("--remind-after-ms")?;
    let no_remind = tokens.take_bool("--no-remind")?;
    let remind_after_ms = match reminder_values.as_slice() {
        [] => None,
        [value] if verb == "ask" => {
            let parsed = value
                .parse::<i64>()
                .map_err(|_| "ask --remind-after-ms must be a positive integer".to_string())?;
            if parsed <= 0 {
                return Err("ask --remind-after-ms must be a positive integer".into());
            }
            Some(parsed)
        }
        [_] => return Err("tell does not accept --remind-after-ms".into()),
        _ => return Err("--remind-after-ms specified more than once".into()),
    };
    if verb == "tell" && no_remind {
        return Err("tell does not accept --no-remind".into());
    }
    if remind_after_ms.is_some() && no_remind {
        return Err("ask accepts only one of --remind-after-ms or --no-remind".into());
    }
    let from = tokens.take_value("--from")?;
    let from_operator = match from.as_deref() {
        None => false,
        Some("operator") if verb == "ask" => true,
        Some("operator") => return Err("tell does not accept --from".into()),
        Some(_) => return Err("--from only accepts operator".into()),
    };
    let positional = tokens.take_positional();
    tokens.finish(verb)?;
    let recipient = match (positional, recipient_id, recipient_incarnation) {
        (Some(name), None, None) => Recipient::Alias(name),
        (None, Some(recipient), Some(incarnation)) => Recipient::Exact(ExactRecipient {
            recipient,
            incarnation,
        }),
        _ => {
            return Err(format!(
                "{verb} requires exactly one of <recipient> or --recipient-id plus --recipient-incarnation"
            ));
        }
    };
    if verb == "tell" {
        Ok(Command::Tell {
            recipient,
            body,
            sender,
            idempotency_key,
            due,
        })
    } else {
        Ok(Command::Ask {
            recipient,
            body,
            sender,
            idempotency_key,
            due,
            remind_after_ms,
            no_remind,
            from_operator,
        })
    }
}

/// Default window an agent gets to write its checkpoint before the deadline.
///
/// Generous on purpose: the agent may be mid-turn when the prepare arrives, and
/// the prompt queues behind that turn before it is even read.
const DEFAULT_PREPARE_TIMEOUT_MS: i64 = 10 * 60 * 1_000;

fn parse_renew(args: &[String]) -> Result<Command, String> {
    let mut tokens = Tokens::new(&args[1..]);
    let recipient_id = tokens.take_value("--recipient-id")?;
    let recipient_incarnation = tokens.take_value("--recipient-incarnation")?;
    let requester = take_caller(&mut tokens)?;
    let prepare_prompt = take_named_body(
        &mut tokens,
        "renew",
        "--prepare-prompt",
        "--prepare-prompt-file",
    )?;
    let prompt = take_named_body(&mut tokens, "renew", "--prompt", "--prompt-file")?;
    let idempotency_key = tokens.take_value("--idempotency-key")?;

    // No default. Aborting leaves a context growing; proceeding destroys
    // whatever the agent did not save. A caller who has not chosen has not
    // thought about it, and this is not a decision to guess on their behalf.
    let on_timeout = match tokens.take_value("--on-timeout")?.as_deref() {
        Some("abort") => "abort",
        Some("proceed") => "proceed",
        Some(other) => {
            return Err(format!(
                "--on-timeout must be abort or proceed, not {other}"
            ));
        }
        None => {
            return Err(
                "renew requires --on-timeout abort|proceed: there is no safe default when an \
                 agent never confirms its checkpoint"
                    .into(),
            );
        }
    };

    let timeout_values = tokens.take_all_values("--prepare-timeout")?;
    let timeout_ms_values = tokens.take_all_values("--prepare-timeout-ms")?;
    let prepare_timeout_ms = match (timeout_values.as_slice(), timeout_ms_values.as_slice()) {
        ([], []) => DEFAULT_PREPARE_TIMEOUT_MS,
        ([value], []) => parse_duration_for("--prepare-timeout", value)?,
        ([], [value]) => {
            let parsed = value
                .parse::<i64>()
                .map_err(|_| "--prepare-timeout-ms must be a positive integer".to_string())?;
            if parsed <= 0 {
                return Err("--prepare-timeout-ms must be a positive integer".into());
            }
            parsed
        }
        _ => return Err("use exactly one of --prepare-timeout or --prepare-timeout-ms".into()),
    };

    let every_values = tokens.take_all_values("--every")?;
    let every_ms = match every_values.as_slice() {
        [] => None,
        [value] => Some(parse_duration_for("--every", value)?),
        _ => return Err("--every specified more than once".into()),
    };
    let due = take_due(&mut tokens, "renew")?;
    if every_ms.is_some() && due.is_some() {
        return Err(
            "renew accepts either a one-shot due time or --every, not both: --every re-arms \
             itself after each cycle"
                .into(),
        );
    }

    let positional = tokens.take_positional();
    tokens.finish("renew")?;
    let recipient = match (positional, recipient_id, recipient_incarnation) {
        // Self-target. The duplicate refusal is scoped per incarnation, so it
        // only protects a caller aiming at itself. Making that the default
        // makes the re-arm probe safe by construction rather than by care.
        (None, None, None) => None,
        (None, Some(recipient), Some(incarnation)) => Some(ExactRecipient {
            recipient,
            incarnation,
        }),
        (Some(name), _, _) => {
            return Err(format!(
                "renew does not take an alias: {name} is a live address another agent may hold, \
                 and a policy aimed at the wrong one clears its conversation every cycle. Pass no \
                 recipient to renew yourself, or --recipient-id with --recipient-incarnation to \
                 renew another agent deliberately"
            ));
        }
        _ => {
            return Err(
                "renew requires --recipient-id and --recipient-incarnation together, or neither \
                 to renew yourself"
                    .into(),
            );
        }
    };
    Ok(Command::Renew(Box::new(RenewArgs {
        recipient,
        requester,
        prepare_prompt,
        prompt,
        on_timeout,
        prepare_timeout_ms,
        every_ms,
        due,
        idempotency_key,
    })))
}

/// Take one body from an explicitly named text/file flag pair.
///
/// A renew carries two distinct prompts, so the shared `--body`/`--file`/
/// `--stdin` form cannot address them unambiguously.
fn take_named_body(
    tokens: &mut Tokens<'_>,
    verb: &str,
    text_flag: &str,
    file_flag: &str,
) -> Result<BodySource, String> {
    let text = tokens.take_all_values(text_flag)?;
    let file = tokens.take_all_values(file_flag)?;
    match (text.as_slice(), file.as_slice()) {
        ([value], []) => Ok(BodySource::Literal(value.clone())),
        ([], [path]) => Ok(BodySource::File(PathBuf::from(path))),
        ([], []) => Err(format!("{verb} requires {text_flag} or {file_flag}")),
        _ => Err(format!(
            "{verb} accepts exactly one of {text_flag} or {file_flag}"
        )),
    }
}

fn parse_renew_cancel(args: &[String]) -> Result<Command, String> {
    let mut tokens = Tokens::new(&args[1..]);
    let requester = take_caller(&mut tokens)?;
    let reason = tokens.take_value("--reason")?.ok_or(
        "renew-cancel requires --reason: a policy that ends without saying why is the silence \
         renew reporting exists to avoid",
    )?;
    let renew_id = tokens
        .take_positional()
        .ok_or("usage: kelpie renew-cancel <renew-id> --reason TEXT")?;
    tokens.finish("renew-cancel")?;
    Ok(Command::RenewCancel {
        renew_id,
        reason,
        requester,
    })
}

fn parse_reminder_snooze(args: &[String]) -> Result<Command, String> {
    let mut tokens = Tokens::new(&args[1..]);
    let requester = take_caller(&mut tokens)?;
    let until = tokens
        .take_value("--until-ms")?
        .ok_or("missing --until-ms")?;
    let until_ms = until
        .parse::<i64>()
        .map_err(|_| "--until-ms must be a non-negative integer".to_string())?;
    if until_ms < 0 {
        return Err("--until-ms must be a non-negative integer".into());
    }
    let ask_id = tokens
        .take_positional()
        .ok_or("usage: kelpie reminder-snooze <ask-id> --until-ms MS")?;
    tokens.finish("reminder-snooze")?;
    Ok(Command::ReminderSnooze {
        ask_id,
        until_ms,
        requester,
    })
}

fn parse_reminder_disable(args: &[String]) -> Result<Command, String> {
    let mut tokens = Tokens::new(&args[1..]);
    let requester = take_caller(&mut tokens)?;
    let ask_id = tokens
        .take_positional()
        .ok_or("usage: kelpie reminder-disable <ask-id>")?;
    tokens.finish("reminder-disable")?;
    Ok(Command::ReminderDisable { ask_id, requester })
}

fn parse_reply(args: &[String]) -> Result<Command, String> {
    let mut tokens = Tokens::new(&args[1..]);
    let progress = tokens.take_bool("--progress")?;
    let final_ = tokens.take_bool("--final")?;
    let body = take_body(&mut tokens, "reply")?;
    let idempotency_key = tokens.take_value("--idempotency-key")?;
    let requester = take_caller(&mut tokens)?;
    let reply_to = tokens
        .take_positional()
        .ok_or("usage: kelpie reply <ask-id> --progress|--final --stdin|--file|--body")?;
    tokens.finish("reply")?;
    let disposition = match (progress, final_) {
        (true, false) => "progress",
        (false, true) => "final",
        _ => return Err("reply requires exactly one of --progress or --final".into()),
    };
    Ok(Command::Reply {
        reply_to,
        requester,
        body,
        disposition,
        idempotency_key,
    })
}

fn parse_pending(args: &[String]) -> Result<Command, String> {
    let mut tokens = Tokens::new(&args[1..]);
    let target = take_single_target(&mut tokens, "pending")?;
    tokens.finish("pending")?;
    Ok(Command::Pending { target })
}

fn parse_whoami(args: &[String]) -> Result<Command, String> {
    let mut tokens = Tokens::new(&args[1..]);
    let sender_id = tokens.take_value("--sender-id")?;
    if sender_id.is_some() {
        return Err("whoami does not accept --sender-id; use an alias or --pane".into());
    }
    let target = take_single_target(&mut tokens, "whoami")?;
    tokens.finish("whoami")?;
    Ok(Command::Whoami { target })
}

fn parse_ask_info(args: &[String]) -> Result<Command, String> {
    let mut tokens = Tokens::new(&args[1..]);
    let ask_id = tokens
        .take_positional()
        .ok_or("usage: kelpie ask-info <ask-id>")?;
    tokens.finish("ask-info")?;
    Ok(Command::AskInfo { ask_id })
}

fn parse_name_info(args: &[String]) -> Result<Command, String> {
    let mut tokens = Tokens::new(&args[1..]);
    let name = tokens
        .take_positional()
        .ok_or("usage: kelpie name-info <alias>")?;
    tokens.finish("name-info")?;
    Ok(Command::NameInfo { name })
}

fn parse_attribution(args: &[String]) -> Result<Command, String> {
    let mut tokens = Tokens::new(&args[1..]);
    let incarnation_id = tokens.take_value("--incarnation-id")?;
    let agent_id = tokens.take_value("--agent-id")?;
    let pane = tokens.take_value("--pane")?;
    let refresh = tokens.take_bool("--refresh")?;
    let alias = tokens.take_positional();
    tokens.finish("attribution")?;
    let target = match (incarnation_id, agent_id, pane, alias) {
        (Some(id), None, None, None) => AttributionTarget::Incarnation(id),
        (None, Some(id), None, None) => AttributionTarget::Agent(id),
        (None, None, Some(pane), None) => AttributionTarget::Pane(pane),
        (None, None, None, Some(alias)) => AttributionTarget::Alias(alias),
        (None, None, None, None) => AttributionTarget::Pane(
            std::env::var("HERDR_PANE_ID")
                .ok()
                .filter(|pane| !pane.is_empty())
                .ok_or("cannot resolve caller; set HERDR_PANE_ID or pass a target")?,
        ),
        _ => {
            return Err(
                "attribution accepts exactly one target: a name, --pane, --agent-id, or \
                 --incarnation-id"
                    .into(),
            );
        }
    };
    Ok(Command::Attribution { target, refresh })
}

/// Read the `--keep-open` / `--no-keep-open` pair, noting a bad combination.
///
/// The three `take_*` helpers below each resolve one exclusive choice. They
/// return a usable placeholder on a bad combination rather than returning
/// early, so the rest of the argument list still gets parsed and reported.
fn take_keep_open(problems: &mut Problems, tokens: &mut Tokens<'_>, command: &str) -> bool {
    let keep_open = problems.switch(tokens, "--keep-open");
    let no_keep_open = problems.switch(tokens, "--no-keep-open");
    match (keep_open, no_keep_open) {
        (true, false) => true,
        (false, true) => false,
        _ => {
            problems.note(format!(
                "{command} requires exactly one of --keep-open or --no-keep-open"
            ));
            false
        }
    }
}

fn take_parent(
    problems: &mut Problems,
    tokens: &mut Tokens<'_>,
    command: &str,
) -> Option<StartParent> {
    let parentless = problems.switch(tokens, "--parentless");
    let parent_id = problems.value(tokens, "--parent-id");
    match (parentless, parent_id) {
        (true, None) => Some(StartParent::Parentless),
        (false, Some(id)) => Some(StartParent::Agent(id)),
        _ => {
            problems.note(format!(
                "{command} requires exactly one of --parentless or --parent-id"
            ));
            None
        }
    }
}

fn take_initial_kind(
    problems: &mut Problems,
    tokens: &mut Tokens<'_>,
    command: &str,
) -> &'static str {
    let tell = problems.switch(tokens, "--tell");
    let ask = problems.switch(tokens, "--ask");
    match (tell, ask) {
        (true, false) => "tell",
        (false, true) => "ask",
        _ => {
            problems.note(format!("{command} requires exactly one of --tell or --ask"));
            "tell"
        }
    }
}

fn parse_start(args: &[String], handoff: bool) -> Result<Command, String> {
    let command = if handoff { "handoff" } else { "start" };
    let mut tokens = Tokens::new(args);
    let mut problems = Problems::default();
    let supersedes = problems.value(&mut tokens, "--replace");
    match (handoff, supersedes.as_ref()) {
        (true, None) => problems.note(
            "handoff requires --replace INCARNATION-ID: name the incarnation \
             the new one takes over from",
        ),
        (false, Some(_)) => problems.note(
            "start does not accept --replace; use handoff to replace a running \
             incarnation of the same logical agent",
        ),
        _ => {}
    }
    let public_name = problems.required(&mut tokens, "--name");
    let pane_id = problems.required(&mut tokens, "--pane");
    let terminal_id = problems.required(&mut tokens, "--terminal");
    let backend_kind = problems.required(&mut tokens, "--backend");
    let working_directory = problems.required(&mut tokens, "--cwd");
    let readiness_timeout_ms = problems
        .required(&mut tokens, "--timeout-ms")
        .and_then(|timeout| {
            let milliseconds = timeout.parse::<u64>().ok();
            if milliseconds.is_none() {
                problems.note("invalid --timeout-ms");
            }
            milliseconds
        });
    let keep_open = take_keep_open(&mut problems, &mut tokens, command);
    let parent = take_parent(&mut problems, &mut tokens, command);
    let initial_kind = take_initial_kind(&mut problems, &mut tokens, command);
    // An initial ask needs an agent waiting identity, but the caller does not
    // have to spell it out: an absent --sender-id resolves to the caller's Ready
    // binding, matching tell, ask, pending, and cancel. The store still rejects
    // an operator-attributed ask, which has nobody to owe the reply to.
    let initial_sender = problems.value(&mut tokens, "--sender-id");
    let body = match take_body(&mut tokens, command) {
        Ok(body) => Some(body),
        Err(problem) => {
            problems.note(problem);
            None
        }
    };
    let backend_args = match tokens.take_all_values("--arg") {
        Ok(values) => values,
        Err(problem) => {
            problems.note(problem);
            Vec::new()
        }
    };
    let logical_agent_id = problems.value(&mut tokens, "--logical-id");
    let herdr_session = problems
        .value(&mut tokens, "--session")
        .unwrap_or_else(|| "default".into());
    let requested_model = problems.value(&mut tokens, "--requested-model");
    let requested_provider = problems.value(&mut tokens, "--requested-provider");
    let requested_effort = problems.value(&mut tokens, "--requested-effort");
    let idempotency_key = problems.value(&mut tokens, "--idempotency-key");
    problems.resolve(&tokens, command)?;
    // Past resolve every branch above that produced None also noted a problem,
    // so these unwraps restate a check that has already passed.
    let public_name = public_name.ok_or("missing --name")?;
    let pane_id = pane_id.ok_or("missing --pane")?;
    let terminal_id = terminal_id.ok_or("missing --terminal")?;
    let backend_kind = backend_kind.ok_or("missing --backend")?;
    let working_directory = working_directory.ok_or("missing --cwd")?;
    let readiness_timeout_ms = readiness_timeout_ms.ok_or("missing --timeout-ms")?;
    let parent = parent.ok_or("missing --parentless or --parent-id")?;
    let body = body.ok_or("missing --stdin, --file, or --body")?;
    Ok(Command::Start(Box::new(StartCommand {
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
    })))
}

fn parse_adopt(args: &[String]) -> Result<Command, String> {
    let mut tokens = Tokens::new(args);
    let mut problems = Problems::default();
    let pane_id = problems.required(&mut tokens, "--pane");
    let terminal_id = problems.required(&mut tokens, "--terminal");
    let public_name = problems.value(&mut tokens, "--name");
    let backend_kind = problems.value(&mut tokens, "--backend");
    let logical_agent_id = problems.value(&mut tokens, "--logical-id");
    let herdr_session = problems
        .value(&mut tokens, "--session")
        .unwrap_or_else(|| "default".into());
    let backend_args = match tokens.take_all_values("--arg") {
        Ok(values) => values,
        Err(problem) => {
            problems.note(problem);
            Vec::new()
        }
    };
    let requested_model = problems.value(&mut tokens, "--requested-model");
    let requested_provider = problems.value(&mut tokens, "--requested-provider");
    let requested_effort = problems.value(&mut tokens, "--requested-effort");
    let idempotency_key = problems.value(&mut tokens, "--idempotency-key");
    problems.resolve(&tokens, "adopt")?;
    // Past resolve both of these are present; see the note in parse_start.
    let pane_id = pane_id.ok_or("missing --pane")?;
    let terminal_id = terminal_id.ok_or("missing --terminal")?;
    Ok(Command::Adopt(Box::new(AdoptArgs {
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
    })))
}

fn parse_cancel(args: &[String]) -> Result<Command, String> {
    let mut tokens = Tokens::new(&args[1..]);
    let reason = tokens.take_value("--reason")?.ok_or("missing --reason")?;
    let requester = take_caller(&mut tokens)?;
    let ask_id = tokens
        .take_positional()
        .ok_or("usage: kelpie cancel <ask-id> --reason TEXT")?;
    tokens.finish("cancel")?;
    Ok(Command::Cancel {
        ask_id,
        reason,
        requester,
    })
}

fn parse_retire(args: &[String]) -> Result<Command, String> {
    let mut tokens = Tokens::new(args);
    let incarnation_id = tokens
        .take_value("--incarnation")?
        .ok_or("missing --incarnation")?;
    let idempotency_key = tokens.take_value("--idempotency-key")?;
    let close_pane = tokens.take_bool("--close-pane")?;
    tokens.finish("retire")?;
    Ok(Command::Retire {
        incarnation_id,
        idempotency_key,
        close_pane,
    })
}

/// One requested delivery time, before the client consults a clock.
///
/// `--due-at-ms` is the primitive the daemon speaks. The other two are sugar
/// over it, and they exist because a wrong epoch fails in the worst possible
/// way: silently, as a delivery at the wrong moment. A malformed duration or
/// timestamp fails loudly at parse instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Due {
    /// Exact Unix epoch milliseconds.
    AtMs(i64),
    /// Milliseconds after the moment the client resolves this.
    InMs(i64),
}

/// Accept exactly one of `--due-at-ms`, `--due-in`, or `--due-at`.
fn take_due(tokens: &mut Tokens<'_>, verb: &str) -> Result<Option<Due>, String> {
    let at_ms = tokens.take_all_values("--due-at-ms")?;
    let in_values = tokens.take_all_values("--due-in")?;
    let at_values = tokens.take_all_values("--due-at")?;
    // A due time postpones the message. On an ask that leaves an obligation the
    // recipient cannot see: owed on the server, invisible in their pane, and
    // indistinguishable from a delivered ask nobody answered. Use
    // `--remind-after-ms` to be nudged about an ask that is already working.
    if verb == "ask" && at_ms.len() + in_values.len() + at_values.len() > 0 {
        return Err(
            "ask does not accept --due-in, --due-at, or --due-at-ms: an ask is delivered now. \
             Use --remind-after-ms to be reminded about an unanswered ask, or tell for a \
             message that should arrive later"
                .into(),
        );
    }
    if at_ms.len() + in_values.len() + at_values.len() > 1 {
        return Err(format!(
            "{verb} allows only one of --due-in, --due-at, or --due-at-ms"
        ));
    }
    if let [value] = at_ms.as_slice() {
        let parsed = value
            .parse::<i64>()
            .map_err(|_| format!("{verb} --due-at-ms must be a non-negative integer"))?;
        if parsed < 0 {
            return Err(format!("{verb} --due-at-ms must be a non-negative integer"));
        }
        return Ok(Some(Due::AtMs(parsed)));
    }
    if let [value] = in_values.as_slice() {
        return parse_duration_ms(value).map(|ms| Some(Due::InMs(ms)));
    }
    if let [value] = at_values.as_slice() {
        return parse_utc_rfc3339_ms(value).map(|ms| Some(Due::AtMs(ms)));
    }
    Ok(None)
}

/// Parse `10m`, `2h`, `30s`, or `1d` into milliseconds.
fn parse_duration_ms(value: &str) -> Result<i64, String> {
    parse_duration_for("--due-in", value)
}

fn parse_duration_for(flag: &str, value: &str) -> Result<i64, String> {
    let shape = || format!("{flag} must look like 10m, 2h, 30s, or 1d");
    let split = value
        .find(|character: char| !character.is_ascii_digit())
        .ok_or_else(shape)?;
    let (digits, unit) = value.split_at(split);
    if digits.is_empty() {
        return Err(shape());
    }
    let multiplier = match unit {
        "s" => 1_000,
        "m" => 60 * 1_000,
        "h" => 60 * 60 * 1_000,
        "d" => 24 * 60 * 60 * 1_000,
        _ => return Err(format!("{flag} unit must be s, m, h, or d")),
    };
    let amount = digits
        .parse::<i64>()
        .map_err(|_| format!("{flag} overflowed"))?;
    if amount <= 0 {
        return Err(format!("{flag} must be a positive duration"));
    }
    amount
        .checked_mul(multiplier)
        .ok_or_else(|| format!("{flag} overflowed"))
}

/// Parse a UTC RFC3339 timestamp into Unix epoch milliseconds.
///
/// UTC only. An offset would have to be applied to a wall clock that a caller
/// may not have meant, and getting that silently wrong is the failure this flag
/// exists to prevent.
fn parse_utc_rfc3339_ms(value: &str) -> Result<i64, String> {
    let body = value
        .strip_suffix('Z')
        .or_else(|| value.strip_suffix("+00:00"))
        .ok_or("--due-at must be UTC RFC3339 ending in Z or +00:00")?;
    let (date, time) = body
        .split_once('T')
        .ok_or("--due-at must be UTC RFC3339 like 2026-08-12T20:00:00Z")?;
    let mut date_parts = date.split('-');
    let year: i64 = next_number(&mut date_parts, "--due-at date is invalid")?;
    let month: i64 = next_number(&mut date_parts, "--due-at date is invalid")?;
    let day: i64 = next_number(&mut date_parts, "--due-at date is invalid")?;
    if date_parts.next().is_some() || !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return Err("--due-at date is invalid".into());
    }
    // Seconds may carry a fractional part; whole milliseconds are enough here.
    let time = time.split_once('.').map_or(time, |(whole, _)| whole);
    let mut time_parts = time.split(':');
    let hour: i64 = next_number(&mut time_parts, "--due-at time is invalid")?;
    let minute: i64 = next_number(&mut time_parts, "--due-at time is invalid")?;
    let second: i64 = next_number(&mut time_parts, "--due-at time is invalid")?;
    if time_parts.next().is_some()
        || !(0..=23).contains(&hour)
        || !(0..=59).contains(&minute)
        || !(0..=60).contains(&second)
    {
        return Err("--due-at time is invalid".into());
    }
    let days = days_from_civil(year, month, day);
    days.checked_mul(86_400)
        .and_then(|seconds| seconds.checked_add(hour * 3_600 + minute * 60 + second))
        .and_then(|seconds| seconds.checked_mul(1_000))
        .ok_or_else(|| "--due-at overflowed".to_string())
}

fn next_number<'a>(parts: &mut impl Iterator<Item = &'a str>, error: &str) -> Result<i64, String> {
    let part = parts.next().ok_or_else(|| error.to_string())?;
    if part.is_empty() || !part.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(error.to_string());
    }
    part.parse::<i64>().map_err(|_| error.to_string())
}

/// Days since 1970-01-01 for a proleptic Gregorian date.
///
/// Hinnant's `days_from_civil`, which handles leap years and centuries without
/// a calendar dependency.
fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let year_of_era = year - era * 400;
    let day_of_year = (153 * (month + if month > 2 { -3 } else { 9 }) + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

/// Render epoch milliseconds as a UTC timestamp, the inverse of `--due-at`.
///
/// Receipts show the moment, not the epoch, because a scheduled delivery is the
/// one outcome a reader is most likely to mistake for a completed one.
fn format_utc_ms(ms: i64) -> String {
    let (days, millis_of_day) = (ms.div_euclid(86_400_000), ms.rem_euclid(86_400_000));
    let (hours, minutes, seconds) = (
        millis_of_day / 3_600_000,
        (millis_of_day / 60_000) % 60,
        (millis_of_day / 1_000) % 60,
    );
    // Inverse of days_from_civil (Howard Hinnant's civil_from_days).
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let day_of_era = z - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = month_prime + if month_prime < 10 { 3 } else { -9 };
    let year = year_of_era + era * 400 + i64::from(month <= 2);
    format!("{year:04}-{month:02}-{day:02}T{hours:02}:{minutes:02}:{seconds:02}Z")
}

fn take_caller(tokens: &mut Tokens<'_>) -> Result<Option<Caller>, String> {
    let sender = tokens.take_value("--sender")?;
    let sender_id = tokens.take_value("--sender-id")?;
    let pane = tokens.take_value("--pane")?;
    match (sender, sender_id, pane) {
        (None, None, None) => Ok(None),
        (Some(alias), None, None) => Ok(Some(Caller::Alias(alias))),
        (None, Some(id), None) => Ok(Some(Caller::Id(id))),
        (None, None, Some(pane)) => Ok(Some(Caller::Pane(pane))),
        _ => Err("use only one of --sender, --sender-id, or --pane".into()),
    }
}

fn take_single_target(tokens: &mut Tokens<'_>, command: &str) -> Result<Option<Caller>, String> {
    let caller = take_caller(tokens)?;
    let positional = tokens.take_positional();
    match (positional, caller) {
        (None, None) => Ok(None),
        (Some(alias), None) => Ok(Some(Caller::Alias(alias))),
        (None, Some(caller)) => Ok(Some(caller)),
        (Some(_), Some(_)) => Err(format!(
            "{command} accepts only one target form: a name or one caller flag"
        )),
    }
}

fn take_body(tokens: &mut Tokens<'_>, command: &str) -> Result<BodySource, String> {
    let stdin = tokens.take_bool("--stdin")?;
    let file = tokens.take_value("--file")?;
    let body = tokens.take_value("--body")?;
    match (stdin, file, body) {
        (true, None, None) => Ok(BodySource::Stdin),
        (false, Some(path), None) => Ok(BodySource::File(PathBuf::from(path))),
        (false, None, Some(text)) => Ok(BodySource::Literal(text)),
        (false, None, None) => Err(format!(
            "{command} requires --stdin, --file PATH, or --body TEXT"
        )),
        _ => Err("use exactly one of --stdin, --file, or --body".into()),
    }
}

/// Flags `parse_invocation` reads before the command name, and only there.
const GLOBAL_FLAGS: [&str; 2] = ["--socket", "--json"];

struct Tokens<'a> {
    items: &'a [String],
    used: Vec<bool>,
}

impl Tokens<'_> {
    fn new(items: &[String]) -> Tokens<'_> {
        Tokens {
            items,
            used: vec![false; items.len()],
        }
    }

    fn take_bool(&mut self, flag: &str) -> Result<bool, String> {
        let hits: Vec<usize> = self
            .items
            .iter()
            .enumerate()
            .filter(|(index, item)| !self.used[*index] && *item == flag)
            .map(|(index, _)| index)
            .collect();
        match hits.as_slice() {
            [] => Ok(false),
            [index] => {
                self.used[*index] = true;
                Ok(true)
            }
            _ => Err(format!("{flag} specified more than once")),
        }
    }

    fn take_next_raw_value(&mut self, flag: &str) -> Result<Option<String>, String> {
        self.take_next_value_inner(flag, true)
    }

    fn take_next_value_inner(
        &mut self,
        flag: &str,
        allow_dash_value: bool,
    ) -> Result<Option<String>, String> {
        let index = self
            .items
            .iter()
            .enumerate()
            .find_map(|(index, item)| (!self.used[index] && item == flag).then_some(index));
        let Some(index) = index else {
            return Ok(None);
        };
        let value_index = index + 1;
        if value_index >= self.items.len() || self.used[value_index] {
            return Err(format!("{flag} needs a value"));
        }
        if !allow_dash_value && self.items[value_index].starts_with('-') {
            return Err(format!("{flag} needs a value"));
        }
        self.used[index] = true;
        self.used[value_index] = true;
        Ok(Some(self.items[value_index].clone()))
    }

    fn take_all_values(&mut self, flag: &str) -> Result<Vec<String>, String> {
        let mut values = Vec::new();
        while let Some(value) = self.take_next_raw_value(flag)? {
            values.push(value);
        }
        Ok(values)
    }

    fn take_value(&mut self, flag: &str) -> Result<Option<String>, String> {
        let hits: Vec<usize> = self
            .items
            .iter()
            .enumerate()
            .filter(|(index, item)| !self.used[*index] && *item == flag)
            .map(|(index, _)| index)
            .collect();
        match hits.as_slice() {
            [] => Ok(None),
            [index] => {
                let value_index = index + 1;
                if value_index >= self.items.len()
                    || self.used[value_index]
                    || self.items[value_index].starts_with('-')
                {
                    return Err(format!("{flag} needs a value"));
                }
                self.used[*index] = true;
                self.used[value_index] = true;
                Ok(Some(self.items[value_index].clone()))
            }
            _ => Err(format!("{flag} specified more than once")),
        }
    }

    fn take_positional(&mut self) -> Option<String> {
        let index = self.items.iter().enumerate().find_map(|(index, item)| {
            (!self.used[index] && !item.starts_with('-')).then_some(index)
        })?;
        self.used[index] = true;
        Some(self.items[index].clone())
    }

    /// Describe every argument this parse never consumed, one entry each.
    ///
    /// An unrecognised flag also swallows the unused value token behind it, so
    /// a misspelled flag reads as one problem rather than as a bad flag plus a
    /// stray positional.
    fn leftovers(&self) -> Vec<String> {
        let mut problems = Vec::new();
        let mut index = 0;
        while index < self.items.len() {
            if self.used[index] {
                index += 1;
                continue;
            }
            let item = &self.items[index];
            if !item.starts_with('-') {
                problems.push(format!("unexpected argument {item}"));
                index += 1;
                continue;
            }
            // Global flags are read only until the command name, so one placed
            // after it lands here looking like a typo. Say where it goes
            // instead: rewriting the command line to hunt for it is how
            // arguments get dropped.
            if GLOBAL_FLAGS.contains(&item.as_str()) {
                problems.push(format!(
                    "{item} is a global flag and goes before the command, as \
                     `kelpie {item} <command> ...`"
                ));
            } else {
                problems.push(format!("unknown argument {item}"));
            }
            let value = index + 1;
            index = if value < self.items.len()
                && !self.used[value]
                && !self.items[value].starts_with('-')
            {
                value + 1
            } else {
                index + 1
            };
        }
        problems
    }

    fn finish(self, command: &str) -> Result<(), String> {
        let problems = self.leftovers();
        if problems.is_empty() {
            return Ok(());
        }
        Err(format!("{command} {}", problems.join("; ")))
    }
}

/// Every usage problem found while parsing one invocation.
///
/// A parse that returns on the first problem makes a caller discover a
/// multi-argument requirement one rejected invocation at a time, and every
/// rewrite in between is a chance to drop an argument the parser does not
/// itself require. That is not hypothetical: on 2026-08-26 a caller spent six
/// invocations finding `start`'s six required flags and lost
/// `--dangerously-skip-permissions` during one of the rewrites, which started
/// an agent in a permission mode nobody wanted and no later check caught.
/// So the argument-heavy parses collect their problems and report them once.
#[derive(Debug, Default)]
struct Problems {
    found: Vec<String>,
}

impl Problems {
    fn note(&mut self, problem: impl Into<String>) {
        self.found.push(problem.into());
    }

    /// Take an optional value, recording a malformed one instead of returning.
    fn value(&mut self, tokens: &mut Tokens<'_>, flag: &str) -> Option<String> {
        match tokens.take_value(flag) {
            Ok(value) => value,
            Err(problem) => {
                self.note(problem);
                None
            }
        }
    }

    /// Take a required value, recording its absence as one more problem.
    fn required(&mut self, tokens: &mut Tokens<'_>, flag: &str) -> Option<String> {
        match tokens.take_value(flag) {
            Ok(Some(value)) => Some(value),
            Ok(None) => {
                self.note(format!("missing {flag}"));
                None
            }
            // A malformed flag already explains itself; do not also call it missing.
            Err(problem) => {
                self.note(problem);
                None
            }
        }
    }

    fn switch(&mut self, tokens: &mut Tokens<'_>, flag: &str) -> bool {
        match tokens.take_bool(flag) {
            Ok(present) => present,
            Err(problem) => {
                self.note(problem);
                false
            }
        }
    }

    /// Fold in whatever the parse never consumed, then report everything at once.
    fn resolve(mut self, tokens: &Tokens<'_>, command: &str) -> Result<(), String> {
        for leftover in tokens.leftovers() {
            self.note(leftover);
        }
        match self.found.len() {
            0 => Ok(()),
            1 => Err(format!("{command}: {}", self.found[0])),
            count => {
                let mut message = format!("{command}: {count} problems");
                for problem in &self.found {
                    message.push_str("\n  ");
                    message.push_str(problem);
                }
                Err(message)
            }
        }
    }
}

fn required_value(args: &[String], index: &mut usize, flag: &str) -> Result<String, String> {
    let value = args
        .get(*index + 1)
        .cloned()
        .ok_or_else(|| format!("{flag} needs a value"))?;
    if value.starts_with('-') {
        return Err(format!("{flag} needs a value"));
    }
    *index += 2;
    Ok(value)
}

fn is_raw_socket_token(token: &str) -> bool {
    token.contains('/')
        || Path::new(token)
            .extension()
            .is_some_and(|ext| ext.eq_ignore_ascii_case("sock"))
        || Path::new(token).exists()
}

fn nested_field(value: &Value, object: &str, name: &str) -> String {
    match value.get(object) {
        Some(inner) => field(inner, name),
        None => "-".into(),
    }
}

fn format_waiter_retire_receipt(result: &Value) -> String {
    let cancelled = result["cancelled_ask_ids"]
        .as_array()
        .map(|ids| {
            ids.iter()
                .filter_map(Value::as_str)
                .collect::<Vec<_>>()
                .join(",")
        })
        .filter(|text| !text.is_empty())
        .unwrap_or_else(|| "none".into());
    let mut delivered = 0;
    let mut recorded = 0;
    if let Some(notices) = result["owing_notices"].as_array() {
        for notice in notices {
            match notice["owing_response"].as_str() {
                Some("delivered") => delivered += 1,
                _ => recorded += 1,
            }
        }
    }
    format!(
        "waiter-retire agent={} targeting-ended={} cancelled-asks={} owing-delivered={} owing-recorded={}\n",
        field(result, "logical_agent_id"),
        field(result, "targeting_ended"),
        cancelled,
        delivered,
        recorded
    )
}

fn field(value: &Value, name: &str) -> String {
    match &value[name] {
        Value::String(text) => text.clone(),
        Value::Number(number) => number.to_string(),
        Value::Bool(flag) => flag.to_string(),
        Value::Null => "-".into(),
        other => other.to_string(),
    }
}

/// Build tell/ask params after the caller and recipient are resolved.
#[must_use]
pub fn message_params(
    sender: &str,
    recipient_alias: Option<&str>,
    exact: Option<&ExactRecipient>,
    body: &str,
    idempotency_key: &str,
    due_at_ms: Option<i64>,
) -> Value {
    let mut params = json!({
        "sender": sender,
        "body": body,
        "idempotency_key": idempotency_key,
    });
    if let Some(exact) = exact {
        params["recipient"] = json!(exact.recipient);
        params["recipient_incarnation"] = json!(exact.incarnation);
    } else if let Some(alias) = recipient_alias {
        params["recipient_alias"] = json!(alias);
    }
    if let Some(due_at_ms) = due_at_ms {
        params["due_at_ms"] = json!(due_at_ms);
    }
    params
}

/// Build a whoami params object.
#[must_use]
pub fn whoami_params(caller: &Caller) -> Value {
    match caller {
        Caller::Alias(alias) => json!({"alias": alias}),
        Caller::Pane(pane_id) => json!({"pane_id": pane_id}),
        Caller::Id(_) => json!({}),
    }
}

/// Build an attribution params object naming exactly one identity.
#[must_use]
pub fn attribution_params(target: &AttributionTarget) -> Value {
    match target {
        AttributionTarget::Incarnation(id) => json!({"incarnation_id": id}),
        AttributionTarget::Agent(id) => json!({"agent_id": id}),
        AttributionTarget::Alias(alias) => json!({"alias": alias}),
        AttributionTarget::Pane(pane_id) => json!({"pane_id": pane_id}),
    }
}

/// Render one observed field as `undetermined` or its reported value.
fn observed_field(observation: &Value, name: &str) -> String {
    match observation.get(name).and_then(|field| field.get("status")) {
        Some(Value::String(status)) if status == "reported" => observation[name]
            .get("value")
            .and_then(Value::as_str)
            .unwrap_or("-")
            .to_string(),
        Some(Value::String(status)) => status.clone(),
        _ => "-".into(),
    }
}

/// Render name-info: every claimant of a name and every unresolved ask touching
/// them, with both parties named and marked live or not. Facts only — a name's
/// history belongs to whoever reads it, not to a verdict.
fn render_name_info(result: &Value) -> String {
    let empty = Vec::new();
    let claimants = result["claimants"].as_array().unwrap_or(&empty);
    let unresolved = result["unresolved"].as_array().unwrap_or(&empty);
    let name = result["name"].as_str().unwrap_or("?");
    let mut text = format!("name-info {name} claimants={}", claimants.len());
    for claimant in claimants {
        let live = claimant["live"].as_bool().unwrap_or(false);
        let addressable = claimant["addressable"].as_bool().unwrap_or(live);
        let transport = claimant["delivery_transport"]
            .as_str()
            .unwrap_or("herdr_prompt");
        let state = match (transport, live, addressable) {
            ("socket_inbox", _, true) => "active-waiter",
            ("socket_inbox", _, false) => "retired-waiter",
            (_, true, _) => "live",
            _ => "not-live",
        };
        let _ = write!(
            text,
            "\n  {} created={} {} transport={} unresolved={}",
            claimant["logical_agent_id"].as_str().unwrap_or("?"),
            claimant["created_at_ms"]
                .as_i64()
                .map_or("?".into(), format_utc_ms),
            state,
            transport,
            claimant["unresolved_count"].as_i64().unwrap_or(0),
        );
    }
    let _ = write!(text, "\nunresolved obligations={}", unresolved.len());
    for obligation in unresolved {
        let party = |side: &str| {
            let side_value = &obligation[side];
            let live = side_value["live"].as_bool().unwrap_or(false);
            format!(
                "{} ({}, {})",
                side_value["name"].as_str().unwrap_or("?"),
                side_value["agent_id"].as_str().unwrap_or("?"),
                if live { "live" } else { "not-live" },
            )
        };
        let _ = write!(
            text,
            "\n  ask {} state={} created={} last-activity={}\n    asker {}\n    responder {}",
            obligation["ask_message_id"].as_str().unwrap_or("?"),
            obligation["state"].as_str().unwrap_or("?"),
            obligation["created_at_ms"]
                .as_i64()
                .map_or("?".into(), format_utc_ms),
            obligation["last_activity_at_ms"]
                .as_i64()
                .map_or("?".into(), format_utc_ms),
            party("asker"),
            party("responder"),
        );
    }
    text.push('\n');
    text
}

/// Render ask-info: the durable content of one ask and its parties. This is
/// the amnesia-recovery read — a renewed agent re-reads what it was asked.
fn render_ask_info(result: &Value) -> String {
    let mut text = format!(
        "ask-info {} state={}",
        field(result, "ask_message_id"),
        field(result, "state")
    );
    let asker = &result["asker"];
    let responder = &result["responder"];
    let _ = write!(
        text,
        "\n  asked-by {} ({})\n  responder {} ({})\n  created={} last-activity={}",
        asker["name"].as_str().unwrap_or("?"),
        asker["agent_id"].as_str().unwrap_or("?"),
        responder["name"].as_str().unwrap_or("?"),
        responder["agent_id"].as_str().unwrap_or("?"),
        result["created_at_ms"]
            .as_i64()
            .map_or("?".into(), format_utc_ms),
        result["last_activity_at_ms"]
            .as_i64()
            .map_or("?".into(), format_utc_ms),
    );
    if let Some(reason) = result["cancellation_reason"].as_str() {
        let _ = write!(text, "\n  cancellation-reason {reason}");
    }
    let _ = write!(text, "\n\n{}\n", field(result, "body"));
    text
}

/// Render the fleet as a parentage tree with obligations as edges.
///
/// Facts only: states are printed as recorded, never labelled healthy or stuck.
/// Callers wanting to judge should read `--json`.
fn render_report(result: &Value) -> String {
    let empty = Vec::new();
    let agents = result["agents"].as_array().unwrap_or(&empty);
    let obligations = result["obligations"].as_array().unwrap_or(&empty);
    let now = result["generated_at_ms"].as_i64().unwrap_or_default();

    // The reader is usually an agent with no prior context, so the report says
    // what it is and who owns each fact before showing any of it.
    // "ready" is the only state that proves an agent is addressable. Counting
    // starting and unknown alongside it would claim a binding Kelpie has not
    // proven, so they are reported separately as what they are: undecided.
    let ready = agents
        .iter()
        .filter(|agent| newest_state(agent) == "ready")
        .count();
    let unsettled = agents
        .iter()
        .filter(|agent| matches!(newest_state(agent).as_str(), "starting" | "unknown"))
        .count();
    let open = obligations
        .iter()
        .filter(|obligation| matches!(obligation["state"].as_str(), Some("open" | "in_progress")))
        .count();
    let mut text = String::from(
        "kelpie report: every logical agent Kelpie has ever recorded, nested under \
         the agent that started it.\n\
         indent means \"started by the line above\". kelpie=<what Kelpie recorded> \
         herdr=<Herdr's live status, only with --live>.\n\
         a logical agent outlives its runtimes: incarnations= counts how many \
         times it has been bound to one.\n\
         conversation= is how long the CURRENT context has been running, not the \
         agent's age. it resets on clear, compaction, resume, or renew, and \
         reads unknown until Kelpie observes one.\n\
         ready=addressable now. unsettled=starting or unknown, may or may not be \
         alive. every other state is history.\n",
    );
    let _ = write!(
        text,
        "fleet agents={} ready={ready} unsettled={unsettled} obligations={} open={open} \
         generated={}",
        agents.len(),
        obligations.len(),
        format_utc_ms(now)
    );
    if let Some(live_at) = result.get("live_snapshot_at_ms").and_then(Value::as_i64) {
        let _ = write!(text, " herdr-snapshot={}", format_utc_ms(live_at));
    }
    text.push('\n');

    // Roots are agents with no parent recorded, plus any whose parent is absent
    // from this report, so nothing is silently dropped from the tree.
    let known: Vec<&str> = agents
        .iter()
        .filter_map(|agent| agent["agent_id"].as_str())
        .collect();
    let mut roots: Vec<&Value> = agents
        .iter()
        .filter(|agent| {
            agent["parent_agent_id"]
                .as_str()
                .is_none_or(|parent| !known.contains(&parent))
        })
        .collect();
    roots.sort_by_key(|agent| agent["created_at_ms"].as_i64().unwrap_or_default());

    let mut seen: Vec<&str> = Vec::new();
    for root in roots {
        render_agent(&mut text, root, agents, obligations, now, 0, &mut seen);
    }

    // A footnote, not a headline: names are reusable aliases, so one name held by
    // several agents over time is ordinary history rather than a fault.
    if let Some(collisions) = result["alias_collisions"]
        .as_object()
        .filter(|map| !map.is_empty())
    {
        let mut names: Vec<&String> = collisions.keys().collect();
        names.sort();
        let _ = writeln!(
            text,
            "\nnames held by more than one agent over time (aliases are reusable, \
             so this is history, not a fault):"
        );
        for name in names {
            let count = collisions[name].as_array().map_or(0, Vec::len);
            let _ = writeln!(text, "  {name} agents={count}");
        }
    }
    text
}

/// The state of an agent's newest incarnation, or `-` when it has none.
fn newest_state(agent: &Value) -> String {
    agent["incarnations"]
        .as_array()
        .and_then(|list| list.first())
        .map_or("-".into(), |value| field(value, "state"))
}

/// Render a duration the way a reader reasons about it, not in epoch units.
fn render_agent<'a>(
    text: &mut String,
    agent: &'a Value,
    agents: &'a [Value],
    obligations: &'a [Value],
    now: i64,
    depth: usize,
    seen: &mut Vec<&'a str>,
) {
    let Some(id) = agent["agent_id"].as_str() else {
        return;
    };
    // Parentage is data, and data can cycle; stop rather than recurse forever.
    if seen.contains(&id) {
        return;
    }
    seen.push(id);

    let indent = "  ".repeat(depth);
    let branch = if depth == 0 { "" } else { "└─ " };
    let newest = agent["incarnations"]
        .as_array()
        .and_then(|list| list.first());
    let state = newest.map_or("-".into(), |value| field(value, "state"));
    let backend = newest.map_or("-".into(), |value| field(value, "backend_kind"));
    let live = newest
        .and_then(|value| value.get("live"))
        .and_then(Value::as_str)
        .map_or(String::new(), |status| format!(" herdr={status}"));
    let incarnations = agent["incarnations"].as_array().map_or(0, Vec::len);
    // How long the current context has been running. Only meaningful for a
    // binding that is still live, and only once a rotation has been observed;
    // an absent stamp is reported as unknown rather than filled in from the
    // incarnation's own age, which is a different and longer number.
    let conversation = match state.as_str() {
        "ready" => newest
            .and_then(|value| value["native_session_rotated_at_ms"].as_i64())
            .map_or_else(
                || " conversation=unknown".to_string(),
                |started| format!(" conversation={}", format_duration_ms(now - started)),
            ),
        _ => String::new(),
    };
    // An unsettled incarnation is the one a caller has to act on, so name the
    // incarnation and the operation that produced it. Settled rows stay terse.
    let unsettled = match state.as_str() {
        "starting" | "unknown" => newest.map_or(String::new(), |value| {
            let operation = value["latest_operation"]["operation_id"]
                .as_str()
                .map_or(String::new(), |operation| format!(" operation={operation}"));
            format!(" incarnation={}{operation}", field(value, "incarnation_id"))
        }),
        _ => String::new(),
    };
    // Whether a context-bounding policy is armed, and when it next fires.
    // Absent on a Ready root means unsupervised, and nothing else in the report
    // distinguishes that from a supervised one — a policy ends with its
    // incarnation and adoption does not bring it back.
    let renew = match state.as_str() {
        "ready" => newest
            .and_then(|value| value.get("renew"))
            .map_or_else(String::new, |renew| {
                if renew.is_null() {
                    return String::new();
                }
                let phase = renew["phase"].as_str().unwrap_or("?");
                let every = renew["every_ms"].as_i64().map_or_else(
                    || " one-shot".to_string(),
                    |ms| format!(" every={}", format_duration_ms(ms)),
                );
                // Only a scheduled cycle has a future fire time. An in-flight
                // one carries its own original due time, already past, and
                // rendering that as `next-in=0s` reads as "fires now" when the
                // truth is "the next cycle comes after this one finishes".
                let due = if phase == "scheduled" {
                    renew["cycle_due_at_ms"]
                        .as_i64()
                        .map_or_else(String::new, |due| {
                            format!(" next-in={}", format_duration_ms((due - now).max(0)))
                        })
                } else {
                    String::new()
                };
                format!(
                    " renew={phase} cycle={}{every}{due}",
                    renew["cycle"].as_i64().unwrap_or(0)
                )
            }),
        _ => String::new(),
    };
    let _ = writeln!(
        text,
        "{indent}{branch}{} agent={id} backend={backend} kelpie={state}{live} \
         incarnations={incarnations}{conversation}{renew}{unsettled}",
        field(agent, "public_name")
    );

    for obligation in obligations
        .iter()
        .filter(|obligation| obligation["owing_agent_id"].as_str() == Some(id))
        .filter(|obligation| matches!(obligation["state"].as_str(), Some("open" | "in_progress")))
    {
        let waiting = obligation["waiting_agent_id"].as_str().unwrap_or("-");
        let waiting_name = agents
            .iter()
            .find(|candidate| candidate["agent_id"].as_str() == Some(waiting))
            .map_or(waiting.to_string(), |candidate| {
                field(candidate, "public_name")
            });
        let open_ms = now - obligation["created_at_ms"].as_i64().unwrap_or(now);
        let _ = writeln!(
            text,
            "{indent}   owes a reply to {waiting_name} ask={} state={} unanswered-for={}",
            field(obligation, "ask_message_id"),
            field(obligation, "state"),
            format_duration_ms(open_ms)
        );
    }

    let mut children: Vec<&Value> = agents
        .iter()
        .filter(|candidate| candidate["parent_agent_id"].as_str() == Some(id))
        .collect();
    children.sort_by_key(|child| child["created_at_ms"].as_i64().unwrap_or_default());
    for child in children {
        render_agent(text, child, agents, obligations, now, depth + 1, seen);
    }
}

/// Render attribution with requested and observed on separate lines.
///
/// They are never merged: requested is launch intent, observed is evidence.
fn render_attribution(result: &Value) -> String {
    let requested = &result["requested"];
    let backend_args = requested
        .get("backend_args")
        .map_or_else(|| "[]".to_string(), ToString::to_string);
    let observed = match result.get("observed") {
        Some(observed) if !observed.is_null() => format!(
            "observed adapter={} model={} provider={} effort={} recorded-at-ms={}",
            field(observed, "adapter"),
            observed_field(observed, "model"),
            observed_field(observed, "provider"),
            observed_field(observed, "effort"),
            field(observed, "recorded_at_ms")
        ),
        _ => "observed none".into(),
    };
    format!(
        "attribution name={} agent={} incarnation={} backend={} state={}\n\
         requested model={} provider={} effort={}\n\
         requested-args {backend_args}\n\
         {observed}\n",
        field(result, "public_name"),
        field(result, "logical_agent_id"),
        field(result, "incarnation_id"),
        field(result, "backend_kind"),
        field(result, "incarnation_state"),
        field(requested, "model"),
        field(requested, "provider"),
        field(requested, "effort"),
    )
}

/// Default caller from the Herdr pane environment.
#[must_use]
pub fn env_caller() -> Option<Caller> {
    std::env::var("HERDR_PANE_ID")
        .ok()
        .filter(|pane| !pane.is_empty())
        .map(Caller::Pane)
}

/// Read stdin for the legacy raw client. Kept for the existing socket tests.
///
/// # Errors
///
/// Returns an error unless stdin contains exactly one JSON request line.
pub fn read_raw_request(stdin: &mut impl Read) -> Result<String, Box<dyn std::error::Error>> {
    let mut request = String::new();
    stdin.read_to_string(&mut request)?;
    if request.lines().count() != 1 || request.trim().is_empty() {
        return Err("stdin must contain exactly one JSON request".into());
    }
    Ok(request)
}

#[cfg(test)]
mod tests {
    use std::io;

    use super::*;

    fn args(items: &[&str]) -> Vec<String> {
        items.iter().map(|item| (*item).to_string()).collect()
    }

    #[test]
    fn typed_tell_defaults_to_runtime_socket() {
        let invocation = parse_invocation(&args(&["tell", "quorum", "--stdin"])).expect("parse");
        match invocation {
            Invocation::Typed {
                json: false,
                command: Command::Tell {
                    recipient, body, ..
                },
                ..
            } => {
                assert_eq!(recipient, Recipient::Alias("quorum".into()));
                assert_eq!(body, BodySource::Stdin);
            }
            other => panic!("{other:?}"),
        }
    }

    /// The mistake this refusal exists for: one suffix short of the intended
    /// name armed a live policy on a maintainer session.
    ///
    /// Every other verb takes an alias because a wrong one costs a misdelivered
    /// message. A renew aimed at the wrong agent clears its conversation once a
    /// cycle, so the alias is not offered at all.
    #[test]
    fn renew_refuses_an_alias_and_says_what_to_use_instead() {
        let error = parse_invocation(&args(&[
            "renew",
            "divine-work",
            "--prepare-prompt",
            "checkpoint",
            "--prompt",
            "resume",
            "--on-timeout",
            "abort",
        ]))
        .expect_err("an alias may resolve to somebody else");
        assert!(error.contains("divine-work"), "{error}");
        assert!(
            error.contains("clears its conversation every cycle"),
            "the refusal says what it is protecting: {error}"
        );
        assert!(
            error.contains("--recipient-id"),
            "and how to target another agent deliberately: {error}"
        );
    }

    /// No recipient means the caller, which is what makes the re-arm probe safe.
    ///
    /// The duplicate refusal is scoped per incarnation, so it only protects a
    /// caller aiming at itself. Defaulting to self makes that true by
    /// construction rather than by the caller aiming correctly.
    #[test]
    fn name_info_takes_one_positional_alias() {
        match parse_invocation(&args(&["name-info", "divine-work"])).expect("parse") {
            Invocation::Typed {
                command: Command::NameInfo { name },
                ..
            } => assert_eq!(name, "divine-work"),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn renew_with_no_recipient_targets_the_caller() {
        let invocation = parse_invocation(&args(&[
            "renew",
            "--prepare-prompt",
            "checkpoint",
            "--prompt",
            "resume",
            "--on-timeout",
            "abort",
            "--every",
            "45m",
        ]))
        .expect("parse");
        match invocation {
            Invocation::Typed {
                command: Command::Renew(renew),
                ..
            } => {
                assert_eq!(renew.recipient, None, "no target means the caller");
                assert_eq!(renew.every_ms, Some(45 * 60 * 1_000));
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn renew_still_takes_a_deliberate_exact_target() {
        let invocation = parse_invocation(&args(&[
            "renew",
            "--recipient-id",
            "agent-1",
            "--recipient-incarnation",
            "inc-1",
            "--prepare-prompt",
            "checkpoint",
            "--prompt",
            "resume",
            "--on-timeout",
            "abort",
        ]))
        .expect("parse");
        match invocation {
            Invocation::Typed {
                command: Command::Renew(renew),
                ..
            } => {
                let exact = renew.recipient.expect("exact target");
                assert_eq!(exact.recipient, "agent-1");
                assert_eq!(exact.incarnation, "inc-1");
            }
            other => panic!("{other:?}"),
        }
    }

    /// Half an exact target is the shape most likely to be a typo.
    #[test]
    fn renew_refuses_half_an_exact_target() {
        let error = parse_invocation(&args(&[
            "renew",
            "--recipient-id",
            "agent-1",
            "--prepare-prompt",
            "checkpoint",
            "--prompt",
            "resume",
            "--on-timeout",
            "abort",
        ]))
        .expect_err("an incarnation is required with an agent id");
        assert!(error.contains("--recipient-incarnation"), "{error}");
    }

    #[test]
    fn renew_cancel_requires_a_stated_reason() {
        let error = parse_invocation(&args(&["renew-cancel", "renew-1"]))
            .expect_err("a policy that ends silently is the thing being fixed");
        assert!(error.contains("--reason"), "{error}");

        let invocation = parse_invocation(&args(&[
            "renew-cancel",
            "renew-1",
            "--reason",
            "wrong agent",
        ]))
        .expect("parse");
        match invocation {
            Invocation::Typed {
                command:
                    Command::RenewCancel {
                        renew_id, reason, ..
                    },
                ..
            } => {
                assert_eq!(renew_id, "renew-1");
                assert_eq!(reason, "wrong agent");
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn typed_clear_uses_the_message_recipient_shapes() {
        let alias = parse_invocation(&args(&["clear", "worker"])).expect("alias");
        assert!(matches!(
            alias,
            Invocation::Typed {
                command: Command::Clear {
                    recipient: Recipient::Alias(ref name),
                    ..
                },
                ..
            } if name == "worker"
        ));

        let exact = parse_invocation(&args(&[
            "clear",
            "--recipient-id",
            "agent-1",
            "--recipient-incarnation",
            "inc-1",
        ]))
        .expect("exact");
        assert!(matches!(
            exact,
            Invocation::Typed {
                command: Command::Clear {
                    recipient: Recipient::Exact(ExactRecipient {
                        ref recipient,
                        ref incarnation,
                    }),
                    ..
                },
                ..
            } if recipient == "agent-1" && incarnation == "inc-1"
        ));
    }

    #[test]
    fn explicit_socket_path_stays_raw() {
        let invocation = parse_invocation(&args(&["/tmp/kelpie.sock"])).expect("parse");
        assert!(matches!(invocation, Invocation::Raw { .. }));
    }

    #[test]
    fn body_flags_are_exclusive() {
        let error = parse_invocation(&args(&["tell", "quorum", "--stdin", "--body", "nope"]))
            .expect_err("exclusive");
        assert!(error.contains("exactly one"));
    }

    #[test]
    fn reply_requires_one_disposition() {
        let error = parse_invocation(&args(&["reply", "ask-1", "--stdin"])).expect_err("disp");
        assert!(error.contains("--progress"));
    }

    #[test]
    fn read_body_preserves_metacharacters_quotes_unicode_and_html() {
        let expected = "line1 `ls` $(ls) \"quotes\" 'apos'\n<kelpie from=x>\nunicodé Δ\n";
        let directory = tempfile::tempdir().expect("temp");
        let path = directory.path().join("body.txt");
        fs::write(&path, expected).expect("write");
        let got = read_body(&BodySource::File(path), &mut io::empty()).expect("read");
        assert_eq!(got, expected);
        let got = read_body(&BodySource::Literal(expected.into()), &mut io::empty()).expect("lit");
        assert_eq!(got, expected);
        let got = read_body(&BodySource::Stdin, &mut expected.as_bytes()).expect("stdin");
        assert_eq!(got, expected);
    }

    #[test]
    fn message_params_prefer_alias_unless_exact_ids() {
        let alias = message_params("sender", Some("quorum"), None, "hello `ls`", "key", None);
        assert_eq!(alias["recipient_alias"], "quorum");
        assert_eq!(alias["body"], "hello `ls`");
        assert!(alias.get("recipient").is_none());
        let exact = message_params(
            "sender",
            Some("ignored"),
            Some(&ExactRecipient {
                recipient: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee".into(),
                incarnation: "ffffffff-bbbb-cccc-dddd-eeeeeeeeeeee".into(),
            }),
            "body",
            "key",
            None,
        );
        assert!(exact.get("recipient_alias").is_none());
        assert!(exact.get("recipient").is_some());
    }

    #[test]
    fn name_info_distinguishes_active_and_retired_socket_waiters() {
        let response = serde_json::json!({"result":{
            "name":"botserver",
            "claimants":[
                {"logical_agent_id":"active","created_at_ms":1,
                 "delivery_transport":"socket_inbox","live":false,"addressable":true,
                 "unresolved_count":0},
                {"logical_agent_id":"retired","created_at_ms":2,
                 "delivery_transport":"socket_inbox","live":false,"addressable":false,
                 "unresolved_count":0}
            ],
            "unresolved":[]
        }});
        let text = format_receipt("name.info", &response);
        assert!(
            text.contains("active-waiter transport=socket_inbox"),
            "{text}"
        );
        assert!(
            text.contains("retired-waiter transport=socket_inbox"),
            "{text}"
        );
        assert!(!text.contains("active created=1970-01-01T00:00:00.001Z not-live"));
    }

    #[test]
    fn generated_ids_are_unique() {
        assert_ne!(generated_id(), generated_id());
    }

    #[test]
    fn the_report_explains_itself_to_a_reader_with_no_prior_context() {
        let now = parse_utc_rfc3339_ms("2026-08-16T00:00:00Z").expect("parse");
        let report = serde_json::json!({"result":{
            "generated_at_ms": now,
            "live_snapshot_at_ms": now,
            "alias_collisions": {"reviewer": ["a", "b"]},
            "agents": [
                {"agent_id":"parent-id","public_name":"coordinator",
                 "parent_agent_id":null,"explicitly_parentless":true,"created_at_ms":0,
                 "incarnations":[{"incarnation_id":"i1","state":"ready",
                    "backend_kind":"opencode","live":"idle"}]},
                {"agent_id":"child-id","public_name":"reviewer",
                 "parent_agent_id":"parent-id","explicitly_parentless":false,
                 "created_at_ms":0,
                 "incarnations":[{"incarnation_id":"i2","state":"ready",
                    "backend_kind":"claude","live":"working"}]}
            ],
            "obligations": [
                {"ask_message_id":"ask-1","owing_agent_id":"child-id",
                 "waiting_agent_id":"parent-id","state":"open",
                 "created_at_ms": now - 5_400_000_i64}
            ]
        }});
        let text = format_receipt("report", &report);

        // It says what it is and what the shape means.
        assert!(text.contains("nested under"), "{text}");
        assert!(text.contains("started by the line above"), "{text}");
        // Each fact is attributed to the authority that owns it.
        assert!(text.contains("kelpie=ready"), "{text}");
        assert!(text.contains("herdr=working"), "{text}");
        // Parentage is drawn, not inferred from whitespace.
        assert!(text.contains("└─ reviewer"), "{text}");
        // Durations are read, not converted.
        assert!(text.contains("unanswered-for=1h30m"), "{text}");
        assert!(!text.contains("open-ms="), "{text}");
        // Epoch milliseconds never reach the reader.
        assert!(text.contains("generated=2026-08-16T00:00:00Z"), "{text}");
        // Reusable names are a footnote, after the tree, and explained.
        let collisions = text.find("reviewer agents=2").expect("collision line");
        assert!(
            collisions > text.find("└─ reviewer").expect("tree"),
            "{text}"
        );
        assert!(text.contains("history, not a fault"), "{text}");
    }

    #[test]
    fn conversation_age_is_reported_only_when_it_was_actually_observed() {
        let now = parse_utc_rfc3339_ms("2026-08-16T00:00:00Z").expect("parse");
        let report = serde_json::json!({"result":{
            "generated_at_ms": now,
            "agents": [
                {"agent_id":"measured-id","public_name":"measured",
                 "parent_agent_id":null,"explicitly_parentless":true,"created_at_ms":0,
                 "incarnations":[{"incarnation_id":"i1","state":"ready",
                    "backend_kind":"claude","created_at_ms": now - 259_200_000_i64,
                    "native_session_rotated_at_ms": now - 5_400_000_i64}]},
                {"agent_id":"unseen-id","public_name":"unseen",
                 "parent_agent_id":null,"explicitly_parentless":true,"created_at_ms":0,
                 "incarnations":[{"incarnation_id":"i2","state":"ready",
                    "backend_kind":"claude","created_at_ms": now - 259_200_000_i64,
                    "native_session_rotated_at_ms": null}]}
            ],
            "obligations": []
        }});
        let text = format_receipt("report", &report);

        assert!(text.contains("conversation=1h30m"), "{text}");
        // The agent has been bound for three days and its context is 90 minutes
        // old. Reporting 3d here is the exact wrong answer this measurement
        // exists to avoid, so an unobserved start says so instead.
        assert!(text.contains("conversation=unknown"), "{text}");
        assert!(!text.contains("conversation=3d0h"), "{text}");
        // The legend has to distinguish the two ages, or the number is a trap.
        assert!(text.contains("not the agent's age"), "{text}");
    }

    /// An armed root and an unarmed one must not read the same.
    ///
    /// A policy ends when its incarnation stops being Ready, and an agent
    /// adopted back afterwards looks identical in every other field.
    #[test]
    fn report_distinguishes_an_armed_root_from_an_unarmed_one() {
        let now = parse_utc_rfc3339_ms("2026-08-23T00:00:00Z").expect("parse");
        let report = serde_json::json!({"result":{
            "generated_at_ms": now,
            "agents": [
                {"agent_id":"armed-id","public_name":"armed",
                 "parent_agent_id":null,"explicitly_parentless":true,"created_at_ms":0,
                 "incarnations":[{"incarnation_id":"i1","state":"ready",
                    "backend_kind":"opencode","created_at_ms": now - 3_600_000_i64,
                    "native_session_rotated_at_ms": now - 600_000_i64,
                    "renew":{"renew_id":"r1","phase":"scheduled","cycle":97,
                             "every_ms":2_700_000_i64,
                             "cycle_due_at_ms": now + 900_000_i64}}]},
                {"agent_id":"bare-id","public_name":"unarmed",
                 "parent_agent_id":null,"explicitly_parentless":true,"created_at_ms":0,
                 "incarnations":[{"incarnation_id":"i2","state":"ready",
                    "backend_kind":"opencode","created_at_ms": now - 3_600_000_i64,
                    "native_session_rotated_at_ms": now - 600_000_i64,
                    "renew": null}]}
            ],
            "obligations": []
        }});
        let text = format_receipt("report", &report);

        // Exact, not a prefix. `contains("every=45m")` also passes on the
        // `45m0s` this actually renders, which is how the documented example
        // drifted from the real output without a test noticing.
        assert!(
            text.contains("renew=scheduled cycle=97 every=45m0s next-in=15m0s"),
            "{text}"
        );
        // The unarmed root says nothing about renew, and that silence is the
        // signal: it is the only Ready row without one.
        let unarmed = text
            .lines()
            .find(|line| line.contains("unarmed"))
            .expect("unarmed row");
        assert!(!unarmed.contains("renew="), "{unarmed}");
    }

    #[test]
    fn ask_refuses_a_due_time_and_says_what_to_use_instead() {
        // A worker wrote `ask ... --due-in 45m` meaning "check back later" and
        // got the opposite: the work was withheld for 45 minutes while the
        // obligation existed unseen. The flag is gone from ask.
        for flag in [
            vec!["ask", "reviewer", "--body", "x", "--due-in", "45m"],
            vec![
                "ask",
                "reviewer",
                "--body",
                "x",
                "--due-at",
                "2026-08-15T23:35:08Z",
            ],
            vec![
                "ask",
                "reviewer",
                "--body",
                "x",
                "--due-at-ms",
                "1786800908000",
            ],
        ] {
            let args: Vec<String> = flag.iter().map(|value| (*value).to_string()).collect();
            let error = parse_message_command(&args).expect_err("ask refuses a due time");
            assert!(error.contains("--remind-after-ms"), "{error}");
            assert!(error.contains("tell"), "{error}");
        }
        // tell keeps it: no obligation, so a delayed tell is just a later message.
        let args: Vec<String> = ["tell", "coordinator", "--body", "x", "--due-in", "45m"]
            .iter()
            .map(|value| (*value).to_string())
            .collect();
        parse_message_command(&args).expect("tell still schedules");
    }

    #[test]
    fn a_queued_delivery_receipt_cannot_be_skimmed_as_a_dispatch() {
        // A worker read `delivery=queued due=<epoch>` as successful dispatch and
        // two children sat idle for 45 minutes. The receipt now says so in words.
        // Derive the epoch from the parser rather than inventing a constant.
        let due_at = parse_utc_rfc3339_ms("2026-08-15T23:35:08Z").expect("parse");
        let text = format_receipt(
            "ask",
            &serde_json::json!({"result":{
                "message_id":"m","operation_id":"o","recipient":"r",
                "delivery_outcome":"queued","due_at_ms":due_at
            }}),
        );
        assert!(text.contains("delivery=queued"), "{text}");
        assert!(
            text.contains("NOT-SENT-UNTIL=2026-08-15T23:35:08Z"),
            "{text}"
        );
        assert!(text.contains(&format!("due-at-ms={due_at}")), "{text}");
    }

    #[test]
    fn utc_rendering_round_trips_the_due_at_parser() {
        for stamp in [
            "2026-08-15T23:35:08Z",
            "1970-01-01T00:00:00Z",
            "2000-02-29T12:00:00Z",
            "2100-12-31T23:59:59Z",
        ] {
            let ms = parse_utc_rfc3339_ms(stamp).expect("parse");
            assert_eq!(format_utc_ms(ms), stamp);
        }
    }

    #[test]
    fn receipt_does_not_hide_errors() {
        let text = format_receipt(
            "tell",
            &json!({"id":"1","error":{"class":"unknown_outcome","message":"ambiguous"}}),
        );
        assert!(text.contains("unknown_outcome"));
        assert!(text.contains("ambiguous"));
    }

    #[test]
    fn cancel_receipt_names_both_audiences() {
        let text = format_receipt(
            "cancel",
            &json!({"result":{
                "state":"cancelled",
                "response":"delivered",
                "message_id":"ask-notice",
                "owing_response":"recorded",
                "owing_message_id":"owing-notice"
            }}),
        );
        assert!(text.contains("response=delivered"), "{text}");
        assert!(text.contains("owing-response=recorded"), "{text}");
        assert!(text.contains("owing-message=owing-notice"), "{text}");
    }

    #[test]
    fn waiter_retire_receipt_joins_ids_and_counts_owing_notices() {
        let text = format_receipt(
            "waiter.retire",
            &json!({"result":{
                "logical_agent_id":"waiter-1",
                "targeting_ended":true,
                "cancelled_ask_ids":["ask-a","ask-b"],
                "owing_notices":[
                    {"ask_message_id":"ask-a","message_id":"n1","owing_response":"delivered"},
                    {"ask_message_id":"ask-b","message_id":"n2","owing_response":"recorded"}
                ]
            }}),
        );
        assert!(text.contains("cancelled-asks=ask-a,ask-b"), "{text}");
        assert!(text.contains("owing-delivered=1"), "{text}");
        assert!(text.contains("owing-recorded=1"), "{text}");
        let empty = format_receipt(
            "waiter.retire",
            &json!({"result":{
                "logical_agent_id":"waiter-1",
                "targeting_ended":true,
                "cancelled_ask_ids":[],
                "owing_notices":[]
            }}),
        );
        assert!(empty.contains("cancelled-asks=none"), "{empty}");
        assert!(empty.contains("owing-delivered=0"), "{empty}");
    }

    #[test]
    fn tell_accepts_exact_ids_without_alias() {
        let invocation = parse_invocation(&args(&[
            "tell",
            "--recipient-id",
            "aaaaaaaa-bbbb-7ccc-dddd-eeeeeeeeeeee",
            "--recipient-incarnation",
            "ffffffff-bbbb-7ccc-dddd-eeeeeeeeeeee",
            "--stdin",
        ]))
        .expect("parse");
        match invocation {
            Invocation::Typed {
                command: Command::Tell { recipient, .. },
                ..
            } => {
                assert_eq!(
                    recipient,
                    Recipient::Exact(ExactRecipient {
                        recipient: "aaaaaaaa-bbbb-7ccc-dddd-eeeeeeeeeeee".into(),
                        incarnation: "ffffffff-bbbb-7ccc-dddd-eeeeeeeeeeee".into(),
                    })
                );
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn tell_rejects_mixed_alias_and_exact_ids() {
        let error = parse_invocation(&args(&[
            "tell",
            "quorum",
            "--recipient-id",
            "aaaaaaaa-bbbb-7ccc-dddd-eeeeeeeeeeee",
            "--recipient-incarnation",
            "ffffffff-bbbb-7ccc-dddd-eeeeeeeeeeee",
            "--stdin",
        ]))
        .expect_err("mixed");
        assert!(error.contains("exactly one"), "{error}");
    }

    #[test]
    fn tell_accepts_due_at_ms() {
        let invocation = parse_invocation(&args(&[
            "tell",
            "quorum",
            "--due-at-ms",
            "1770000000000",
            "--stdin",
        ]))
        .expect("parse");
        match invocation {
            Invocation::Typed {
                command: Command::Tell { due, .. },
                ..
            } => assert_eq!(due, Some(Due::AtMs(1_770_000_000_000))),
            other => panic!("{other:?}"),
        }
        let error = parse_invocation(&args(&["tell", "quorum", "--due-at-ms", "-1", "--stdin"]))
            .expect_err("negative");
        assert!(error.contains("non-negative"), "{error}");
    }

    fn parsed_due(args_in: &[&str]) -> Due {
        match parse_invocation(&args(args_in)).expect("parse") {
            Invocation::Typed {
                command: Command::Tell { due, .. },
                ..
            } => due.expect("a due time"),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn due_in_is_a_relative_offset_and_due_at_is_absolute() {
        assert_eq!(
            parsed_due(&["tell", "quorum", "--due-in", "10m", "--stdin"]),
            Due::InMs(600_000)
        );
        assert_eq!(
            parsed_due(&["tell", "quorum", "--due-in", "2h", "--stdin"]),
            Due::InMs(7_200_000)
        );
        assert_eq!(
            parsed_due(&["tell", "quorum", "--due-in", "1d", "--stdin"]),
            Due::InMs(86_400_000)
        );
        // The epoch itself, and a date past a leap day, both land exactly.
        assert_eq!(
            parsed_due(&[
                "tell",
                "quorum",
                "--due-at",
                "1970-01-01T00:00:00Z",
                "--stdin"
            ]),
            Due::AtMs(0)
        );
        assert_eq!(
            parsed_due(&[
                "tell",
                "quorum",
                "--due-at",
                "2026-08-12T20:00:00Z",
                "--stdin"
            ]),
            Due::AtMs(1_786_564_800_000)
        );
        assert_eq!(
            parsed_due(&[
                "tell",
                "quorum",
                "--due-at",
                "2024-03-01T00:00:00+00:00",
                "--stdin"
            ]),
            Due::AtMs(1_709_251_200_000)
        );
    }

    #[test]
    fn due_flags_fail_loudly_rather_than_at_the_wrong_time() {
        let cases = [
            (vec!["--due-in", "10x"], "unit must be"),
            (vec!["--due-in", "soon"], "must look like"),
            (vec!["--due-in", "0m"], "positive"),
            (vec!["--due-at", "2026-08-12 20:00:00Z"], "RFC3339"),
            (vec!["--due-at", "2026-08-12T20:00:00"], "ending in Z"),
            (vec!["--due-at", "2026-08-12T20:00:00-03:00"], "ending in Z"),
            (vec!["--due-at", "2026-13-12T20:00:00Z"], "date is invalid"),
            (vec!["--due-at", "2026-08-12T25:00:00Z"], "time is invalid"),
        ];
        for (flags, expected) in cases {
            let mut argv = vec!["tell", "quorum"];
            argv.extend(flags.iter().copied());
            argv.push("--stdin");
            let error = parse_invocation(&args(&argv)).expect_err("rejected");
            assert!(error.contains(expected), "{argv:?} gave {error}");
        }
        // Only one form at a time; two would be an ambiguous instruction.
        let conflict = parse_invocation(&args(&[
            "tell",
            "quorum",
            "--due-in",
            "10m",
            "--due-at-ms",
            "1770000000000",
            "--stdin",
        ]))
        .expect_err("two forms");
        assert!(conflict.contains("only one of"), "{conflict}");
    }

    #[test]
    fn tell_rejects_unknown_duplicate_and_extra_tokens() {
        let unknown = parse_invocation(&args(&["tell", "quorum", "--stdin", "--quiet"]))
            .expect_err("unknown");
        assert!(unknown.contains("unknown argument"), "{unknown}");
        let duplicate =
            parse_invocation(&args(&["tell", "quorum", "--stdin", "--stdin"])).expect_err("dup");
        assert!(duplicate.contains("more than once"), "{duplicate}");
        let extra =
            parse_invocation(&args(&["tell", "quorum", "spare", "--stdin"])).expect_err("extra");
        assert!(extra.contains("unexpected argument"), "{extra}");
        let incomplete = parse_invocation(&args(&[
            "tell",
            "--recipient-id",
            "aaaaaaaa-bbbb-7ccc-dddd-eeeeeeeeeeee",
            "--stdin",
        ]))
        .expect_err("incomplete");
        assert!(incomplete.contains("exactly one"), "{incomplete}");
    }

    #[test]
    fn pending_and_whoami_reject_multiple_target_forms() {
        let pending =
            parse_invocation(&args(&["pending", "alice", "--pane", "w1:p1"])).expect_err("pending");
        assert!(pending.contains("only one target form"), "{pending}");
        let whoami =
            parse_invocation(&args(&["whoami", "alice", "--sender", "bob"])).expect_err("whoami");
        assert!(whoami.contains("only one target form"), "{whoami}");
        let whoami_id =
            parse_invocation(&args(&["whoami", "--sender-id", "agent-1"])).expect_err("whoami id");
        assert!(whoami_id.contains("--sender-id"), "{whoami_id}");
    }

    #[test]
    fn attribution_accepts_exactly_one_target_form() {
        let by_incarnation =
            parse_invocation(&args(&["attribution", "--incarnation-id", "inc-1"])).expect("parse");
        match by_incarnation {
            Invocation::Typed {
                command: Command::Attribution { target, .. },
                ..
            } => assert_eq!(target, AttributionTarget::Incarnation("inc-1".into())),
            other => panic!("{other:?}"),
        }
        let by_alias = parse_invocation(&args(&["attribution", "reviewer"])).expect("parse");
        match by_alias {
            Invocation::Typed {
                command: Command::Attribution { target, .. },
                ..
            } => assert_eq!(target, AttributionTarget::Alias("reviewer".into())),
            other => panic!("{other:?}"),
        }
        assert_eq!(
            attribution_params(&AttributionTarget::Agent("agent-1".into())),
            json!({"agent_id": "agent-1"})
        );
        let conflicting =
            parse_invocation(&args(&["attribution", "reviewer", "--agent-id", "agent-1"]))
                .expect_err("two targets");
        assert!(conflicting.contains("exactly one target"), "{conflicting}");
        assert!(parse_invocation(&args(&["attribution", "--agent-id"])).is_err());
        assert!(parse_invocation(&args(&["attribution", "--nope", "x"])).is_err());
    }

    #[test]
    fn attribution_receipt_keeps_requested_and_observed_apart() {
        let reported = format_receipt(
            "attribution",
            &json!({"result": {
                "public_name": "reviewer",
                "logical_agent_id": "agent-1",
                "incarnation_id": "inc-1",
                "backend_kind": "codex",
                "incarnation_state": "ready",
                "requested": {"model": "requested-only"},
                "observed": {
                    "recorded_at_ms": 42,
                    "adapter": "codex",
                    "model": {"status": "reported", "value": "o3"},
                    "provider": {"status": "reported", "value": "openai"},
                    "effort": {"status": "undetermined"}
                }
            }}),
        );
        assert!(
            reported.contains("requested model=requested-only"),
            "{reported}"
        );
        assert!(
            reported
                .contains("observed adapter=codex model=o3 provider=openai effort=undetermined"),
            "{reported}"
        );
        // The requested value must never appear on the observed line.
        let observed_line = reported
            .lines()
            .find(|line| line.starts_with("observed"))
            .expect("observed line");
        assert!(!observed_line.contains("requested-only"));

        let none = format_receipt(
            "attribution",
            &json!({"result": {
                "public_name": "fresh",
                "logical_agent_id": "agent-1",
                "incarnation_id": "inc-1",
                "backend_kind": "grok",
                "incarnation_state": "declared",
                "requested": {},
                "observed": null,
                "observations": []
            }}),
        );
        assert!(none.contains("observed none"), "{none}");
        assert!(
            none.contains("requested model=- provider=- effort=-"),
            "{none}"
        );
    }

    #[test]
    fn positional_after_flags_does_not_steal_flag_values() {
        let invocation = parse_invocation(&args(&["tell", "--stdin", "quorum"])).expect("parse");
        match invocation {
            Invocation::Typed {
                command: Command::Tell {
                    recipient, body, ..
                },
                ..
            } => {
                assert_eq!(recipient, Recipient::Alias("quorum".into()));
                assert_eq!(body, BodySource::Stdin);
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn receipt_shows_delivery_outcomes() {
        for (method, outcome) in [
            ("tell", "accepted"),
            ("ask", "rejected"),
            ("tell", "target_unavailable"),
            ("ask", "unknown"),
        ] {
            let text = format_receipt(
                method,
                &json!({"result":{"message_id":"m","operation_id":"o","recipient":"r","delivery_outcome":outcome}}),
            );
            assert!(text.contains(&format!("delivery={outcome}")), "{text}");
        }
    }

    fn start_args() -> Vec<String> {
        args(&[
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
        ])
    }

    #[test]
    fn typed_start_wraps_required_start_intent_fields() {
        let invocation = parse_invocation(&start_args()).expect("parse");
        match invocation {
            Invocation::Typed {
                command: Command::Start(start),
                ..
            } => {
                assert_eq!(start.parent, StartParent::Parentless);
                let StartCommand {
                    public_name,
                    pane_id,
                    terminal_id,
                    backend_kind,
                    backend_args,
                    initial_kind,
                    initial_sender,
                    body,
                    working_directory,
                    readiness_timeout_ms,
                    keep_open,
                    herdr_session,
                    logical_agent_id,
                    ..
                } = *start;
                assert_eq!(public_name, "worker");
                assert_eq!(pane_id, "w1:p1");
                assert_eq!(terminal_id, "term-1");
                assert_eq!(backend_kind, "codex");
                assert!(backend_args.is_empty());
                assert_eq!(initial_kind, "tell");
                assert_eq!(initial_sender, None);
                assert_eq!(body, BodySource::Literal("hello `ls`".into()));
                assert_eq!(working_directory, "/tmp/work");
                assert_eq!(readiness_timeout_ms, 5000);
                assert!(keep_open);
                assert_eq!(herdr_session, "default");
                assert_eq!(logical_agent_id, None);
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn typed_start_accepts_continue_parent_ask_and_repeated_args() {
        let invocation = parse_invocation(&args(&[
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
            "15000",
            "--no-keep-open",
            "--parent-id",
            "aaaaaaaa-bbbb-7ccc-dddd-eeeeeeeeeeee",
            "--ask",
            "--sender-id",
            "ffffffff-bbbb-7ccc-dddd-eeeeeeeeeeee",
            "--stdin",
            "--arg",
            "--model",
            "--arg",
            "grok",
            "--logical-id",
            "11111111-bbbb-7ccc-dddd-eeeeeeeeeeee",
            "--session",
            "alpha",
        ]))
        .expect("parse");
        match invocation {
            Invocation::Typed {
                command: Command::Start(start),
                ..
            } => {
                let StartCommand {
                    parent,
                    initial_kind,
                    initial_sender,
                    backend_args,
                    keep_open,
                    logical_agent_id,
                    herdr_session,
                    ..
                } = *start;
                assert_eq!(
                    parent,
                    StartParent::Agent("aaaaaaaa-bbbb-7ccc-dddd-eeeeeeeeeeee".into())
                );
                assert_eq!(initial_kind, "ask");
                assert_eq!(
                    initial_sender.as_deref(),
                    Some("ffffffff-bbbb-7ccc-dddd-eeeeeeeeeeee")
                );
                assert_eq!(backend_args, vec!["--model", "grok"]);
                assert!(!keep_open);
                assert_eq!(
                    logical_agent_id.as_deref(),
                    Some("11111111-bbbb-7ccc-dddd-eeeeeeeeeeee")
                );
                assert_eq!(herdr_session, "alpha");
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn typed_start_rejects_unknown_conflicts_and_missing_ask_sender() {
        let unknown = parse_invocation(&{
            let mut items = start_args();
            items.push("--quiet".into());
            items
        })
        .expect_err("unknown");
        assert!(unknown.contains("unknown argument"), "{unknown}");
        let both_parent = parse_invocation(&{
            let mut items = start_args();
            items.extend(args(&[
                "--parent-id",
                "aaaaaaaa-bbbb-7ccc-dddd-eeeeeeeeeeee",
            ]));
            items
        })
        .expect_err("parent");
        assert!(both_parent.contains("--parentless"), "{both_parent}");
        let both_kind = parse_invocation(&{
            let mut items = start_args();
            items.push("--ask".into());
            items
        })
        .expect_err("kind");
        assert!(both_kind.contains("--tell"), "{both_kind}");
        // An ask without --sender-id now parses; the client resolves the caller
        // and the store still refuses an operator-attributed ask.
        let ask_no_sender = parse_invocation(&args(&[
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
            "--ask",
            "--body",
            "q",
        ]))
        .expect("ask defaults its sender to the caller");
        match ask_no_sender {
            Invocation::Typed {
                command: Command::Start(start),
                ..
            } => {
                assert_eq!(start.initial_kind, "ask");
                assert_eq!(start.initial_sender, None);
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn typed_start_reports_every_missing_requirement_in_one_error() {
        // The failure this guards against is not the bare error text. A caller
        // that learns one requirement per invocation rewrites its command line
        // six times, and a rewrite drops arguments the parser never asked for —
        // which is how an agent reached a pane without its permission flag.
        let bare = parse_invocation(&args(&["start"])).expect_err("nothing supplied");
        for flag in [
            "--name",
            "--pane",
            "--terminal",
            "--backend",
            "--cwd",
            "--timeout-ms",
        ] {
            assert!(bare.contains(&format!("missing {flag}")), "{bare}");
        }
        assert!(bare.contains("--keep-open"), "{bare}");
        assert!(bare.contains("--parentless"), "{bare}");
        assert!(bare.contains("--tell"), "{bare}");
        assert!(bare.contains("--stdin"), "{bare}");
        assert!(bare.starts_with("start: 10 problems"), "{bare}");
    }

    #[test]
    fn typed_start_names_a_wrong_flag_and_the_requirement_it_missed_together() {
        // Guessed flag names were the other half of the same thrash: the
        // caller has to see that --alias is not a flag AND that --name is
        // required, or it fixes one and gets rejected again for the other.
        let guessed = parse_invocation(&{
            let mut items = start_args();
            let name = items
                .iter()
                .position(|item| item == "--name")
                .expect("--name present");
            items[name] = "--alias".into();
            items
        })
        .expect_err("guessed flag name");
        assert!(guessed.contains("unknown argument --alias"), "{guessed}");
        assert!(guessed.contains("missing --name"), "{guessed}");
        // The value behind an unrecognised flag is part of that flag's mistake,
        // not a second stray positional to report on its own.
        assert!(!guessed.contains("unexpected argument"), "{guessed}");
    }

    #[test]
    fn typed_start_keeps_a_lone_problem_on_one_line() {
        let single = parse_invocation(&{
            let mut items = start_args();
            let cwd = items
                .iter()
                .position(|item| item == "--cwd")
                .expect("--cwd present");
            items.drain(cwd..=cwd + 1);
            items
        })
        .expect_err("one requirement missing");
        assert_eq!(single, "start: missing --cwd");
    }

    #[test]
    fn a_global_flag_after_the_command_is_told_where_it_belongs() {
        // The rewrite that dropped --dangerously-skip-permissions ended on this
        // rejection, so "unknown argument --json" was the last thing standing
        // between the caller and a correct command line.
        let misplaced = parse_invocation(&{
            let mut items = start_args();
            items.push("--json".into());
            items
        })
        .expect_err("global flag after the command");
        assert_eq!(
            misplaced,
            "start: --json is a global flag and goes before the command, as \
             `kelpie --json <command> ...`"
        );
        parse_invocation(&{
            let mut items = args(&["--json"]);
            items.extend(start_args());
            items
        })
        .expect("the placement the error points at parses");
    }

    #[test]
    fn typed_adopt_reports_both_of_its_requirements_together() {
        let bare = parse_invocation(&args(&["adopt"])).expect_err("nothing supplied");
        assert_eq!(
            bare,
            "adopt: 2 problems\n  missing --pane\n  missing --terminal"
        );
    }

    #[test]
    fn start_receipt_shows_separate_runtime_and_message_outcomes() {
        let text = format_receipt(
            "start",
            &json!({
                "result": {
                    "logical_agent_id": "a",
                    "incarnation_id": "i",
                    "runtime_start": {"outcome": "succeeded", "operation_id": "o"},
                    "initial_message": {"outcome": "unknown", "message_id": "m"}
                }
            }),
        );
        assert!(text.contains("runtime=succeeded"), "{text}");
        assert!(text.contains("delivery=unknown"), "{text}");
        assert!(text.contains("message=m"), "{text}");
    }

    #[test]
    fn waiter_register_and_from_operator_parse() {
        let invocation = parse_invocation(&args(&[
            "waiter-register",
            "--name",
            "inbox",
            "--parentless",
        ]))
        .expect("register");
        match invocation {
            Invocation::Typed {
                command:
                    Command::WaiterRegister {
                        public_name,
                        parent: StartParent::Parentless,
                        ..
                    },
                ..
            } => assert_eq!(public_name, "inbox"),
            other => panic!("{other:?}"),
        }
        let ask = parse_invocation(&args(&[
            "ask",
            "owing",
            "--sender-id",
            "aaaaaaaa-bbbb-7ccc-dddd-eeeeeeeeeeee",
            "--from",
            "operator",
            "--body",
            "q",
        ]))
        .expect("ask");
        match ask {
            Invocation::Typed {
                command: Command::Ask { from_operator, .. },
                ..
            } => assert!(from_operator),
            other => panic!("{other:?}"),
        }
        let bad = parse_invocation(&args(&["ask", "owing", "--from", "relay", "--body", "q"]))
            .expect_err("from");
        assert!(bad.contains("operator"), "{bad}");
    }
}
