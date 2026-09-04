//! Foreground local daemon for Kelpie's newline-delimited JSON protocol.

use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::Shutdown;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

use crate::domain::{
    AdoptIntent, IncarnationId, LogicalAgentId, MessageId, OperationId, Parent, RenewId,
    RenewPhase, RenewTimeout, ReplyDisposition, StartIntent,
};
use crate::herdr::HerdrError;
use crate::herdr_exec::{FailPhase, HerdrEvent, HerdrExec, HerdrJob, HerdrJobResult, LeaseCmd};
use crate::slice::{
    AdoptAfterSnapshot, AdoptRename, AwaitingClear, CancelOutcome, ClearResult, ClearSubmission,
    Kelpie, LiveStatus, PreparedCancellation, PreparedPrompt, PreparedReminder,
    PreparedWaiterRetire, RenamePreflight, RenameWork, RenewClearWrite, RenewInjectWrite,
    RetireAfterSnapshot, RetireCloseWork, RetirePreflight, SliceError, WaiterRetireOutcome,
    WaiterRetireOwingNotice,
};
use crate::store::{BoundaryReminder, DueReminder, DueRenew, StoreError};

const DEFAULT_REMINDER_INTERVAL_MS: i64 = 300_000;
/// A client must finish sending its request line within this window.
const CLIENT_REQUEST_DEADLINE: Duration = Duration::from_secs(10);
/// Bound response writes so a stuck peer cannot freeze the loop.
const CLIENT_WRITE_TIMEOUT: Duration = Duration::from_secs(5);
/// Log any poll phase or whole pass that runs this long.
const SLOW_POLL: Duration = Duration::from_secs(1);
const MAX_ACCEPTS_PER_POLL: usize = 16;
const MAX_REQUEST_BYTES: usize = 64 * 1024;

/// One local client request. Sender fields are same-user attribution, not authentication.
#[derive(Clone, Debug, Deserialize)]
pub struct ClientRequest {
    pub id: String,
    pub method: String,
    #[serde(default)]
    pub params: Value,
}

/// One local client response correlated by request ID.
#[derive(Debug, Serialize)]
pub struct ClientResponse {
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ClientError>,
}

/// Stable client-visible error classification.
///
/// `class` is the SPEC error taxonomy. `code` is a finer stable identifier for
/// callers that must branch, so no client has to parse `message`.
#[derive(Debug, Serialize)]
pub struct ClientError {
    pub class: &'static str,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
}

/// Foreground daemon failures outside an individual request.
#[derive(Debug, Error)]
pub enum DaemonError {
    #[error("another kelpied is already serving {0}")]
    AlreadyRunning(PathBuf),
    #[error("local socket failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("client request is malformed: {0}")]
    Json(#[from] serde_json::Error),
}

/// One start whose runtime is not Ready yet, and the client still waiting on it.
///
/// The connection is held unanswered on purpose. Readiness is a wait on Herdr's
/// work, so it must not hold the accept loop.
#[derive(Debug)]
struct BusyStartRetry {
    busy_deadline: Instant,
    not_before: Instant,
    #[allow(dead_code)]
    attempt_index: u32,
}

#[derive(Debug)]
enum StartPhase {
    Snapshot,
    Opening {
        busy_deadline: Instant,
        attempt_index: u32,
    },
    Sending {
        busy_deadline: Instant,
        attempt_index: u32,
    },
    Busy(BusyStartRetry),
    Ready,
    Initial {
        prepared: PreparedPrompt,
        message_id: MessageId,
    },
}

#[derive(Debug)]
struct AwaitingStart {
    request_id: String,
    intent: StartIntent,
    declared: Option<crate::store::DeclaredStart>,
    deadline: Instant,
    stream: UnixStream,
    phase: StartPhase,
    herdr_job: Option<u64>,
    lease: Option<std::sync::mpsc::Sender<LeaseCmd>>,
    intent_committed: bool,
    attempt_index: u32,
    busy_deadline: Instant,
}

/// A prompt whose Herdr write is running off-thread.
#[derive(Debug)]
struct AwaitingPrompt {
    request_id: String,
    stream: Option<UnixStream>,
    prepared: PreparedPrompt,
    result_json: Value,
    reply_to: Option<MessageId>,
    lease: Option<std::sync::mpsc::Sender<LeaseCmd>>,
    intent_committed: bool,
    owner: PromptOwner,
    reminder: Option<PreparedReminder>,
}

#[derive(Debug, Clone, Copy)]
enum PromptOwner {
    Client,
    Internal,
    Cancel { session: u64, waiting: bool },
    WaiterRetire { session: u64, ask: MessageId },
}

#[derive(Debug)]
struct AwaitingCancel {
    request_id: String,
    stream: UnixStream,
    outcome: CancelOutcome,
    remaining: u32,
}

#[derive(Debug)]
struct AwaitingWaiterRetire {
    request_id: String,
    stream: UnixStream,
    logical_agent_id: LogicalAgentId,
    outcome: WaiterRetireOutcome,
    remaining: u32,
}

#[derive(Debug, Clone)]
struct ResolvedClear {
    recipient: LogicalAgentId,
    recipient_incarnation: IncarnationId,
    idempotency_key: String,
}

#[derive(Debug, Clone)]
enum AwaitingClearState {
    Settling {
        clear: ResolvedClear,
        not_before_ms: i64,
    },
    Probe {
        clear: ResolvedClear,
        pane_id: String,
    },
    Sending {
        clear: ResolvedClear,
        pane_id: String,
        operation_id: crate::domain::OperationId,
        command: String,
        rotation: crate::slice::RotationTiming,
        pre_clear_session: Value,
        backend_kind: String,
    },
    Rotation(AwaitingClear),
}

#[derive(Debug)]
enum ClearDispatch {
    Complete(ClearResult),
    Awaiting(AwaitingClearState),
}

#[derive(Debug)]
struct AwaitingClearRequest {
    request_id: String,
    state: AwaitingClearState,
    stream: UnixStream,
    herdr_job: Option<u64>,
    lease: Option<std::sync::mpsc::Sender<LeaseCmd>>,
}

/// Long-lived socket-inbox client. One-shot RPCs are not this receive path.
#[derive(Debug)]
struct InboxSession {
    waiter_id: LogicalAgentId,
    stream: UnixStream,
    read_buf: Vec<u8>,
    write_buf: Vec<u8>,
    offered: HashSet<MessageId>,
    awaiting_write_pause: bool,
}

/// A client that connected but has not yet sent a complete NDJSON line.
#[derive(Debug)]
struct ReadingClient {
    stream: UnixStream,
    buf: Vec<u8>,
    accepted_at: Instant,
}

/// A bound foreground daemon. Dropping it removes only the socket it created.
#[derive(Debug)]
pub struct Daemon {
    listener: UnixListener,
    socket_path: PathBuf,
    kelpie: Kelpie,
    awaiting_starts: Vec<AwaitingStart>,
    awaiting_clears: Vec<AwaitingClearRequest>,
    inboxes: Vec<InboxSession>,
    reading: Vec<ReadingClient>,
    herdr_exec: HerdrExec,
    awaiting_prompts: HashMap<u64, AwaitingPrompt>,
    awaiting_cancels: HashMap<u64, AwaitingCancel>,
    awaiting_waiter_retires: HashMap<u64, AwaitingWaiterRetire>,
    next_herdr_job: u64,
    next_cancel_session: u64,
    next_waiter_retire_session: u64,
    reminder_job: Option<u64>,
    pending_reminders: Option<(Vec<DueReminder>, Vec<BoundaryReminder>)>,
    reminder_inflight: HashSet<MessageId>,
    due_inflight: HashSet<OperationId>,
    herdr_owners: HashMap<u64, HerdrOwner>,
    awaiting_reads: HashMap<u64, AwaitingRead>,
    awaiting_renews: Vec<AwaitingRenew>,
    renew_inflight: HashSet<RenewId>,
    awaiting_adopts: Vec<AwaitingAdopt>,
    awaiting_renames: Vec<AwaitingRename>,
    awaiting_retires: Vec<AwaitingRetireClose>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HerdrOwner {
    Prompt,
    Start,
    ReminderSnapshot,
    Clear,
    ClientRead,
    Renew,
    Adopt,
    Rename,
    Retire,
}

#[derive(Debug, Clone)]
enum RenewParkState {
    Probe,
    ClearingSend(RenewClearWrite),
    RotationGet,
    InjectSend(RenewInjectWrite),
    ConfirmGet,
}

#[derive(Debug)]
struct AwaitingRenew {
    item: DueRenew,
    state: RenewParkState,
    herdr_job: Option<u64>,
    lease: Option<std::sync::mpsc::Sender<LeaseCmd>>,
}

#[derive(Debug, Clone, Copy)]
enum AdoptReply {
    Rpc,
    Whoami,
    Who { refresh: bool },
}

#[derive(Debug, Clone)]
enum AdoptParkState {
    Snapshot(AdoptIntent),
    Renaming {
        work: AdoptRename,
        request_id: String,
    },
    Confirm(AdoptRename),
}

#[derive(Debug)]
struct AwaitingAdopt {
    request_id: String,
    stream: UnixStream,
    reply: AdoptReply,
    resume: Option<ClientRequest>,
    state: AdoptParkState,
    herdr_job: Option<u64>,
    lease: Option<std::sync::mpsc::Sender<LeaseCmd>>,
}

#[derive(Clone, Debug)]
enum ClientReadKind {
    Recover,
    Report {
        active: bool,
    },
    Whoami {
        pane_id: String,
        lazy_key: String,
    },
    Alias {
        alias: String,
        lazy_key: String,
        resume: ClientRequest,
    },
    Attribution {
        incarnation_id: IncarnationId,
    },
    WhoAttribution {
        incarnation_id: IncarnationId,
    },
    WhoPane {
        pane_id: String,
        lazy_key: String,
        refresh: bool,
    },
}

#[derive(Debug)]
struct AwaitingRead {
    request_id: String,
    stream: UnixStream,
    kind: ClientReadKind,
}

#[derive(Debug, Clone)]
enum RenameParkState {
    Snapshot(RenamePreflight),
    Opening(RenamePreflight),
    Sending(RenameWork),
    Confirm(RenameWork),
}

#[derive(Debug)]
struct AwaitingRename {
    request_id: String,
    stream: UnixStream,
    state: RenameParkState,
    herdr_job: Option<u64>,
    lease: Option<std::sync::mpsc::Sender<LeaseCmd>>,
}

#[derive(Debug, Clone)]
enum RetireParkState {
    Snapshot(RetirePreflight),
    Closing(RetireCloseWork),
    Confirm(RetireCloseWork),
}

#[derive(Debug)]
struct AwaitingRetireClose {
    request_id: String,
    stream: UnixStream,
    state: RetireParkState,
    herdr_job: Option<u64>,
    lease: Option<std::sync::mpsc::Sender<LeaseCmd>>,
}

/// Make sure nothing else is serving `socket_path` before this process does.
///
/// A daemon that is still alive there answers the probe, and this one must not
/// start: two daemons over one database would each hold intent the other
/// cannot see. A socket file whose owner died without unlinking it, as a
/// `SIGKILL` or a terminal closing does, refuses the connection; that file is
/// removed so the bind can proceed. The probe sends nothing, and the live
/// daemon logs nothing for a client that hangs up before its first byte.
///
/// # Errors
///
/// Returns `AlreadyRunning` when a daemon answers, or the I/O error from an
/// unexpected probe failure or from removing the stale file.
pub fn claim_socket_path(socket_path: &Path) -> Result<(), DaemonError> {
    match UnixStream::connect(socket_path) {
        Ok(_probe) => Err(DaemonError::AlreadyRunning(socket_path.to_path_buf())),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::ConnectionRefused => {
            fs::remove_file(socket_path)?;
            Ok(())
        }
        Err(error) => Err(DaemonError::Io(error)),
    }
}

impl Daemon {
    /// Bind an unused local socket path around an initialized coordinator.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the exact path cannot be bound. Existing paths
    /// are never removed automatically.
    pub fn bind(socket_path: impl AsRef<Path>, kelpie: Kelpie) -> Result<Self, DaemonError> {
        let socket_path = socket_path.as_ref().to_path_buf();
        let listener = UnixListener::bind(&socket_path)?;
        let herdr_exec = HerdrExec::spawn(kelpie.herdr_client().clone());
        Ok(Self {
            listener,
            socket_path,
            kelpie,
            awaiting_starts: Vec::new(),
            awaiting_clears: Vec::new(),
            inboxes: Vec::new(),
            reading: Vec::new(),
            herdr_exec,
            awaiting_prompts: HashMap::new(),
            awaiting_cancels: HashMap::new(),
            awaiting_waiter_retires: HashMap::new(),
            next_herdr_job: 1,
            next_cancel_session: 1,
            next_waiter_retire_session: 1,
            reminder_job: None,
            pending_reminders: None,
            reminder_inflight: HashSet::new(),
            due_inflight: HashSet::new(),
            herdr_owners: HashMap::new(),
            awaiting_reads: HashMap::new(),
            awaiting_renews: Vec::new(),
            renew_inflight: HashSet::new(),
            awaiting_adopts: Vec::new(),
            awaiting_renames: Vec::new(),
            awaiting_retires: Vec::new(),
        })
    }

    /// Serve requests forever in the foreground.
    ///
    /// Accept is non-blocking so due work can fire with no client connected.
    /// The idle sleep is the store-clock remainder until the next due time,
    /// capped so a new client is not delayed indefinitely.
    ///
    /// # Errors
    ///
    /// Returns if accepting a connection or encoding a response fails.
    pub fn run(&mut self) -> Result<(), DaemonError> {
        loop {
            if !self.poll()? {
                thread::sleep(self.kelpie.idle_wait(Duration::from_millis(100)));
            }
        }
    }

    /// Fire due deliveries, pump half-read clients, then accept if anyone is waiting.
    ///
    /// # Errors
    ///
    /// Returns for socket, request decoding, or response encoding failures.
    pub fn poll(&mut self) -> Result<bool, DaemonError> {
        let pass_started = Instant::now();
        let mut phase = Instant::now();
        self.drain_herdr_events();
        log_slow_phase("herdr_events", &mut phase);
        match self.kelpie.begin_due_deliveries() {
            Ok(prepared) => {
                for prompt in prepared {
                    if !self.due_inflight.insert(prompt.operation_id) {
                        continue;
                    }
                    self.park_prompt(AwaitingPrompt {
                        request_id: String::new(),
                        stream: None,
                        prepared: prompt,
                        result_json: Value::Null,
                        reply_to: None,
                        lease: None,
                        intent_committed: false,
                        owner: PromptOwner::Internal,
                        reminder: None,
                    });
                }
            }
            Err(error) => {
                let _ = self
                    .kelpie
                    .store_mut()
                    .create_operator_notice(&format!("due fire failed: {error}"));
            }
        }
        log_slow_phase("due_deliveries", &mut phase);
        self.drive_parked_opens();
        log_slow_phase("due_opens", &mut phase);
        self.schedule_reminders();
        log_slow_phase("due_reminders", &mut phase);
        let renewed = self.park_due_renews();
        log_slow_phase("drive_renews", &mut phase);
        let renew_advanced = self.advance_awaiting_renews();
        log_slow_phase("awaiting_renews", &mut phase);
        self.drive_parked_renews();
        log_slow_phase("renew_herdr", &mut phase);
        let start_advanced = self.advance_awaiting_starts();
        log_slow_phase("awaiting_starts", &mut phase);
        let clear_advanced = self.advance_awaiting_clears();
        log_slow_phase("awaiting_clears", &mut phase);
        self.drive_parked_clears();
        log_slow_phase("clear_herdr", &mut phase);
        let inbox_advanced = self.advance_inboxes();
        log_slow_phase("inboxes", &mut phase);
        let reading_advanced = self.pump_reading();
        log_slow_phase("reading", &mut phase);
        let accepted = self.accept_waiting()?;
        log_slow_phase("accept", &mut phase);
        let reading_after_accept = self.pump_reading();
        log_slow_phase("reading_after_accept", &mut phase);
        if pass_started.elapsed() >= SLOW_POLL {
            eprintln!(
                "kelpied: slow poll {}ms starts={} clears={} reading={}",
                pass_started.elapsed().as_millis(),
                self.awaiting_starts.len(),
                self.awaiting_clears.len(),
                self.reading.len(),
            );
        }
        let advanced = start_advanced
            || clear_advanced
            || inbox_advanced
            || reading_advanced
            || reading_after_accept
            || renewed
            || renew_advanced;
        Ok(advanced
            || accepted
            || !self.awaiting_starts.is_empty()
            || !self.awaiting_clears.is_empty()
            || !self.reading.is_empty()
            || !self.awaiting_prompts.is_empty()
            || !self.awaiting_cancels.is_empty()
            || self.reminder_job.is_some()
            || !self.awaiting_reads.is_empty()
            || !self.awaiting_renews.is_empty()
            || !self.awaiting_adopts.is_empty()
            || !self.awaiting_renames.is_empty()
            || !self.awaiting_retires.is_empty())
    }

    fn accept_waiting(&mut self) -> Result<bool, DaemonError> {
        self.listener.set_nonblocking(true)?;
        let mut accepted = false;
        for _ in 0..MAX_ACCEPTS_PER_POLL {
            match self.listener.accept() {
                Ok((stream, _)) => {
                    stream.set_nonblocking(true)?;
                    self.reading.push(ReadingClient {
                        stream,
                        buf: Vec::new(),
                        accepted_at: Instant::now(),
                    });
                    accepted = true;
                }
                Err(error)
                    if error.kind() == std::io::ErrorKind::WouldBlock
                        || error.kind() == std::io::ErrorKind::Interrupted =>
                {
                    break;
                }
                Err(error) => return Err(error.into()),
            }
        }
        Ok(accepted)
    }

    fn pump_reading(&mut self) -> bool {
        let mut progressed = false;
        let mut still = Vec::with_capacity(self.reading.len());
        for mut client in std::mem::take(&mut self.reading) {
            if client.accepted_at.elapsed() >= CLIENT_REQUEST_DEADLINE {
                eprintln!("kelpied: client request timed out before a complete line");
                progressed = true;
                continue;
            }
            match read_into_buf(&mut client.stream, &mut client.buf) {
                Ok(ReadProgress::WouldBlock) => still.push(client),
                Ok(ReadProgress::Eof) => {
                    if !client.buf.is_empty() {
                        eprintln!("kelpied: client hung up mid-request");
                    }
                    progressed = true;
                }
                Ok(ReadProgress::Got) => {
                    if client.buf.len() > MAX_REQUEST_BYTES {
                        eprintln!("kelpied: client request exceeded {MAX_REQUEST_BYTES} bytes");
                        progressed = true;
                        continue;
                    }
                    if let Some(split) = client.buf.iter().position(|byte| *byte == b'\n') {
                        let mut line: Vec<u8> = client.buf.drain(..=split).collect();
                        line.pop();
                        if line.last() == Some(&b'\r') {
                            line.pop();
                        }
                        let leftover = std::mem::take(&mut client.buf);
                        progressed = true;
                        self.dispatch_line(client.stream, &line, leftover);
                    } else {
                        still.push(client);
                    }
                }
                Err(error) => {
                    eprintln!("kelpied: client connection failed: {error}");
                    progressed = true;
                }
            }
        }
        self.reading = still;
        progressed
    }

    fn dispatch_line(&mut self, stream: UnixStream, line: &[u8], leftover: Vec<u8>) {
        match serve_request(stream, line, leftover, &mut self.kelpie) {
            Ok(Served::Answered) => {}
            Ok(Served::AwaitingStart(awaiting)) => self.park_start(*awaiting),
            Ok(Served::AwaitingClear(awaiting)) => self.awaiting_clears.push(*awaiting),
            Ok(Served::Inbox(session)) => self.park_inbox(*session),
            Ok(Served::AwaitingPrompt(awaiting)) => self.park_prompt(*awaiting),
            Ok(Served::AwaitingCancel(pending)) => self.park_cancel(*pending),
            Ok(Served::AwaitingWaiterRetire(pending)) => {
                self.park_waiter_retire(*pending);
            }
            Ok(Served::AwaitingRead(read)) => self.park_read(*read),
            Ok(Served::AwaitingAdopt(adopt)) => self.park_adopt(*adopt),
            Ok(Served::AwaitingRename(rename)) => self.park_rename(*rename),
            Ok(Served::AwaitingRetire(retire)) => self.park_retire(*retire),
            Err(error) => {
                eprintln!("kelpied: client connection failed: {error}");
            }
        }
    }

    fn park_start(&mut self, mut awaiting: AwaitingStart) {
        let job_id = self.alloc_job();
        self.submit_owned(
            HerdrJob::Snapshot {
                job_id,
                negotiate: awaiting.declared.is_none(),
            },
            HerdrOwner::Start,
        );
        awaiting.herdr_job = Some(job_id);
        awaiting.phase = StartPhase::Snapshot;
        self.awaiting_starts.push(awaiting);
    }

    fn start_index_for_job(&self, job_id: u64) -> Option<usize> {
        self.awaiting_starts
            .iter()
            .position(|start| start.herdr_job == Some(job_id))
    }

    fn park_prompt(&mut self, awaiting: AwaitingPrompt) {
        let job_id = self.alloc_job();
        self.submit_owned(
            HerdrJob::Open {
                job_id,
                pane_id: awaiting.prepared.pane_id.clone(),
                negotiate: false,
            },
            HerdrOwner::Prompt,
        );
        self.awaiting_prompts.insert(job_id, awaiting);
    }

    fn park_read(&mut self, read: AwaitingRead) {
        let job_id = self.alloc_job();
        let job = match &read.kind {
            ClientReadKind::Report { .. } => HerdrJob::LifecycleSnapshot { job_id },
            ClientReadKind::Recover
            | ClientReadKind::Whoami { .. }
            | ClientReadKind::Alias { .. }
            | ClientReadKind::Attribution { .. }
            | ClientReadKind::WhoAttribution { .. }
            | ClientReadKind::WhoPane { .. } => HerdrJob::Snapshot {
                job_id,
                negotiate: true,
            },
        };
        self.submit_owned(job, HerdrOwner::ClientRead);
        self.awaiting_reads.insert(job_id, read);
    }

    fn park_retire(&mut self, mut retire: AwaitingRetireClose) {
        let RetireParkState::Snapshot(_) = &retire.state else {
            return;
        };
        let job_id = self.alloc_job();
        self.submit_owned(
            HerdrJob::Snapshot {
                job_id,
                negotiate: true,
            },
            HerdrOwner::Retire,
        );
        retire.herdr_job = Some(job_id);
        self.awaiting_retires.push(retire);
    }

    fn retire_index_for_job(&self, job_id: u64) -> Option<usize> {
        self.awaiting_retires
            .iter()
            .position(|item| item.herdr_job == Some(job_id))
    }

    fn on_retire_opened(&mut self, job_id: u64, lease: std::sync::mpsc::Sender<LeaseCmd>) {
        let Some(index) = self.retire_index_for_job(job_id) else {
            let _ = lease.send(LeaseCmd::Drop);
            return;
        };
        let RetireParkState::Closing(work) = self.awaiting_retires[index].state.clone() else {
            let _ = lease.send(LeaseCmd::Drop);
            return;
        };
        crate::test_fault::pause("retire_after_intent_before_close");
        let send = LeaseCmd::Send {
            request_id: work.request.clone(),
            method: "pane.close".into(),
            params: serde_json::json!({"pane_id": work.pane}),
            after_write_pause: "retire_after_write_before_response",
        };
        if lease.send(send).is_err() {
            self.fail_retire_at(
                index,
                SliceError::Herdr(HerdrError::Unexpected(
                    "herdr lease closed before retire close".into(),
                )),
            );
        } else {
            self.awaiting_retires[index].lease = Some(lease);
        }
    }

    fn on_retire_done(&mut self, job_id: u64, result: Result<HerdrJobResult, HerdrError>) {
        let Some(index) = self.retire_index_for_job(job_id) else {
            return;
        };
        match self.awaiting_retires[index].state.clone() {
            RetireParkState::Snapshot(preflight) => {
                let snapshot = match result {
                    Ok(HerdrJobResult::Snapshot(snapshot)) => snapshot,
                    Ok(_) => {
                        return self.fail_retire_at(
                            index,
                            SliceError::Herdr(HerdrError::Unexpected(
                                "retire snapshot returned wrong result".into(),
                            )),
                        );
                    }
                    Err(error) => return self.fail_retire_at(index, SliceError::Herdr(error)),
                };
                match self.kelpie.retire_after_snapshot(&preflight, &snapshot) {
                    Ok(RetireAfterSnapshot::Done { released }) => {
                        self.answer_retire_at(index, Ok((preflight.operation_id, released)));
                    }
                    Ok(RetireAfterSnapshot::Close(work)) => {
                        let next = self.alloc_job();
                        self.submit_owned(
                            HerdrJob::Open {
                                job_id: next,
                                pane_id: work.pane.clone(),
                                negotiate: false,
                            },
                            HerdrOwner::Retire,
                        );
                        self.awaiting_retires[index].state = RetireParkState::Closing(work);
                        self.awaiting_retires[index].herdr_job = Some(next);
                    }
                    Err(error) => self.fail_retire_at(index, error),
                }
            }
            RetireParkState::Closing(work) => {
                drop_lease(self.awaiting_retires[index].lease.take());
                match result {
                    Ok(HerdrJobResult::Closed) => {
                        crate::test_fault::pause("retire_after_close_before_commit");
                        let next = self.alloc_job();
                        self.submit_owned(
                            HerdrJob::Snapshot {
                                job_id: next,
                                negotiate: false,
                            },
                            HerdrOwner::Retire,
                        );
                        self.awaiting_retires[index].state = RetireParkState::Confirm(work);
                        self.awaiting_retires[index].herdr_job = Some(next);
                    }
                    Ok(_) => self.fail_retire_at(
                        index,
                        SliceError::Herdr(HerdrError::Unexpected(
                            "retire close returned wrong result".into(),
                        )),
                    ),
                    Err(error) => {
                        let error = Kelpie::retire_close_error(&work, error);
                        self.fail_retire_at(index, error);
                    }
                }
            }
            RetireParkState::Confirm(work) => {
                let result = match result {
                    Ok(HerdrJobResult::Snapshot(snapshot)) => {
                        self.kelpie.complete_retire_confirm(&work, &snapshot)
                    }
                    Ok(_) => Err(SliceError::Herdr(HerdrError::Unexpected(
                        "retire confirmation returned wrong result".into(),
                    ))),
                    Err(source) => Err(Kelpie::retire_close_error(&work, source)),
                };
                self.answer_retire_at(index, result);
            }
        }
    }

    fn on_retire_failed(&mut self, job_id: u64, phase: FailPhase, source: HerdrError) {
        let Some(index) = self.retire_index_for_job(job_id) else {
            return;
        };
        let error = match self.awaiting_retires[index].state.clone() {
            RetireParkState::Closing(work)
                if matches!(phase, FailPhase::Read | FailPhase::Lease) =>
            {
                Kelpie::retire_close_error(&work, source)
            }
            RetireParkState::Snapshot(_)
            | RetireParkState::Closing(_)
            | RetireParkState::Confirm(_) => SliceError::Herdr(source),
        };
        self.fail_retire_at(index, error);
    }

    fn answer_retire_at(&mut self, index: usize, result: Result<(OperationId, bool), SliceError>) {
        let mut retire = self.awaiting_retires.remove(index);
        drop_lease(retire.lease.take());
        let result = result.map(|(operation_id, released)| {
            serde_json::json!({
                "operation_id": operation_id,
                "pane_released": released
            })
        });
        let response = respond(&retire.request_id, result);
        if let Err(error) = write_response(&mut retire.stream, &response) {
            eprintln!("kelpied: parked retire response failed: {error}");
        }
    }

    fn fail_retire_at(&mut self, index: usize, error: SliceError) {
        self.answer_retire_at(index, Err(error));
    }

    fn park_rename(&mut self, mut rename: AwaitingRename) {
        let RenameParkState::Snapshot(_) = &rename.state else {
            return;
        };
        let job_id = self.alloc_job();
        self.submit_owned(
            HerdrJob::Snapshot {
                job_id,
                negotiate: true,
            },
            HerdrOwner::Rename,
        );
        rename.herdr_job = Some(job_id);
        self.awaiting_renames.push(rename);
    }

    fn rename_index_for_job(&self, job_id: u64) -> Option<usize> {
        self.awaiting_renames
            .iter()
            .position(|item| item.herdr_job == Some(job_id))
    }

    fn on_rename_opened(&mut self, job_id: u64, lease: std::sync::mpsc::Sender<LeaseCmd>) {
        let Some(index) = self.rename_index_for_job(job_id) else {
            let _ = lease.send(LeaseCmd::Drop);
            return;
        };
        let RenameParkState::Opening(preflight) = self.awaiting_renames[index].state.clone() else {
            let _ = lease.send(LeaseCmd::Drop);
            return;
        };
        match self.kelpie.commit_rename_intent(&preflight) {
            Ok(work) => {
                let send = LeaseCmd::Send {
                    request_id: work.request_id.clone(),
                    method: "agent.rename".into(),
                    params: serde_json::json!({"target": work.pane_id, "name": work.new_name}),
                    after_write_pause: "",
                };
                if lease.send(send).is_err() {
                    let source = HerdrError::Unexpected("herdr lease closed before rename".into());
                    let error = self
                        .kelpie
                        .apply_rename_write_error(&work, source)
                        .expect_err("rename write error must fail");
                    self.fail_rename_at(index, error);
                } else {
                    self.awaiting_renames[index].state = RenameParkState::Sending(work);
                    self.awaiting_renames[index].lease = Some(lease);
                }
            }
            Err(error) => {
                let _ = lease.send(LeaseCmd::Drop);
                self.fail_rename_at(index, error);
            }
        }
    }

    fn on_rename_done(&mut self, job_id: u64, result: Result<HerdrJobResult, HerdrError>) {
        let Some(index) = self.rename_index_for_job(job_id) else {
            return;
        };
        match self.awaiting_renames[index].state.clone() {
            RenameParkState::Snapshot(preflight) => {
                let snapshot = match result {
                    Ok(HerdrJobResult::Snapshot(snapshot)) => snapshot,
                    Ok(_) => {
                        return self.fail_rename_at(
                            index,
                            SliceError::Herdr(HerdrError::Unexpected(
                                "rename snapshot returned wrong result".into(),
                            )),
                        );
                    }
                    Err(error) => return self.fail_rename_at(index, SliceError::Herdr(error)),
                };
                match Kelpie::verify_rename_after_snapshot(&preflight, &snapshot) {
                    Ok(()) => {
                        let next = self.alloc_job();
                        self.submit_owned(
                            HerdrJob::Open {
                                job_id: next,
                                pane_id: preflight.pane_id.clone(),
                                negotiate: false,
                            },
                            HerdrOwner::Rename,
                        );
                        self.awaiting_renames[index].state = RenameParkState::Opening(preflight);
                        self.awaiting_renames[index].herdr_job = Some(next);
                    }
                    Err(error) => self.fail_rename_at(index, error),
                }
            }
            RenameParkState::Opening(_) => self.fail_rename_at(
                index,
                SliceError::Herdr(HerdrError::Unexpected(
                    "rename lease completed before send".into(),
                )),
            ),
            RenameParkState::Sending(work) => {
                drop_lease(self.awaiting_renames[index].lease.take());
                match result {
                    Ok(HerdrJobResult::Agent(_)) => {
                        let next = self.alloc_job();
                        self.submit_owned(
                            HerdrJob::Snapshot {
                                job_id: next,
                                negotiate: false,
                            },
                            HerdrOwner::Rename,
                        );
                        self.awaiting_renames[index].state = RenameParkState::Confirm(work);
                        self.awaiting_renames[index].herdr_job = Some(next);
                    }
                    Ok(_) => self.fail_rename_at(
                        index,
                        SliceError::Herdr(HerdrError::Unexpected(
                            "rename returned wrong result".into(),
                        )),
                    ),
                    Err(error) => {
                        let error = self
                            .kelpie
                            .apply_rename_write_error(&work, error)
                            .expect_err("rename write error must fail");
                        self.fail_rename_at(index, error);
                    }
                }
            }
            RenameParkState::Confirm(work) => {
                let result = match result {
                    Ok(HerdrJobResult::Snapshot(snapshot)) => {
                        self.kelpie.commit_rename_confirm(&work, &snapshot)
                    }
                    Ok(_) => Err(SliceError::Herdr(HerdrError::Unexpected(
                        "rename confirmation returned wrong result".into(),
                    ))),
                    Err(source) => self.kelpie.apply_rename_write_error(&work, source),
                };
                self.answer_rename_at(index, result);
            }
        }
    }

    fn on_rename_failed(&mut self, job_id: u64, phase: FailPhase, source: HerdrError) {
        let Some(index) = self.rename_index_for_job(job_id) else {
            return;
        };
        let error = match self.awaiting_renames[index].state.clone() {
            RenameParkState::Sending(work)
                if matches!(phase, FailPhase::Read | FailPhase::Lease) =>
            {
                self.kelpie
                    .apply_rename_write_error(&work, source)
                    .expect_err("rename write error must fail")
            }
            RenameParkState::Sending(work) => self
                .kelpie
                .abandon_rename_before_write(&work, source)
                .expect_err("rename pre-write error must fail"),
            RenameParkState::Snapshot(_)
            | RenameParkState::Opening(_)
            | RenameParkState::Confirm(_) => SliceError::Herdr(source),
        };
        self.fail_rename_at(index, error);
    }

    fn answer_rename_at(
        &mut self,
        index: usize,
        result: Result<crate::store::ReadyIdentity, SliceError>,
    ) {
        let mut rename = self.awaiting_renames.remove(index);
        drop_lease(rename.lease.take());
        let result = result.map(|identity| {
            serde_json::json!({
                "logical_agent_id": identity.logical_agent_id,
                "incarnation_id": identity.incarnation_id,
                "public_name": identity.public_name
            })
        });
        let response = respond(&rename.request_id, result);
        if let Err(error) = write_response(&mut rename.stream, &response) {
            eprintln!("kelpied: parked rename response failed: {error}");
        }
    }

    fn fail_rename_at(&mut self, index: usize, error: SliceError) {
        self.answer_rename_at(index, Err(error));
    }

    fn park_adopt(&mut self, mut adopt: AwaitingAdopt) {
        let job_id = self.alloc_job();
        self.submit_owned(
            HerdrJob::Snapshot {
                job_id,
                negotiate: true,
            },
            HerdrOwner::Adopt,
        );
        adopt.herdr_job = Some(job_id);
        self.awaiting_adopts.push(adopt);
    }

    fn adopt_index_for_job(&self, job_id: u64) -> Option<usize> {
        self.awaiting_adopts
            .iter()
            .position(|adopt| adopt.herdr_job == Some(job_id))
    }

    fn on_adopt_opened(&mut self, job_id: u64, lease: std::sync::mpsc::Sender<LeaseCmd>) {
        let Some(index) = self.adopt_index_for_job(job_id) else {
            let _ = lease.send(LeaseCmd::Drop);
            return;
        };
        let AdoptParkState::Renaming { work, .. } = self.awaiting_adopts[index].state.clone()
        else {
            let _ = lease.send(LeaseCmd::Drop);
            return;
        };
        let pane_id = work.pane_id;
        let name = work.evidence.public_name;
        let declared = work.declared;
        match self.kelpie.submit_adopt_rename_intent(declared) {
            Ok(intent_id) => {
                crate::test_fault::pause("adopt_rename_after_submitted_before_write");
                let send = LeaseCmd::Send {
                    request_id: intent_id.clone(),
                    method: "agent.rename".into(),
                    params: serde_json::json!({ "target": pane_id, "name": name }),
                    after_write_pause: "adopt_rename_after_write_before_response",
                };
                if lease.send(send).is_err() {
                    let source =
                        HerdrError::Unexpected("herdr lease closed before adopt rename".into());
                    let error = self
                        .kelpie
                        .apply_adopt_rename_error(declared, source)
                        .expect_err("adopt rename error must fail");
                    self.fail_parked_adopt(job_id, error);
                } else {
                    if let AdoptParkState::Renaming { request_id, .. } =
                        &mut self.awaiting_adopts[index].state
                    {
                        *request_id = intent_id;
                    }
                    self.awaiting_adopts[index].lease = Some(lease);
                }
            }
            Err(error) => {
                let _ = lease.send(LeaseCmd::Drop);
                self.fail_parked_adopt(job_id, error);
            }
        }
    }

    fn on_adopt_done(&mut self, job_id: u64, result: Result<HerdrJobResult, HerdrError>) {
        let Some(index) = self.adopt_index_for_job(job_id) else {
            return;
        };
        match &self.awaiting_adopts[index].state {
            AdoptParkState::Snapshot(_) => self.on_adopt_snapshot(index, result),
            AdoptParkState::Renaming { .. } => self.on_adopt_renamed(index, result),
            AdoptParkState::Confirm(_) => self.on_adopt_confirm(index, result),
        }
    }

    fn on_adopt_snapshot(&mut self, index: usize, result: Result<HerdrJobResult, HerdrError>) {
        let snapshot = match result {
            Ok(HerdrJobResult::Snapshot(snapshot)) => snapshot,
            Ok(_) => {
                self.fail_adopt_at(
                    index,
                    SliceError::Herdr(HerdrError::Unexpected(
                        "adopt snapshot returned the wrong Herdr result".into(),
                    )),
                );
                return;
            }
            Err(error) => {
                self.fail_adopt_at(index, SliceError::Herdr(error));
                return;
            }
        };
        let AdoptParkState::Snapshot(intent) = self.awaiting_adopts[index].state.clone() else {
            return;
        };
        match self.kelpie.adopt_after_snapshot(&intent, &snapshot) {
            Ok(AdoptAfterSnapshot::Ready(created)) => self.answer_adopt_at(index, Ok(created)),
            Ok(AdoptAfterSnapshot::Rename(work)) => {
                let job_id = self.alloc_job();
                self.submit_owned(
                    HerdrJob::Open {
                        job_id,
                        pane_id: work.pane_id.clone(),
                        negotiate: false,
                    },
                    HerdrOwner::Adopt,
                );
                self.awaiting_adopts[index].herdr_job = Some(job_id);
                self.awaiting_adopts[index].state = AdoptParkState::Renaming {
                    work,
                    request_id: String::new(),
                };
            }
            Err(error) => self.fail_adopt_at(index, error),
        }
    }

    fn on_adopt_renamed(&mut self, index: usize, result: Result<HerdrJobResult, HerdrError>) {
        drop_lease(self.awaiting_adopts[index].lease.take());
        let AdoptParkState::Renaming { work, .. } = self.awaiting_adopts[index].state.clone()
        else {
            return;
        };
        match result {
            Ok(HerdrJobResult::Agent(_)) => {
                crate::test_fault::pause("adopt_rename_after_response_before_commit");
                let job_id = self.alloc_job();
                self.submit_owned(
                    HerdrJob::Snapshot {
                        job_id,
                        negotiate: false,
                    },
                    HerdrOwner::Adopt,
                );
                self.awaiting_adopts[index].herdr_job = Some(job_id);
                self.awaiting_adopts[index].state = AdoptParkState::Confirm(work);
            }
            Ok(_) => {
                let source =
                    HerdrError::Unexpected("adopt rename returned a non-agent result".into());
                let error = self
                    .kelpie
                    .apply_adopt_rename_error(work.declared, source)
                    .expect_err("adopt rename error must fail");
                self.fail_adopt_at(index, error);
            }
            Err(source) => {
                let error = self
                    .kelpie
                    .apply_adopt_rename_error(work.declared, source)
                    .expect_err("adopt rename error must fail");
                self.fail_adopt_at(index, error);
            }
        }
    }

    fn on_adopt_confirm(&mut self, index: usize, result: Result<HerdrJobResult, HerdrError>) {
        let snapshot = match result {
            Ok(HerdrJobResult::Snapshot(snapshot)) => snapshot,
            Ok(_) => {
                let AdoptParkState::Confirm(work) = self.awaiting_adopts[index].state.clone()
                else {
                    return;
                };
                let source =
                    HerdrError::Unexpected("adopt confirm returned a non-snapshot result".into());
                let error = self
                    .kelpie
                    .apply_adopt_confirm_error(work.declared, source)
                    .expect_err("adopt confirm error must fail");
                self.fail_adopt_at(index, error);
                return;
            }
            Err(source) => {
                let AdoptParkState::Confirm(work) = self.awaiting_adopts[index].state.clone()
                else {
                    return;
                };
                let error = self
                    .kelpie
                    .apply_adopt_confirm_error(work.declared, source)
                    .expect_err("adopt confirm error must fail");
                self.fail_adopt_at(index, error);
                return;
            }
        };
        let AdoptParkState::Confirm(work) = self.awaiting_adopts[index].state.clone() else {
            return;
        };
        match self.kelpie.accept_adopt_confirm(&work, &snapshot) {
            Ok(created) => {
                if let AdoptReply::Who { refresh } = self.awaiting_adopts[index].reply {
                    let reason = if refresh {
                        self.kelpie
                            .refresh_attribution_after_snapshot(created.incarnation_id, &snapshot)
                    } else {
                        Ok(None)
                    };
                    let result = reason.and_then(|reason| {
                        who_identity_response(
                            &self.kelpie,
                            WhoIdentity::Incarnation(created.incarnation_id),
                            reason,
                        )
                    });
                    self.answer_who_adopt_at(index, result);
                } else {
                    self.answer_adopt_at(index, Ok(created));
                }
            }
            Err(error) => self.fail_adopt_at(index, error),
        }
    }

    fn answer_who_adopt_at(&mut self, index: usize, result: Result<Value, SliceError>) {
        let mut adopt = self.awaiting_adopts.remove(index);
        drop_lease(adopt.lease.take());
        debug_assert!(adopt.resume.is_none());
        let response = respond(&adopt.request_id, result);
        if let Err(error) = write_response(&mut adopt.stream, &response) {
            eprintln!("kelpied: parked who response failed: {error}");
        }
    }

    fn answer_adopt_at(
        &mut self,
        index: usize,
        created: Result<crate::store::DeclaredStart, SliceError>,
    ) {
        let mut adopt = self.awaiting_adopts.remove(index);
        drop_lease(adopt.lease.take());
        if let Some(resume) = adopt.resume.take() {
            match created {
                Ok(_) => self.resume_bound_request(resume, adopt.stream),
                Err(error) => {
                    let response = respond(&adopt.request_id, Err(error));
                    if let Err(error) = write_response(&mut adopt.stream, &response) {
                        eprintln!("kelpied: parked adopt response failed: {error}");
                    }
                }
            }
            return;
        }
        let result = match (adopt.reply, created) {
            (AdoptReply::Rpc, Ok(created)) => Ok(serde_json::json!({
                "logical_agent_id": created.logical_agent_id,
                "incarnation_id": created.incarnation_id,
                "operation_id": created.operation_id,
                "outcome": "succeeded"
            })),
            (AdoptReply::Whoami, Ok(created)) => {
                let pane = match &adopt.state {
                    AdoptParkState::Snapshot(intent) => intent.pane_id.as_str(),
                    AdoptParkState::Renaming { work, .. } | AdoptParkState::Confirm(work) => {
                        work.pane_id.as_str()
                    }
                };
                Ok(self
                    .kelpie
                    .store()
                    .ready_identity_for_pane(pane)
                    .map_or_else(
                        |_| {
                            serde_json::json!({
                                "logical_agent_id": created.logical_agent_id,
                                "incarnation_id": created.incarnation_id
                            })
                        },
                        |identity| {
                            serde_json::json!({
                                "logical_agent_id": identity.logical_agent_id,
                                "incarnation_id": identity.incarnation_id,
                                "public_name": identity.public_name
                            })
                        },
                    ))
            }
            (AdoptReply::Who { .. }, Ok(_)) => Err(SliceError::Herdr(HerdrError::Unexpected(
                "who adoption completed without its confirming snapshot".into(),
            ))),
            (_, Err(error)) => Err(error),
        };
        let response = respond(&adopt.request_id, result);
        if let Err(error) = write_response(&mut adopt.stream, &response) {
            eprintln!("kelpied: parked adopt response failed: {error}");
        }
    }

    fn fail_adopt_at(&mut self, index: usize, error: SliceError) {
        self.answer_adopt_at(index, Err(error));
    }

    fn fail_parked_adopt(&mut self, job_id: u64, error: SliceError) {
        if let Some(index) = self.adopt_index_for_job(job_id) {
            self.fail_adopt_at(index, error);
        }
    }

    fn on_adopt_failed(&mut self, job_id: u64, source: HerdrError) {
        let Some(index) = self.adopt_index_for_job(job_id) else {
            return;
        };
        let error = match self.awaiting_adopts[index].state.clone() {
            AdoptParkState::Renaming { work, request_id } if !request_id.is_empty() => self
                .kelpie
                .apply_adopt_rename_error(work.declared, source)
                .expect_err("adopt rename error must fail"),
            AdoptParkState::Snapshot(_) | AdoptParkState::Renaming { .. } => {
                SliceError::Herdr(source)
            }
            AdoptParkState::Confirm(work) => self
                .kelpie
                .apply_adopt_confirm_error(work.declared, source)
                .expect_err("adopt confirm error must fail"),
        };
        self.fail_adopt_at(index, error);
    }

    fn on_client_read_done(&mut self, job_id: u64, result: Result<HerdrJobResult, HerdrError>) {
        let Some(mut read) = self.awaiting_reads.remove(&job_id) else {
            return;
        };
        match (read.kind.clone(), result) {
            (ClientReadKind::Recover, Ok(HerdrJobResult::Snapshot(snapshot))) => {
                let response = respond(
                    &read.request_id,
                    self.kelpie
                        .recover_with_snapshot(&snapshot)
                        .map(recover_result),
                );
                if let Err(error) = write_response(&mut read.stream, &response) {
                    eprintln!("kelpied: parked read response failed: {error}");
                }
            }
            (ClientReadKind::Report { active }, Ok(HerdrJobResult::Lifecycle(observations))) => {
                let live = LiveStatus::from_observations(observations);
                let response = respond(
                    &read.request_id,
                    render_report(&self.kelpie, active, Some(&live)),
                );
                if let Err(error) = write_response(&mut read.stream, &response) {
                    eprintln!("kelpied: parked read response failed: {error}");
                }
            }
            (
                ClientReadKind::Whoami { pane_id, lazy_key },
                Ok(HerdrJobResult::Snapshot(snapshot)),
            ) => self.finish_whoami_snapshot(read, &pane_id, &lazy_key, &snapshot),
            (
                ClientReadKind::Alias {
                    alias,
                    lazy_key,
                    resume,
                },
                Ok(HerdrJobResult::Snapshot(snapshot)),
            ) => self.finish_alias_snapshot(read, &alias, &lazy_key, resume, &snapshot),
            (
                ClientReadKind::Attribution { incarnation_id },
                Ok(HerdrJobResult::Snapshot(snapshot)),
            ) => {
                let result = self
                    .kelpie
                    .refresh_attribution_after_snapshot(incarnation_id, &snapshot)
                    .and_then(|reason| attribution_response(&self.kelpie, incarnation_id, reason));
                let response = respond(&read.request_id, result);
                if let Err(error) = write_response(&mut read.stream, &response) {
                    eprintln!("kelpied: parked attribution response failed: {error}");
                }
            }
            (
                ClientReadKind::WhoAttribution { incarnation_id },
                Ok(HerdrJobResult::Snapshot(snapshot)),
            ) => {
                let result = self
                    .kelpie
                    .refresh_attribution_after_snapshot(incarnation_id, &snapshot)
                    .and_then(|reason| {
                        who_identity_response(
                            &self.kelpie,
                            WhoIdentity::Incarnation(incarnation_id),
                            reason,
                        )
                    });
                let response = respond(&read.request_id, result);
                if let Err(error) = write_response(&mut read.stream, &response) {
                    eprintln!("kelpied: parked who response failed: {error}");
                }
            }
            (
                ClientReadKind::WhoPane {
                    pane_id,
                    lazy_key,
                    refresh,
                },
                Ok(HerdrJobResult::Snapshot(snapshot)),
            ) => self.finish_who_snapshot(read, &pane_id, &lazy_key, refresh, &snapshot),
            (_, Ok(_)) => {
                let response = respond(
                    &read.request_id,
                    Err(SliceError::Herdr(HerdrError::Unexpected(
                        "client read returned the wrong Herdr result".into(),
                    ))),
                );
                if let Err(error) = write_response(&mut read.stream, &response) {
                    eprintln!("kelpied: parked read response failed: {error}");
                }
            }
            (_, Err(error)) => {
                let response = respond(&read.request_id, Err(SliceError::Herdr(error)));
                if let Err(error) = write_response(&mut read.stream, &response) {
                    eprintln!("kelpied: parked read response failed: {error}");
                }
            }
        }
    }

    fn finish_whoami_snapshot(
        &mut self,
        mut read: AwaitingRead,
        pane_id: &str,
        lazy_key: &str,
        snapshot: &crate::herdr::Snapshot,
    ) {
        match self
            .kelpie
            .pane_adopt_after_snapshot(pane_id, lazy_key, snapshot)
        {
            Ok(AdoptAfterSnapshot::Ready(_)) => {
                let result = self
                    .kelpie
                    .store()
                    .ready_identity_for_pane(pane_id)
                    .map(|identity| {
                        serde_json::json!({
                            "logical_agent_id": identity.logical_agent_id,
                            "incarnation_id": identity.incarnation_id,
                            "public_name": identity.public_name
                        })
                    })
                    .map_err(SliceError::Store);
                let response = respond(&read.request_id, result);
                if let Err(error) = write_response(&mut read.stream, &response) {
                    eprintln!("kelpied: parked read response failed: {error}");
                }
            }
            Ok(AdoptAfterSnapshot::Rename(work)) => {
                let job_id = self.alloc_job();
                self.submit_owned(
                    HerdrJob::Open {
                        job_id,
                        pane_id: work.pane_id.clone(),
                        negotiate: false,
                    },
                    HerdrOwner::Adopt,
                );
                self.awaiting_adopts.push(AwaitingAdopt {
                    request_id: read.request_id,
                    stream: read.stream,
                    reply: AdoptReply::Whoami,
                    resume: None,
                    state: AdoptParkState::Renaming {
                        work,
                        request_id: String::new(),
                    },
                    herdr_job: Some(job_id),
                    lease: None,
                });
            }
            Err(error) => {
                let response = respond(&read.request_id, Err(error));
                if let Err(error) = write_response(&mut read.stream, &response) {
                    eprintln!("kelpied: parked read response failed: {error}");
                }
            }
        }
    }

    fn finish_who_snapshot(
        &mut self,
        mut read: AwaitingRead,
        pane_id: &str,
        lazy_key: &str,
        refresh: bool,
        snapshot: &crate::herdr::Snapshot,
    ) {
        match self
            .kelpie
            .pane_adopt_after_snapshot(pane_id, lazy_key, snapshot)
        {
            Ok(AdoptAfterSnapshot::Ready(created)) => {
                let reason = if refresh {
                    self.kelpie
                        .refresh_attribution_after_snapshot(created.incarnation_id, snapshot)
                } else {
                    Ok(None)
                };
                let result = reason.and_then(|reason| {
                    who_identity_response(
                        &self.kelpie,
                        WhoIdentity::Incarnation(created.incarnation_id),
                        reason,
                    )
                });
                let response = respond(&read.request_id, result);
                if let Err(error) = write_response(&mut read.stream, &response) {
                    eprintln!("kelpied: parked who response failed: {error}");
                }
            }
            Ok(AdoptAfterSnapshot::Rename(work)) => {
                let job_id = self.alloc_job();
                self.submit_owned(
                    HerdrJob::Open {
                        job_id,
                        pane_id: work.pane_id.clone(),
                        negotiate: false,
                    },
                    HerdrOwner::Adopt,
                );
                self.awaiting_adopts.push(AwaitingAdopt {
                    request_id: read.request_id,
                    stream: read.stream,
                    reply: AdoptReply::Who { refresh },
                    resume: None,
                    state: AdoptParkState::Renaming {
                        work,
                        request_id: String::new(),
                    },
                    herdr_job: Some(job_id),
                    lease: None,
                });
            }
            Err(error) => {
                let response = respond(&read.request_id, Err(error));
                if let Err(error) = write_response(&mut read.stream, &response) {
                    eprintln!("kelpied: parked who response failed: {error}");
                }
            }
        }
    }

    fn finish_alias_snapshot(
        &mut self,
        mut read: AwaitingRead,
        alias: &str,
        lazy_key: &str,
        resume: ClientRequest,
        snapshot: &crate::herdr::Snapshot,
    ) {
        match self.kelpie.alias_after_snapshot(alias, lazy_key, snapshot) {
            Ok(AdoptAfterSnapshot::Ready(_)) => self.resume_bound_request(resume, read.stream),
            Ok(AdoptAfterSnapshot::Rename(work)) => {
                let job_id = self.alloc_job();
                self.submit_owned(
                    HerdrJob::Open {
                        job_id,
                        pane_id: work.pane_id.clone(),
                        negotiate: false,
                    },
                    HerdrOwner::Adopt,
                );
                self.awaiting_adopts.push(AwaitingAdopt {
                    request_id: read.request_id,
                    stream: read.stream,
                    reply: AdoptReply::Rpc,
                    resume: Some(resume),
                    state: AdoptParkState::Renaming {
                        work,
                        request_id: String::new(),
                    },
                    herdr_job: Some(job_id),
                    lease: None,
                });
            }
            Err(error) => {
                let response = respond(&read.request_id, Err(error));
                if let Err(error) = write_response(&mut read.stream, &response) {
                    eprintln!("kelpied: parked read response failed: {error}");
                }
            }
        }
    }

    fn resume_bound_request(&mut self, request: ClientRequest, mut stream: UnixStream) {
        if request.method == "ask" || request.method == "tell" || request.method == "reply" {
            match prepare_client_prompt(&request, &mut self.kelpie) {
                Ok((result_json, None, _)) => {
                    let response = respond(&request.id, Ok(result_json));
                    if let Err(error) = write_response(&mut stream, &response) {
                        eprintln!("kelpied: parked alias response failed: {error}");
                    }
                }
                Ok((result_json, Some(prepared), reply_to)) => {
                    self.park_prompt(AwaitingPrompt {
                        request_id: request.id,
                        stream: Some(stream),
                        prepared,
                        result_json,
                        reply_to,
                        lease: None,
                        intent_committed: false,
                        owner: PromptOwner::Client,
                        reminder: None,
                    });
                }
                Err(error) => {
                    let response = respond(&request.id, Err(error));
                    if let Err(error) = write_response(&mut stream, &response) {
                        eprintln!("kelpied: parked alias response failed: {error}");
                    }
                }
            }
            return;
        }
        if request.method == "clear" {
            match begin_clear(request.params, &mut self.kelpie) {
                Ok(ClearDispatch::Complete(cleared)) => {
                    let response = respond(&request.id, Ok(clear_result(cleared)));
                    if let Err(error) = write_response(&mut stream, &response) {
                        eprintln!("kelpied: parked alias response failed: {error}");
                    }
                }
                Ok(ClearDispatch::Awaiting(state)) => {
                    self.awaiting_clears.push(AwaitingClearRequest {
                        request_id: request.id,
                        state,
                        stream,
                        herdr_job: None,
                        lease: None,
                    });
                }
                Err(error) => {
                    let response = respond(&request.id, Err(error));
                    if let Err(error) = write_response(&mut stream, &response) {
                        eprintln!("kelpied: parked alias response failed: {error}");
                    }
                }
            }
            return;
        }
        let response = respond(
            &request.id,
            Err(SliceError::Store(StoreError::InvalidRecord(
                "cannot resume this method after alias bind".into(),
            ))),
        );
        if let Err(error) = write_response(&mut stream, &response) {
            eprintln!("kelpied: parked alias response failed: {error}");
        }
    }

    fn fail_client_read(&mut self, job_id: u64, error: SliceError) {
        let Some(mut read) = self.awaiting_reads.remove(&job_id) else {
            return;
        };
        let response = respond(&read.request_id, Err(error));
        if let Err(error) = write_response(&mut read.stream, &response) {
            eprintln!("kelpied: parked read response failed: {error}");
        }
    }

    fn park_cancel(&mut self, pending: PendingCancel) {
        let session = self.next_cancel_session;
        self.next_cancel_session = self.next_cancel_session.saturating_add(1);
        let remaining = u32::try_from(pending.prompts.len()).unwrap_or(u32::MAX);
        self.awaiting_cancels.insert(
            session,
            AwaitingCancel {
                request_id: pending.request_id,
                stream: pending.stream,
                outcome: pending.outcome,
                remaining,
            },
        );
        for prompt in pending.prompts {
            self.park_prompt(AwaitingPrompt {
                request_id: String::new(),
                stream: None,
                prepared: prompt.prepared,
                result_json: Value::Null,
                reply_to: None,
                lease: None,
                intent_committed: false,
                owner: PromptOwner::Cancel {
                    session,
                    waiting: prompt.waiting,
                },
                reminder: None,
            });
        }
    }

    fn park_waiter_retire(&mut self, pending: PendingWaiterRetire) {
        let session = self.next_waiter_retire_session;
        self.next_waiter_retire_session = self.next_waiter_retire_session.saturating_add(1);
        let remaining = u32::try_from(
            pending
                .prepared
                .owing_notices
                .iter()
                .filter(|notice| notice.prepared.is_some())
                .count(),
        )
        .unwrap_or(u32::MAX);
        let outcome = WaiterRetireOutcome {
            cancelled_ask_ids: pending.prepared.cancelled_ask_ids,
            owing_notices: pending
                .prepared
                .owing_notices
                .iter()
                .map(|notice| WaiterRetireOwingNotice {
                    ask_message_id: notice.ask_message_id,
                    message_id: notice.message_id,
                    delivered: false,
                })
                .collect(),
        };
        if remaining == 0 {
            let mut stream = pending.stream;
            let response = respond(
                &pending.request_id,
                Ok(waiter_retire_result(pending.logical_agent_id, &outcome)),
            );
            if let Err(error) = write_response(&mut stream, &response) {
                eprintln!("kelpied: parked waiter-retire response failed: {error}");
            }
            return;
        }
        self.awaiting_waiter_retires.insert(
            session,
            AwaitingWaiterRetire {
                request_id: pending.request_id,
                stream: pending.stream,
                logical_agent_id: pending.logical_agent_id,
                outcome,
                remaining,
            },
        );
        for notice in pending.prepared.owing_notices {
            let Some(prepared) = notice.prepared else {
                continue;
            };
            self.park_prompt(AwaitingPrompt {
                request_id: String::new(),
                stream: None,
                prepared,
                result_json: Value::Null,
                reply_to: None,
                lease: None,
                intent_committed: false,
                owner: PromptOwner::WaiterRetire {
                    session,
                    ask: notice.ask_message_id,
                },
                reminder: None,
            });
        }
    }

    /// Issue `Send` on any Open that is already connected so a sequential fake
    /// Herdr is not left blocked on a silent lease.
    fn drive_parked_clears(&mut self) {
        let deadline = Instant::now() + Duration::from_millis(50);
        while Instant::now() < deadline {
            self.drain_herdr_events();
            let busy = self.awaiting_clears.iter().any(|clear| {
                clear.herdr_job.is_some()
                    || matches!(
                        clear.state,
                        AwaitingClearState::Probe { .. } | AwaitingClearState::Sending { .. }
                    )
            });
            if !busy {
                return;
            }
            thread::sleep(Duration::from_millis(1));
        }
    }

    fn drive_parked_renews(&mut self) {
        let deadline = Instant::now() + Duration::from_millis(50);
        while Instant::now() < deadline {
            self.drain_herdr_events();
            let busy = self.awaiting_renews.iter().any(|renew| {
                renew.herdr_job.is_some()
                    || matches!(
                        renew.state,
                        RenewParkState::Probe
                            | RenewParkState::ClearingSend(_)
                            | RenewParkState::RotationGet
                            | RenewParkState::InjectSend(_)
                            | RenewParkState::ConfirmGet
                    )
            });
            if !busy {
                return;
            }
            thread::sleep(Duration::from_millis(1));
        }
    }

    #[allow(clippy::too_many_lines)]
    fn park_due_renews(&mut self) -> bool {
        if let Err(error) = self.kelpie.terminate_ended_renews() {
            let _ = self
                .kelpie
                .store_mut()
                .create_operator_notice(&format!("renew drive failed: {error}"));
            return false;
        }
        let now_ms = crate::store::store_clock_ms().unwrap_or(0);
        let actionable = match self.kelpie.store().actionable_renews(now_ms) {
            Ok(items) => items,
            Err(error) => {
                let _ = self
                    .kelpie
                    .store_mut()
                    .create_operator_notice(&format!("renew drive failed: {error}"));
                return false;
            }
        };
        let mut progressed = false;
        for item in actionable {
            if self.renew_inflight.contains(&item.renew_id) {
                continue;
            }
            match item.phase {
                RenewPhase::Scheduled => {
                    match self
                        .kelpie
                        .prompt_spacing_active(item.incarnation_id, now_ms)
                    {
                        Ok(true) => continue,
                        Ok(false) => {}
                        Err(error) => {
                            self.note_renew_drive_error(&error);
                            continue;
                        }
                    }
                    match self.kelpie.begin_renew_prepare(&item, now_ms) {
                        Ok(prepared) => {
                            self.park_prompt(AwaitingPrompt {
                                request_id: String::new(),
                                stream: None,
                                prepared,
                                result_json: Value::Null,
                                reply_to: None,
                                lease: None,
                                intent_committed: false,
                                owner: PromptOwner::Internal,
                                reminder: None,
                            });
                            progressed = true;
                        }
                        Err(error) if Self::renew_drive_skips(&error) => {}
                        Err(error) => {
                            self.note_renew_drive_error(&error);
                            return progressed;
                        }
                    }
                }
                RenewPhase::Preparing | RenewPhase::TimedOut => {
                    match self.kelpie.advance_renew(&item, now_ms) {
                        Ok(moved) => progressed |= moved,
                        Err(error) if Self::renew_drive_skips(&error) => {}
                        Err(error) => {
                            self.note_renew_drive_error(&error);
                            return progressed;
                        }
                    }
                }
                RenewPhase::Ready => match self.kelpie.renew_clear_ready_to_probe(&item, now_ms) {
                    Ok(false) => {}
                    Ok(true) => {
                        self.park_renew(item, RenewParkState::Probe);
                        progressed = true;
                    }
                    Err(error) if Self::renew_drive_skips(&error) => {}
                    Err(error) => {
                        self.note_renew_drive_error(&error);
                        return progressed;
                    }
                },
                RenewPhase::Clearing => {
                    self.park_renew(item, RenewParkState::RotationGet);
                    progressed = true;
                }
                RenewPhase::Injected if item.inject_not_before_ms.is_none() => {
                    match self.kelpie.advance_renew(&item, now_ms) {
                        Ok(moved) => progressed |= moved,
                        Err(error) if Self::renew_drive_skips(&error) => {}
                        Err(error) => {
                            self.note_renew_drive_error(&error);
                            return progressed;
                        }
                    }
                }
                RenewPhase::Injected => {
                    self.park_renew(item, RenewParkState::ConfirmGet);
                    progressed = true;
                }
                RenewPhase::Done | RenewPhase::Aborted | RenewPhase::Terminated => {}
            }
        }
        progressed
    }

    fn renew_drive_skips(error: &SliceError) -> bool {
        matches!(
            error,
            SliceError::Herdr(_)
                | SliceError::UnknownOutcome { .. }
                | SliceError::UnsupportedBackend { .. }
                | SliceError::Store(StoreError::Conflict(_))
        )
    }

    fn note_renew_drive_error(&mut self, error: &SliceError) {
        let _ = self
            .kelpie
            .store_mut()
            .create_operator_notice(&format!("renew drive failed: {error}"));
    }

    fn park_renew(&mut self, item: DueRenew, state: RenewParkState) {
        self.renew_inflight.insert(item.renew_id);
        self.awaiting_renews.push(AwaitingRenew {
            item,
            state,
            herdr_job: None,
            lease: None,
        });
    }

    fn advance_awaiting_renews(&mut self) -> bool {
        let mut progressed = false;
        let mut idx = 0;
        while idx < self.awaiting_renews.len() {
            if self.awaiting_renews[idx].herdr_job.is_some() {
                idx += 1;
                continue;
            }
            match &self.awaiting_renews[idx].state {
                RenewParkState::Probe => {
                    self.submit_renew_get(
                        idx,
                        format!(
                            "kelpie:renew:probe:{}",
                            self.awaiting_renews[idx].item.renew_id
                        ),
                    );
                    progressed = true;
                    idx += 1;
                }
                RenewParkState::ClearingSend(write) => {
                    let pane_id = write.pane_id.clone();
                    self.submit_renew_open(idx, pane_id);
                    progressed = true;
                    idx += 1;
                }
                RenewParkState::RotationGet => {
                    self.submit_renew_get(
                        idx,
                        format!(
                            "kelpie:renew:rotation:{}",
                            self.awaiting_renews[idx].item.renew_id
                        ),
                    );
                    progressed = true;
                    idx += 1;
                }
                RenewParkState::InjectSend(write) => {
                    let pane_id = write.pane_id.clone();
                    self.submit_renew_open(idx, pane_id);
                    progressed = true;
                    idx += 1;
                }
                RenewParkState::ConfirmGet => {
                    self.submit_renew_get(
                        idx,
                        format!(
                            "kelpie:renew:confirm:{}",
                            self.awaiting_renews[idx].item.renew_id
                        ),
                    );
                    progressed = true;
                    idx += 1;
                }
            }
        }
        progressed
    }

    fn submit_renew_get(&mut self, index: usize, request_id: String) {
        let job_id = self.alloc_job();
        let target = self.awaiting_renews[index].item.pane_id.clone();
        self.submit_owned(
            HerdrJob::AgentGet {
                job_id,
                request_id,
                target,
            },
            HerdrOwner::Renew,
        );
        self.awaiting_renews[index].herdr_job = Some(job_id);
    }

    fn submit_renew_open(&mut self, index: usize, pane_id: String) {
        let job_id = self.alloc_job();
        self.submit_owned(
            HerdrJob::Open {
                job_id,
                pane_id,
                negotiate: false,
            },
            HerdrOwner::Renew,
        );
        self.awaiting_renews[index].herdr_job = Some(job_id);
    }

    fn on_renew_opened(&mut self, job_id: u64, lease: std::sync::mpsc::Sender<LeaseCmd>) {
        let Some(index) = self.renew_index_for_job(job_id) else {
            let _ = lease.send(LeaseCmd::Drop);
            return;
        };
        let (request_id, pane_id, text, pause) = match &self.awaiting_renews[index].state {
            RenewParkState::ClearingSend(write) => (
                write.request_id.clone(),
                write.pane_id.clone(),
                write.command.to_string(),
                "renew_after_ready_before_clear",
            ),
            RenewParkState::InjectSend(write) => (
                write.request_id.clone(),
                write.pane_id.clone(),
                write.envelope.clone(),
                "renew_after_clear_before_inject",
            ),
            _ => {
                let _ = lease.send(LeaseCmd::Drop);
                return;
            }
        };
        crate::test_fault::pause(pause);
        let send = LeaseCmd::Send {
            request_id,
            method: "agent.prompt".into(),
            params: serde_json::json!({ "target": pane_id, "text": text }),
            after_write_pause: "",
        };
        if lease.send(send).is_err() {
            self.fail_parked_renew(
                job_id,
                &SliceError::Herdr(HerdrError::Unexpected(
                    "herdr lease closed before renew send".into(),
                )),
            );
        } else {
            self.awaiting_renews[index].lease = Some(lease);
        }
    }

    fn on_renew_done(&mut self, job_id: u64, result: Result<HerdrJobResult, HerdrError>) {
        let Some(index) = self.renew_index_for_job(job_id) else {
            return;
        };
        match &self.awaiting_renews[index].state {
            RenewParkState::Probe => self.on_renew_probe(index, job_id, result),
            RenewParkState::ClearingSend(_) => self.on_renew_clear_sent(index, job_id, result),
            RenewParkState::RotationGet => self.on_renew_rotation(index, job_id, result),
            RenewParkState::InjectSend(_) => self.on_renew_inject_sent(index, job_id, result),
            RenewParkState::ConfirmGet => self.on_renew_confirm(index, job_id, result),
        }
    }

    fn on_renew_probe(
        &mut self,
        index: usize,
        job_id: u64,
        result: Result<HerdrJobResult, HerdrError>,
    ) {
        let observed = match result {
            Ok(HerdrJobResult::Agent(agent)) => agent,
            Ok(_) => {
                self.drop_parked_renew(
                    index,
                    &SliceError::Herdr(HerdrError::Unexpected(
                        "renew probe returned a non-agent result".into(),
                    )),
                );
                return;
            }
            Err(error) => {
                self.drop_parked_renew(index, &SliceError::Herdr(error));
                return;
            }
        };
        let now_ms = crate::store::store_clock_ms().unwrap_or(0);
        let item = self.awaiting_renews[index].item.clone();
        match self.kelpie.record_renew_clearing(&item, now_ms, &observed) {
            Ok(write) => {
                let open_id = self.alloc_job();
                self.submit_owned(
                    HerdrJob::Open {
                        job_id: open_id,
                        pane_id: write.pane_id.clone(),
                        negotiate: false,
                    },
                    HerdrOwner::Renew,
                );
                self.awaiting_renews[index].herdr_job = Some(open_id);
                self.awaiting_renews[index].state = RenewParkState::ClearingSend(write);
            }
            Err(error) => self.drop_parked_renew(index, &error),
        }
        let _ = job_id;
    }

    fn on_renew_clear_sent(
        &mut self,
        index: usize,
        job_id: u64,
        result: Result<HerdrJobResult, HerdrError>,
    ) {
        drop_lease(self.awaiting_renews[index].lease.take());
        let RenewParkState::ClearingSend(write) = self.awaiting_renews[index].state.clone() else {
            return;
        };
        let now_ms = crate::store::store_clock_ms().unwrap_or(0);
        let renew_id = self.awaiting_renews[index].item.renew_id;
        let mapped = match result {
            Ok(HerdrJobResult::Prompt(_) | HerdrJobResult::Agent(_)) => Ok(()),
            Ok(_) => Err(HerdrError::Unexpected(
                "renew clear returned a non-agent result".into(),
            )),
            Err(error) => Err(error),
        };
        match self
            .kelpie
            .apply_renew_attempt_result(renew_id, &write.request_id, now_ms, mapped)
        {
            Ok(_) => self.finish_parked_renew(index),
            Err(error) if Self::renew_drive_skips(&error) => self.finish_parked_renew(index),
            Err(error) => self.drop_parked_renew(index, &error),
        }
        let _ = job_id;
    }

    fn on_renew_rotation(
        &mut self,
        index: usize,
        job_id: u64,
        result: Result<HerdrJobResult, HerdrError>,
    ) {
        let observed = match result {
            Ok(HerdrJobResult::Agent(agent)) => agent,
            Ok(_) => {
                self.drop_parked_renew(
                    index,
                    &SliceError::Herdr(HerdrError::Unexpected(
                        "renew rotation returned a non-agent result".into(),
                    )),
                );
                return;
            }
            Err(error) => {
                self.drop_parked_renew(index, &SliceError::Herdr(error));
                return;
            }
        };
        let now_ms = crate::store::store_clock_ms().unwrap_or(0);
        let item = self.awaiting_renews[index].item.clone();
        match self.kelpie.renew_inject_decision(&item, now_ms, &observed) {
            Ok(None) => self.finish_parked_renew(index),
            Ok(Some(write)) => {
                let open_id = self.alloc_job();
                self.submit_owned(
                    HerdrJob::Open {
                        job_id: open_id,
                        pane_id: write.pane_id.clone(),
                        negotiate: false,
                    },
                    HerdrOwner::Renew,
                );
                self.awaiting_renews[index].herdr_job = Some(open_id);
                self.awaiting_renews[index].state = RenewParkState::InjectSend(write);
            }
            Err(error) if Self::renew_drive_skips(&error) => self.finish_parked_renew(index),
            Err(error) => self.drop_parked_renew(index, &error),
        }
        let _ = job_id;
    }

    fn on_renew_inject_sent(
        &mut self,
        index: usize,
        job_id: u64,
        result: Result<HerdrJobResult, HerdrError>,
    ) {
        drop_lease(self.awaiting_renews[index].lease.take());
        let RenewParkState::InjectSend(write) = self.awaiting_renews[index].state.clone() else {
            return;
        };
        let now_ms = crate::store::store_clock_ms().unwrap_or(0);
        let item = self.awaiting_renews[index].item.clone();
        let mapped = match result {
            Ok(HerdrJobResult::Prompt(_) | HerdrJobResult::Agent(_)) => Ok(()),
            Ok(_) => Err(HerdrError::Unexpected(
                "renew inject returned a non-agent result".into(),
            )),
            Err(error) => Err(error),
        };
        match self
            .kelpie
            .apply_renew_inject_result(&item, now_ms, &write, mapped)
        {
            Ok(_) => self.finish_parked_renew(index),
            Err(error) if Self::renew_drive_skips(&error) => self.finish_parked_renew(index),
            Err(error) => self.drop_parked_renew(index, &error),
        }
        let _ = job_id;
    }

    fn on_renew_confirm(
        &mut self,
        index: usize,
        job_id: u64,
        result: Result<HerdrJobResult, HerdrError>,
    ) {
        let observed = match result {
            Ok(HerdrJobResult::Agent(agent)) => agent,
            Ok(_) => {
                self.drop_parked_renew(
                    index,
                    &SliceError::Herdr(HerdrError::Unexpected(
                        "renew confirm returned a non-agent result".into(),
                    )),
                );
                return;
            }
            Err(error) => {
                self.drop_parked_renew(index, &SliceError::Herdr(error));
                return;
            }
        };
        let now_ms = crate::store::store_clock_ms().unwrap_or(0);
        let item = self.awaiting_renews[index].item.clone();
        match self
            .kelpie
            .apply_renew_confirm_observation(&item, now_ms, &observed)
        {
            Ok(true) => {
                let _ = self.kelpie.store_mut().complete_renew(item.renew_id);
                self.finish_parked_renew(index);
            }
            Ok(false) => self.finish_parked_renew(index),
            Err(error) if Self::renew_drive_skips(&error) => self.finish_parked_renew(index),
            Err(error) => self.drop_parked_renew(index, &error),
        }
        let _ = job_id;
    }

    fn finish_parked_renew(&mut self, index: usize) {
        let mut renew = self.awaiting_renews.remove(index);
        drop_lease(renew.lease.take());
        self.renew_inflight.remove(&renew.item.renew_id);
    }

    fn drop_parked_renew(&mut self, index: usize, error: &SliceError) {
        if !Self::renew_drive_skips(error) {
            self.note_renew_drive_error(error);
        }
        self.finish_parked_renew(index);
    }

    fn fail_parked_renew(&mut self, job_id: u64, error: &SliceError) {
        if let Some(index) = self.renew_index_for_job(job_id) {
            self.drop_parked_renew(index, error);
        }
    }

    fn renew_index_for_job(&self, job_id: u64) -> Option<usize> {
        self.awaiting_renews
            .iter()
            .position(|renew| renew.herdr_job == Some(job_id))
    }

    fn drive_parked_opens(&mut self) {
        let deadline = Instant::now() + Duration::from_millis(50);
        while Instant::now() < deadline {
            self.drain_herdr_events();
            if self
                .awaiting_prompts
                .values()
                .all(|prompt| prompt.lease.is_some())
            {
                return;
            }
            thread::sleep(Duration::from_millis(1));
        }
    }

    fn schedule_reminders(&mut self) {
        if self.reminder_job.is_some() || !self.reminder_inflight.is_empty() {
            return;
        }
        let occupancy = self.kelpie.occupancy_sample_needed().unwrap_or(false);
        match self.kelpie.collect_due_reminders() {
            Ok((due, boundaries)) if due.is_empty() && boundaries.is_empty() && !occupancy => {}
            Ok((due, boundaries)) => {
                let job_id = self.alloc_job();
                self.submit_owned(
                    HerdrJob::LifecycleSnapshot { job_id },
                    HerdrOwner::ReminderSnapshot,
                );
                self.reminder_job = Some(job_id);
                self.pending_reminders = Some((due, boundaries));
            }
            Err(error) => {
                let _ = self
                    .kelpie
                    .store_mut()
                    .create_operator_notice(&format!("reminder fire failed: {error}"));
            }
        }
    }

    fn on_reminder_snapshot(&mut self, result: Result<HerdrJobResult, HerdrError>) {
        self.reminder_job = None;
        let Some((due, boundaries)) = self.pending_reminders.take() else {
            return;
        };
        let Ok(HerdrJobResult::Lifecycle(snapshot)) = result else {
            return;
        };
        let _ = self.kelpie.accrue_occupancy_from_snapshot(&snapshot);
        match self
            .kelpie
            .reminders_after_snapshot(due, boundaries, &snapshot)
        {
            Ok(prepared) => {
                for reminder in prepared {
                    self.reminder_inflight
                        .insert(reminder.reminder.ask_message_id);
                    self.park_reminder(reminder);
                }
            }
            Err(error) => {
                let _ = self
                    .kelpie
                    .store_mut()
                    .create_operator_notice(&format!("reminder fire failed: {error}"));
            }
        }
    }

    fn park_reminder(&mut self, reminder: PreparedReminder) {
        self.park_prompt(AwaitingPrompt {
            request_id: String::new(),
            stream: None,
            prepared: PreparedPrompt {
                operation_id: OperationId::new(),
                recipient_incarnation: reminder.reminder.recipient_incarnation,
                pane_id: reminder.reminder.pane_id.clone(),
                envelope: reminder.envelope.clone(),
                request_id: reminder.request_id.clone(),
                queued: false,
                pause_before_write: "",
                after_write_pause: "",
                pause_before_commit: "",
            },
            result_json: Value::Null,
            reply_to: None,
            lease: None,
            intent_committed: false,
            owner: PromptOwner::Internal,
            reminder: Some(reminder),
        });
    }

    fn alloc_job(&mut self) -> u64 {
        let job_id = self.next_herdr_job;
        self.next_herdr_job = self.next_herdr_job.saturating_add(1);
        job_id
    }

    fn submit_owned(&mut self, job: HerdrJob, owner: HerdrOwner) {
        let job_id = match &job {
            HerdrJob::Snapshot { job_id, .. }
            | HerdrJob::LifecycleSnapshot { job_id }
            | HerdrJob::Open { job_id, .. }
            | HerdrJob::AgentGet { job_id, .. } => *job_id,
        };
        self.herdr_owners.insert(job_id, owner);
        self.herdr_exec.submit(job);
    }

    fn drain_herdr_events(&mut self) {
        while let Some(event) = self.herdr_exec.try_recv() {
            match event {
                HerdrEvent::Opened { job_id, lease } => {
                    match self.herdr_owners.get(&job_id).copied() {
                        Some(HerdrOwner::Prompt) => self.on_prompt_opened(job_id, lease),
                        Some(HerdrOwner::Start) => self.on_start_opened(job_id, lease),
                        Some(HerdrOwner::Clear) => self.on_clear_opened(job_id, lease),
                        Some(HerdrOwner::Renew) => self.on_renew_opened(job_id, lease),
                        Some(HerdrOwner::Adopt) => self.on_adopt_opened(job_id, lease),
                        Some(HerdrOwner::Rename) => self.on_rename_opened(job_id, lease),
                        Some(HerdrOwner::Retire) => self.on_retire_opened(job_id, lease),
                        _ => {
                            let _ = lease.send(LeaseCmd::Drop);
                        }
                    }
                }
                HerdrEvent::Written {
                    after_write_pause, ..
                } => crate::test_fault::pause(after_write_pause),
                HerdrEvent::Done { job_id, result } => {
                    match self.herdr_owners.get(&job_id).copied() {
                        Some(HerdrOwner::Prompt) => {
                            let prompt = match result {
                                Ok(HerdrJobResult::Prompt(agent)) => Ok(agent),
                                Ok(_) => Err(HerdrError::Unexpected(
                                    "prompt job returned a non-prompt result".into(),
                                )),
                                Err(error) => Err(error),
                            };
                            self.finish_parked_prompt(job_id, prompt);
                        }
                        Some(HerdrOwner::Start) => self.on_start_done(job_id, result),
                        Some(HerdrOwner::ReminderSnapshot) => self.on_reminder_snapshot(result),
                        Some(HerdrOwner::Clear) => self.on_clear_done(job_id, result),
                        Some(HerdrOwner::ClientRead) => self.on_client_read_done(job_id, result),
                        Some(HerdrOwner::Renew) => self.on_renew_done(job_id, result),
                        Some(HerdrOwner::Adopt) => self.on_adopt_done(job_id, result),
                        Some(HerdrOwner::Rename) => self.on_rename_done(job_id, result),
                        Some(HerdrOwner::Retire) => self.on_retire_done(job_id, result),
                        None => {}
                    }
                }
                HerdrEvent::Failed {
                    job_id,
                    phase,
                    error,
                } => match self.herdr_owners.get(&job_id).copied() {
                    Some(HerdrOwner::Prompt) => self.fail_parked_prompt(job_id, Err(error)),
                    Some(HerdrOwner::Start) => self.on_start_failed(job_id, error),
                    Some(HerdrOwner::ReminderSnapshot) => {
                        self.reminder_job = None;
                        self.pending_reminders = None;
                    }
                    Some(HerdrOwner::Clear) => {
                        self.fail_parked_clear(job_id, SliceError::Herdr(error));
                    }
                    Some(HerdrOwner::ClientRead) => {
                        self.fail_client_read(job_id, SliceError::Herdr(error));
                    }
                    Some(HerdrOwner::Renew) => {
                        self.fail_parked_renew(job_id, &SliceError::Herdr(error));
                    }
                    Some(HerdrOwner::Adopt) => self.on_adopt_failed(job_id, error),
                    Some(HerdrOwner::Rename) => {
                        self.on_rename_failed(job_id, phase, error);
                    }
                    Some(HerdrOwner::Retire) => {
                        self.on_retire_failed(job_id, phase, error);
                    }
                    None => {}
                },
                HerdrEvent::Dropped { job_id } => {
                    self.herdr_owners.remove(&job_id);
                }
            }
        }
    }

    fn on_prompt_opened(&mut self, job_id: u64, lease: std::sync::mpsc::Sender<LeaseCmd>) {
        let Some(awaiting) = self.awaiting_prompts.get_mut(&job_id) else {
            let _ = lease.send(LeaseCmd::Drop);
            return;
        };
        if let Some(reminder) = awaiting.reminder.clone() {
            if self.kelpie.commit_reminder_intent(&reminder).is_ok() {
                awaiting.intent_committed = true;
                let send = LeaseCmd::Send {
                    request_id: reminder.request_id,
                    method: "agent.prompt".into(),
                    params: serde_json::json!({
                        "target": reminder.reminder.pane_id,
                        "text": reminder.envelope,
                    }),
                    after_write_pause: "",
                };
                if lease.send(send).is_err() {
                    self.fail_parked_prompt(
                        job_id,
                        Err(HerdrError::Unexpected(
                            "herdr lease closed before send".into(),
                        )),
                    );
                } else {
                    awaiting.lease = Some(lease);
                }
            } else {
                let _ = lease.send(LeaseCmd::Drop);
                self.fail_parked_prompt(
                    job_id,
                    Err(HerdrError::Unexpected("reminder is no longer due".into())),
                );
            }
            return;
        }
        match self.kelpie.commit_prompt_intent(&awaiting.prepared) {
            Ok(()) => {
                awaiting.intent_committed = true;
                let send = LeaseCmd::Send {
                    request_id: awaiting.prepared.request_id.clone(),
                    method: "agent.prompt".into(),
                    params: serde_json::json!({
                        "target": awaiting.prepared.pane_id,
                        "text": awaiting.prepared.envelope,
                    }),
                    after_write_pause: awaiting.prepared.after_write_pause,
                };
                if lease.send(send).is_err() {
                    self.fail_parked_prompt(
                        job_id,
                        Err(HerdrError::Unexpected(
                            "herdr lease closed before send".into(),
                        )),
                    );
                } else {
                    awaiting.lease = Some(lease);
                }
            }
            Err(error) => {
                let _ = lease.send(LeaseCmd::Drop);
                self.answer_parked_prompt(job_id, Err(error));
            }
        }
    }

    fn on_start_opened(&mut self, job_id: u64, lease: std::sync::mpsc::Sender<LeaseCmd>) {
        let Some(index) = self.start_index_for_job(job_id) else {
            let _ = lease.send(LeaseCmd::Drop);
            return;
        };
        if matches!(
            self.awaiting_starts[index].phase,
            StartPhase::Initial { .. }
        ) {
            self.on_initial_opened(index, lease);
            return;
        }
        let Some(declared) = self.awaiting_starts[index].declared else {
            let _ = lease.send(LeaseCmd::Drop);
            self.fail_parked_start(
                job_id,
                SliceError::Store(StoreError::InvalidRecord(
                    "start opened before it was declared".into(),
                )),
            );
            return;
        };
        let attempt_index = match self.awaiting_starts[index].phase {
            StartPhase::Opening { attempt_index, .. }
            | StartPhase::Sending { attempt_index, .. } => attempt_index,
            _ => 1,
        };
        let busy_deadline = match self.awaiting_starts[index].phase {
            StartPhase::Opening { busy_deadline, .. }
            | StartPhase::Sending { busy_deadline, .. } => busy_deadline,
            _ => self.awaiting_starts[index].deadline,
        };
        let request_id = Kelpie::start_request_id(declared.operation_id, attempt_index);
        match self.kelpie.commit_start_intent(&declared, &request_id) {
            Ok(()) => {
                let start = &mut self.awaiting_starts[index];
                start.intent_committed = true;
                let send = LeaseCmd::Send {
                    request_id,
                    method: "agent.start".into(),
                    params: Kelpie::start_params(&start.intent),
                    after_write_pause: "start_after_write_before_response",
                };
                if lease.send(send).is_err() {
                    self.fail_parked_start(
                        job_id,
                        SliceError::Herdr(HerdrError::Unexpected(
                            "herdr lease closed before start send".into(),
                        )),
                    );
                } else {
                    start.lease = Some(lease);
                    start.phase = StartPhase::Sending {
                        busy_deadline,
                        attempt_index,
                    };
                }
            }
            Err(error) => {
                let _ = lease.send(LeaseCmd::Drop);
                self.fail_parked_start(job_id, error);
            }
        }
    }

    /// Route a failed start job by what the failure proves.
    ///
    /// A `Rejected` error during `Sending` means Herdr read the `agent.start`
    /// and answered no: a decisive outcome that belongs to
    /// [`Kelpie::apply_agent_start_result`], which retries a busy pane within
    /// its budget and records every other refusal against the operation. Only
    /// that path can reach the busy retry; answering the client straight from
    /// the lease failure left the operation `pending` and the pane untried.
    /// Transport failures keep their existing fast fail.
    fn on_start_failed(&mut self, job_id: u64, error: HerdrError) {
        let rejected_while_sending = matches!(error, HerdrError::Rejected { .. })
            && self.start_index_for_job(job_id).is_some_and(|index| {
                matches!(
                    self.awaiting_starts[index].phase,
                    StartPhase::Sending { .. }
                )
            });
        if rejected_while_sending {
            self.on_start_done(job_id, Err(error));
        } else {
            self.fail_parked_start(job_id, SliceError::Herdr(error));
        }
    }

    fn on_start_done(&mut self, job_id: u64, result: Result<HerdrJobResult, HerdrError>) {
        let Some(index) = self.start_index_for_job(job_id) else {
            return;
        };
        match &self.awaiting_starts[index].phase {
            StartPhase::Snapshot => self.on_start_snapshot(index, result),
            StartPhase::Sending { .. } => self.on_start_send(index, job_id, result),
            StartPhase::Ready => self.on_start_ready_get(index, job_id, result),
            StartPhase::Initial { .. } => self.on_initial_done(index, job_id, result),
            _ => {}
        }
    }

    fn on_start_snapshot(&mut self, index: usize, result: Result<HerdrJobResult, HerdrError>) {
        let snapshot = match result {
            Ok(HerdrJobResult::Snapshot(snapshot)) => snapshot,
            Ok(_) => {
                self.fail_start_at(
                    index,
                    SliceError::Herdr(HerdrError::Unexpected(
                        "start snapshot returned a non-snapshot result".into(),
                    )),
                );
                return;
            }
            Err(error) => {
                self.fail_start_at(index, SliceError::Herdr(error));
                return;
            }
        };
        let intent = self.awaiting_starts[index].intent.clone();
        if self.awaiting_starts[index].declared.is_none() {
            match self.kelpie.declare_start_from_snapshot(&intent, &snapshot) {
                Ok((declared, deadline, busy_deadline)) => {
                    let start = &mut self.awaiting_starts[index];
                    start.declared = Some(declared);
                    start.deadline = deadline;
                    start.busy_deadline = busy_deadline;
                    start.attempt_index = 1;
                    self.open_start_lease(index, busy_deadline, 1);
                }
                Err(error) => self.fail_start_at(index, error),
            }
            return;
        }
        if let Err(conflict) = crate::slice::check_pane_matches_intent(&snapshot, &intent) {
            if let Some(declared) = self.awaiting_starts[index].declared {
                let _ = self.kelpie.store_mut().mark_rejected(
                    declared.operation_id,
                    declared.incarnation_id,
                    &conflict.to_string(),
                    crate::domain::DeliveryOutcome::Rejected,
                );
            }
            self.fail_start_at(index, conflict);
            return;
        }
        let attempt_index = self.awaiting_starts[index].attempt_index.saturating_add(1);
        let busy_deadline = self.awaiting_starts[index].busy_deadline;
        self.open_start_lease(index, busy_deadline, attempt_index);
    }

    fn open_start_lease(&mut self, index: usize, busy_deadline: Instant, attempt_index: u32) {
        let job_id = self.alloc_job();
        let pane_id = self.awaiting_starts[index].intent.pane_id.clone();
        self.submit_owned(
            HerdrJob::Open {
                job_id,
                pane_id,
                negotiate: false,
            },
            HerdrOwner::Start,
        );
        let start = &mut self.awaiting_starts[index];
        start.herdr_job = Some(job_id);
        start.phase = StartPhase::Opening {
            busy_deadline,
            attempt_index,
        };
    }

    fn on_start_send(
        &mut self,
        index: usize,
        job_id: u64,
        result: Result<HerdrJobResult, HerdrError>,
    ) {
        let agent = match result {
            Ok(HerdrJobResult::Agent(agent)) => Ok(agent),
            Ok(_) => Err(HerdrError::Unexpected(
                "agent.start returned a non-agent result".into(),
            )),
            Err(error) => Err(error),
        };
        let Some(declared) = self.awaiting_starts[index].declared else {
            self.fail_parked_start(
                job_id,
                SliceError::Herdr(HerdrError::Unexpected(
                    "start send completed with no declaration".into(),
                )),
            );
            return;
        };
        let (deadline, busy_deadline, attempt_index) = match self.awaiting_starts[index].phase {
            StartPhase::Sending {
                busy_deadline,
                attempt_index,
            } => (
                self.awaiting_starts[index].deadline,
                busy_deadline,
                attempt_index,
            ),
            _ => (
                self.awaiting_starts[index].deadline,
                self.awaiting_starts[index].deadline,
                1,
            ),
        };
        let intent = self.awaiting_starts[index].intent.clone();
        drop_lease(self.awaiting_starts[index].lease.take());
        match self.kelpie.apply_agent_start_result(
            &intent,
            declared,
            deadline,
            busy_deadline,
            attempt_index,
            agent,
        ) {
            Ok(crate::slice::StartSubmit::Submitted { declared, deadline }) => {
                let start = &mut self.awaiting_starts[index];
                start.declared = Some(declared);
                start.deadline = deadline;
                start.herdr_job = None;
                start.phase = StartPhase::Ready;
            }
            Ok(crate::slice::StartSubmit::BusyRetry {
                declared,
                deadline,
                busy_deadline,
                not_before,
                attempt_index,
            }) => {
                let start = &mut self.awaiting_starts[index];
                start.declared = Some(declared);
                start.deadline = deadline;
                start.herdr_job = None;
                start.attempt_index = attempt_index;
                start.busy_deadline = busy_deadline;
                start.phase = StartPhase::Busy(BusyStartRetry {
                    busy_deadline,
                    not_before,
                    attempt_index,
                });
            }
            Err(error) => self.fail_start_at(index, error),
        }
    }

    fn on_start_ready_get(
        &mut self,
        index: usize,
        job_id: u64,
        result: Result<HerdrJobResult, HerdrError>,
    ) {
        let observed = match result {
            Ok(HerdrJobResult::Agent(agent)) => Ok(Some(agent)),
            Ok(_) => Err(HerdrError::Unexpected(
                "start ready poll returned a non-agent result".into(),
            )),
            Err(error) => Err(error),
        };
        let Some(declared) = self.awaiting_starts[index].declared else {
            self.fail_parked_start(
                job_id,
                SliceError::Herdr(HerdrError::Unexpected(
                    "ready poll with no declaration".into(),
                )),
            );
            return;
        };
        let intent = self.awaiting_starts[index].intent.clone();
        let deadline = self.awaiting_starts[index].deadline;
        self.awaiting_starts[index].herdr_job = None;
        match self
            .kelpie
            .apply_start_observation(&intent, &declared, deadline, observed)
        {
            Ok(None) => {}
            Ok(Some(started)) => self.park_initial_message(index, started),
            Err(error) => self.fail_start_at(index, error),
        }
    }

    fn park_initial_message(&mut self, index: usize, started: crate::store::DeclaredStart) {
        let intent = self.awaiting_starts[index].intent.clone();
        match self.kelpie.begin_initial_message(&intent, started) {
            Ok((prepared, message_id)) => {
                let job_id = self.alloc_job();
                self.submit_owned(
                    HerdrJob::Open {
                        job_id,
                        pane_id: prepared.pane_id.clone(),
                        negotiate: false,
                    },
                    HerdrOwner::Start,
                );
                let start = &mut self.awaiting_starts[index];
                start.declared = Some(started);
                start.herdr_job = Some(job_id);
                start.intent_committed = false;
                start.phase = StartPhase::Initial {
                    prepared,
                    message_id,
                };
            }
            Err(error) => self.fail_start_at(index, error),
        }
    }

    fn on_initial_opened(&mut self, index: usize, lease: std::sync::mpsc::Sender<LeaseCmd>) {
        let prepared =
            if let StartPhase::Initial { prepared, .. } = &self.awaiting_starts[index].phase {
                prepared.clone()
            } else {
                let _ = lease.send(LeaseCmd::Drop);
                return;
            };
        if self.kelpie.commit_prompt_intent(&prepared).is_ok() {
            let send = LeaseCmd::Send {
                request_id: prepared.request_id.clone(),
                method: "agent.prompt".into(),
                params: serde_json::json!({
                    "target": prepared.pane_id,
                    "text": prepared.envelope,
                }),
                after_write_pause: prepared.after_write_pause,
            };
            if lease.send(send).is_err() {
                self.answer_initial_without_write(index);
            } else {
                let start = &mut self.awaiting_starts[index];
                start.intent_committed = true;
                start.lease = Some(lease);
            }
        } else {
            let _ = lease.send(LeaseCmd::Drop);
            self.answer_initial_without_write(index);
        }
    }

    fn on_initial_done(
        &mut self,
        index: usize,
        job_id: u64,
        result: Result<HerdrJobResult, HerdrError>,
    ) {
        let prompt = match result {
            Ok(HerdrJobResult::Prompt(agent)) => Ok(agent),
            Ok(_) => Err(HerdrError::Unexpected(
                "initial message returned a non-prompt result".into(),
            )),
            Err(error) => Err(error),
        };
        let (prepared, message_id) = match &self.awaiting_starts[index].phase {
            StartPhase::Initial {
                prepared,
                message_id,
            } => (prepared.clone(), *message_id),
            _ => return,
        };
        let Some(started) = self.awaiting_starts[index].declared else {
            self.fail_parked_start(
                job_id,
                SliceError::Herdr(HerdrError::Unexpected(
                    "initial message completed with no start declaration".into(),
                )),
            );
            return;
        };
        drop_lease(self.awaiting_starts[index].lease.take());
        let _ = self
            .kelpie
            .complete_initial_delivery(&prepared, started, message_id, prompt);
        self.answer_initial_at(index, started, message_id, prepared.operation_id);
    }

    fn answer_initial_without_write(&mut self, index: usize) {
        let (prepared, message_id) = match &self.awaiting_starts[index].phase {
            StartPhase::Initial {
                prepared,
                message_id,
            } => (prepared.clone(), *message_id),
            _ => return,
        };
        let Some(started) = self.awaiting_starts[index].declared else {
            return;
        };
        drop_lease(self.awaiting_starts[index].lease.take());
        self.answer_initial_at(index, started, message_id, prepared.operation_id);
    }

    fn answer_initial_at(
        &mut self,
        index: usize,
        started: crate::store::DeclaredStart,
        message_id: MessageId,
        operation_id: crate::domain::OperationId,
    ) {
        let result = self
            .kelpie
            .read_launch_result(started, message_id, operation_id);
        self.answer_start_at(index, result);
    }

    fn fail_parked_start(&mut self, job_id: u64, error: SliceError) {
        let Some(index) = self.start_index_for_job(job_id) else {
            return;
        };
        if matches!(
            self.awaiting_starts[index].phase,
            StartPhase::Initial { .. }
        ) {
            if self.awaiting_starts[index].intent_committed {
                self.on_initial_done(
                    index,
                    job_id,
                    Err(HerdrError::Unexpected(error.to_string())),
                );
            } else {
                self.answer_initial_without_write(index);
            }
            return;
        }
        self.fail_start_at(index, error);
    }

    fn fail_start_at(&mut self, index: usize, error: SliceError) {
        let mut start = self.awaiting_starts.remove(index);
        drop_lease(start.lease.take());
        if start.declared.is_some() {
            self.kelpie.note_undelivered_brief(&start.intent, &error);
        }
        let response = respond(&start.request_id, Err(error));
        if let Err(error) = write_response(&mut start.stream, &response) {
            eprintln!("kelpied: parked start response failed: {error}");
        }
    }

    fn answer_start_at(
        &mut self,
        index: usize,
        result: Result<crate::slice::LaunchResult, SliceError>,
    ) {
        let mut start = self.awaiting_starts.remove(index);
        drop_lease(start.lease.take());
        if let Err(error) = &result {
            self.kelpie.note_undelivered_brief(&start.intent, error);
        }
        let response = respond(&start.request_id, result.map(launch_result));
        if let Err(error) = write_response(&mut start.stream, &response) {
            eprintln!("kelpied: parked start response failed: {error}");
        }
    }

    fn release_due_inflight(&mut self, awaiting: &AwaitingPrompt) {
        if matches!(awaiting.owner, PromptOwner::Internal) && awaiting.reminder.is_none() {
            self.due_inflight.remove(&awaiting.prepared.operation_id);
        }
    }

    fn start_clear_probe(&mut self, index: usize) -> Result<(), SliceError> {
        let AwaitingClearState::Settling { clear, .. } = &self.awaiting_clears[index].state else {
            return Ok(());
        };
        let clear = clear.clone();
        let pane_id = self
            .kelpie
            .store()
            .ready_binding(clear.recipient_incarnation)?
            .pane_id;
        let job_id = self.alloc_job();
        let request_id = format!("kelpie:clear:probe:{}", clear.idempotency_key);
        self.submit_owned(
            HerdrJob::AgentGet {
                job_id,
                request_id,
                target: pane_id.clone(),
            },
            HerdrOwner::Clear,
        );
        self.awaiting_clears[index].herdr_job = Some(job_id);
        self.awaiting_clears[index].state = AwaitingClearState::Probe { clear, pane_id };
        Ok(())
    }

    fn start_clear_rotation_get(&mut self, index: usize) {
        let AwaitingClearState::Rotation(ref awaiting) = self.awaiting_clears[index].state else {
            return;
        };
        let operation_id = awaiting.operation_id;
        let target = awaiting.pane_id.clone();
        let job_id = self.alloc_job();
        let request_id = format!("kelpie:clear:rotation:{operation_id}");
        self.submit_owned(
            HerdrJob::AgentGet {
                job_id,
                request_id,
                target,
            },
            HerdrOwner::Clear,
        );
        self.awaiting_clears[index].herdr_job = Some(job_id);
    }

    fn on_clear_opened(&mut self, job_id: u64, lease: std::sync::mpsc::Sender<LeaseCmd>) {
        let Some(index) = self.clear_index_for_job(job_id) else {
            let _ = lease.send(LeaseCmd::Drop);
            return;
        };
        let AwaitingClearState::Sending {
            operation_id,
            pane_id,
            command,
            clear,
            ..
        } = &self.awaiting_clears[index].state
        else {
            let _ = lease.send(LeaseCmd::Drop);
            return;
        };
        let request_id = format!("kelpie:clear:{operation_id}");
        let pane_id = pane_id.clone();
        let command = command.clone();
        let incarnation = clear.recipient_incarnation;
        match self
            .kelpie
            .commit_clear_intent(*operation_id, incarnation, &request_id)
        {
            Ok(()) => {
                crate::test_fault::pause("clear_after_submitted_before_write");
                let send = LeaseCmd::Send {
                    request_id,
                    method: "agent.prompt".into(),
                    params: serde_json::json!({ "target": pane_id, "text": command }),
                    after_write_pause: "clear_after_write_before_response",
                };
                if lease.send(send).is_err() {
                    self.fail_parked_clear(
                        job_id,
                        SliceError::Herdr(HerdrError::Unexpected(
                            "herdr lease closed before clear send".into(),
                        )),
                    );
                } else {
                    self.awaiting_clears[index].lease = Some(lease);
                }
            }
            Err(error) => {
                let _ = lease.send(LeaseCmd::Drop);
                self.fail_parked_clear(job_id, error);
            }
        }
    }

    fn on_clear_done(&mut self, job_id: u64, result: Result<HerdrJobResult, HerdrError>) {
        let Some(index) = self.clear_index_for_job(job_id) else {
            return;
        };
        match &self.awaiting_clears[index].state {
            AwaitingClearState::Probe { .. } => self.on_clear_probe(index, job_id, result),
            AwaitingClearState::Sending { .. } => self.on_clear_command(index, job_id, result),
            AwaitingClearState::Rotation(_) => self.on_clear_rotation(index, job_id, result),
            AwaitingClearState::Settling { .. } => {}
        }
    }

    fn on_clear_probe(
        &mut self,
        index: usize,
        job_id: u64,
        result: Result<HerdrJobResult, HerdrError>,
    ) {
        let observed = match result {
            Ok(HerdrJobResult::Agent(agent)) => agent,
            Ok(_) => {
                self.fail_parked_clear(
                    job_id,
                    SliceError::Herdr(HerdrError::Unexpected(
                        "clear probe returned a non-agent result".into(),
                    )),
                );
                return;
            }
            Err(error) => {
                self.fail_parked_clear(job_id, SliceError::Herdr(error));
                return;
            }
        };
        let AwaitingClearState::Probe { clear, pane_id } = &self.awaiting_clears[index].state
        else {
            return;
        };
        let clear = clear.clone();
        let pane_id = pane_id.clone();
        match self.kelpie.begin_clear_after_probe(
            clear.recipient,
            clear.recipient_incarnation,
            &clear.idempotency_key,
            &pane_id,
            &observed,
        ) {
            Ok((operation_id, command, rotation, pre_clear_session, backend_kind)) => {
                let open_id = self.alloc_job();
                self.submit_owned(
                    HerdrJob::Open {
                        job_id: open_id,
                        pane_id: pane_id.clone(),
                        negotiate: false,
                    },
                    HerdrOwner::Clear,
                );
                self.awaiting_clears[index].herdr_job = Some(open_id);
                self.awaiting_clears[index].state = AwaitingClearState::Sending {
                    clear,
                    pane_id,
                    operation_id,
                    command,
                    rotation,
                    pre_clear_session,
                    backend_kind,
                };
            }
            Err(error) => self.fail_clear_at(index, error),
        }
    }

    fn on_clear_command(
        &mut self,
        index: usize,
        job_id: u64,
        result: Result<HerdrJobResult, HerdrError>,
    ) {
        let observed = match result {
            Ok(HerdrJobResult::Prompt(agent) | HerdrJobResult::Agent(agent)) => Ok(agent),
            Ok(_) => Err(HerdrError::Unexpected(
                "clear command returned a non-agent result".into(),
            )),
            Err(error) => Err(error),
        };
        let AwaitingClearState::Sending {
            clear,
            pane_id,
            operation_id,
            rotation,
            pre_clear_session,
            backend_kind,
            ..
        } = self.awaiting_clears[index].state.clone()
        else {
            return;
        };
        drop_lease(self.awaiting_clears[index].lease.take());
        match self.kelpie.apply_clear_command_result(
            operation_id,
            clear.recipient,
            clear.recipient_incarnation,
            pane_id,
            backend_kind,
            rotation,
            pre_clear_session,
            observed,
        ) {
            Ok(crate::slice::ClearSubmission::Complete(cleared)) => {
                self.answer_clear_at(index, Ok(cleared));
            }
            Ok(crate::slice::ClearSubmission::Awaiting(awaiting)) => {
                self.awaiting_clears[index].herdr_job = None;
                self.awaiting_clears[index].state = AwaitingClearState::Rotation(awaiting);
                self.start_clear_rotation_get(index);
            }
            Err(error) => self.fail_parked_clear(job_id, error),
        }
    }

    fn on_clear_rotation(
        &mut self,
        index: usize,
        job_id: u64,
        result: Result<HerdrJobResult, HerdrError>,
    ) {
        let observed = match result {
            Ok(HerdrJobResult::Agent(agent)) => Ok(agent),
            Ok(_) => Err(HerdrError::Unexpected(
                "clear rotation returned a non-agent result".into(),
            )),
            Err(error) => Err(error),
        };
        let AwaitingClearState::Rotation(awaiting) = self.awaiting_clears[index].state.clone()
        else {
            return;
        };
        self.awaiting_clears[index].herdr_job = None;
        match self
            .kelpie
            .apply_clear_rotation_observation(&awaiting, observed)
        {
            Ok(None) => {}
            Ok(Some(cleared)) => self.answer_clear_at(index, Ok(cleared)),
            Err(error) => self.fail_parked_clear(job_id, error),
        }
    }

    fn answer_clear_at(&mut self, index: usize, result: Result<ClearResult, SliceError>) {
        let mut clear = self.awaiting_clears.remove(index);
        drop_lease(clear.lease.take());
        let response = respond(&clear.request_id, result.map(clear_result));
        if let Err(error) = write_response(&mut clear.stream, &response) {
            eprintln!("kelpied: parked clear response failed: {error}");
        }
    }

    fn fail_parked_clear(&mut self, job_id: u64, error: SliceError) {
        if let Some(index) = self.clear_index_for_job(job_id) {
            self.fail_clear_at(index, error);
        }
    }

    fn clear_index_for_job(&self, job_id: u64) -> Option<usize> {
        self.awaiting_clears
            .iter()
            .position(|clear| clear.herdr_job == Some(job_id))
    }

    fn fail_clear_at(&mut self, index: usize, error: SliceError) {
        let mut clear = self.awaiting_clears.remove(index);
        drop_lease(clear.lease.take());
        let response = respond(&clear.request_id, Err(error));
        if let Err(error) = write_response(&mut clear.stream, &response) {
            eprintln!("kelpied: parked clear response failed: {error}");
        }
    }

    fn finish_parked_prompt(
        &mut self,
        job_id: u64,
        prompt: Result<crate::herdr::AgentObservation, HerdrError>,
    ) {
        let Some(mut awaiting) = self.awaiting_prompts.remove(&job_id) else {
            return;
        };
        self.release_due_inflight(&awaiting);
        drop_lease(awaiting.lease.take());
        if let Some(reminder) = awaiting.reminder.take() {
            let _ = self.kelpie.complete_reminder(&reminder, prompt);
            self.reminder_inflight
                .remove(&reminder.reminder.ask_message_id);
            return;
        }
        if let PromptOwner::Cancel { session, waiting } = awaiting.owner {
            let ok = self
                .kelpie
                .complete_prompt_delivery(&awaiting.prepared, prompt)
                .is_ok();
            self.note_cancel_notice(session, waiting, ok);
            return;
        }
        if let PromptOwner::WaiterRetire { session, ask } = awaiting.owner {
            let ok = self
                .kelpie
                .complete_prompt_delivery(&awaiting.prepared, prompt)
                .is_ok();
            self.note_waiter_retire_notice(session, ask, ok);
            return;
        }
        let delivered = self
            .kelpie
            .complete_prompt_delivery(&awaiting.prepared, prompt);
        fill_prompt_result(&mut self.kelpie, &mut awaiting, delivered.is_ok());
        write_parked_prompt(awaiting, delivered);
    }

    fn fail_parked_prompt(
        &mut self,
        job_id: u64,
        prompt: Result<crate::herdr::AgentObservation, HerdrError>,
    ) {
        let Some(mut awaiting) = self.awaiting_prompts.remove(&job_id) else {
            return;
        };
        self.release_due_inflight(&awaiting);
        drop_lease(awaiting.lease.take());
        if let Some(reminder) = awaiting.reminder.take() {
            if awaiting.intent_committed {
                let _ = self.kelpie.complete_reminder(&reminder, prompt);
            }
            self.reminder_inflight
                .remove(&reminder.reminder.ask_message_id);
            return;
        }
        if let PromptOwner::Cancel { session, waiting } = awaiting.owner {
            let ok = if awaiting.intent_committed {
                self.kelpie
                    .complete_prompt_delivery(&awaiting.prepared, prompt)
                    .is_ok()
            } else {
                if let Err(error) = &prompt {
                    let _ = self.kelpie.reject_unsent_prompt(&awaiting.prepared, error);
                }
                false
            };
            self.note_cancel_notice(session, waiting, ok);
            return;
        }
        if let PromptOwner::WaiterRetire { session, ask } = awaiting.owner {
            let ok = if awaiting.intent_committed {
                self.kelpie
                    .complete_prompt_delivery(&awaiting.prepared, prompt)
                    .is_ok()
            } else {
                if let Err(error) = &prompt {
                    let _ = self.kelpie.reject_unsent_prompt(&awaiting.prepared, error);
                }
                false
            };
            self.note_waiter_retire_notice(session, ask, ok);
            return;
        }
        let delivered = if awaiting.intent_committed {
            self.kelpie
                .complete_prompt_delivery(&awaiting.prepared, prompt)
        } else {
            prompt.map(|_| ()).map_err(SliceError::Herdr)
        };
        fill_prompt_result(&mut self.kelpie, &mut awaiting, delivered.is_ok());
        write_parked_prompt(awaiting, delivered);
    }

    fn note_cancel_notice(&mut self, session: u64, waiting: bool, ok: bool) {
        let Some(cancel) = self.awaiting_cancels.get_mut(&session) else {
            return;
        };
        if waiting {
            cancel.outcome.delivered = ok;
        } else {
            cancel.outcome.owing_delivered = ok;
        }
        cancel.remaining = cancel.remaining.saturating_sub(1);
        if cancel.remaining != 0 {
            return;
        }
        let Some(mut cancel) = self.awaiting_cancels.remove(&session) else {
            return;
        };
        let response = respond(&cancel.request_id, Ok(cancel_result(cancel.outcome)));
        if let Err(error) = write_response(&mut cancel.stream, &response) {
            eprintln!("kelpied: parked cancel response failed: {error}");
        }
    }

    fn note_waiter_retire_notice(&mut self, session: u64, ask: MessageId, ok: bool) {
        let Some(retire) = self.awaiting_waiter_retires.get_mut(&session) else {
            return;
        };
        if let Some(notice) = retire
            .outcome
            .owing_notices
            .iter_mut()
            .find(|notice| notice.ask_message_id == ask)
        {
            notice.delivered = ok;
        }
        retire.remaining = retire.remaining.saturating_sub(1);
        if retire.remaining != 0 {
            return;
        }
        let Some(mut retire) = self.awaiting_waiter_retires.remove(&session) else {
            return;
        };
        let result = waiter_retire_result(retire.logical_agent_id, &retire.outcome);
        let response = respond(&retire.request_id, Ok(result));
        if let Err(error) = write_response(&mut retire.stream, &response) {
            eprintln!("kelpied: parked waiter-retire response failed: {error}");
        }
    }

    fn answer_parked_prompt(&mut self, job_id: u64, result: Result<Value, SliceError>) {
        let Some(mut awaiting) = self.awaiting_prompts.remove(&job_id) else {
            return;
        };
        self.release_due_inflight(&awaiting);
        drop_lease(awaiting.lease.take());
        if let Some(mut stream) = awaiting.stream.take() {
            let response = respond(&awaiting.request_id, result);
            if let Err(error) = write_response(&mut stream, &response) {
                eprintln!("kelpied: parked prompt response failed: {error}");
            }
        }
    }

    /// Observe every awaiting start once, answering the ones that settle.
    ///
    /// Returns whether any start settled, which the caller uses to keep the loop
    /// hot rather than sleeping through a readiness transition.
    fn advance_awaiting_starts(&mut self) -> bool {
        let mut progressed = false;
        let mut idx = 0;
        while idx < self.awaiting_starts.len() {
            if self.awaiting_starts[idx].herdr_job.is_some() {
                idx += 1;
                continue;
            }
            match self.awaiting_starts[idx].phase {
                StartPhase::Busy(ref busy) if Instant::now() < busy.not_before => {
                    idx += 1;
                }
                StartPhase::Busy(ref busy) if Instant::now() >= busy.busy_deadline => {
                    let declared = self.awaiting_starts[idx].declared;
                    let error = SliceError::Store(StoreError::Conflict(
                        "pane stayed busy until the start retry budget elapsed".into(),
                    ));
                    if let Some(declared) = declared {
                        let _ = self.kelpie.store_mut().mark_rejected(
                            declared.operation_id,
                            declared.incarnation_id,
                            "pane stayed busy until the start retry budget elapsed",
                            crate::domain::DeliveryOutcome::Rejected,
                        );
                    }
                    self.fail_start_at(idx, error);
                    progressed = true;
                }
                StartPhase::Busy(_) => {
                    self.park_busy_retry_snapshot(idx);
                    progressed = true;
                    idx += 1;
                }
                StartPhase::Ready => {
                    self.submit_start_ready_get(idx);
                    progressed = true;
                    idx += 1;
                }
                _ => idx += 1,
            }
        }
        progressed
    }

    fn park_busy_retry_snapshot(&mut self, index: usize) {
        let job_id = self.alloc_job();
        self.submit_owned(
            HerdrJob::Snapshot {
                job_id,
                negotiate: false,
            },
            HerdrOwner::Start,
        );
        let start = &mut self.awaiting_starts[index];
        start.herdr_job = Some(job_id);
        start.phase = StartPhase::Snapshot;
    }

    fn submit_start_ready_get(&mut self, index: usize) {
        let Some(declared) = self.awaiting_starts[index].declared else {
            return;
        };
        let job_id = self.alloc_job();
        let request_id = format!("kelpie:start-ready:{}", declared.operation_id);
        let target = self.awaiting_starts[index].intent.pane_id.clone();
        self.submit_owned(
            HerdrJob::AgentGet {
                job_id,
                request_id,
                target,
            },
            HerdrOwner::Start,
        );
        self.awaiting_starts[index].herdr_job = Some(job_id);
    }

    fn advance_awaiting_clears(&mut self) -> bool {
        let mut progressed = false;
        let mut idx = 0;
        while idx < self.awaiting_clears.len() {
            if self.awaiting_clears[idx].herdr_job.is_some() {
                idx += 1;
                continue;
            }
            match self.awaiting_clears[idx].state {
                AwaitingClearState::Settling { not_before_ms, .. } => {
                    let now_ms = crate::store::store_clock_ms().unwrap_or(0);
                    if now_ms < not_before_ms {
                        idx += 1;
                        continue;
                    }
                    if let Err(error) = self.start_clear_probe(idx) {
                        self.fail_clear_at(idx, error);
                        progressed = true;
                    } else {
                        progressed = true;
                        idx += 1;
                    }
                }
                AwaitingClearState::Rotation(_) => {
                    self.start_clear_rotation_get(idx);
                    progressed = true;
                    idx += 1;
                }
                AwaitingClearState::Probe { .. } | AwaitingClearState::Sending { .. } => {
                    idx += 1;
                }
            }
        }
        progressed
    }

    fn park_inbox(&mut self, session: InboxSession) {
        let waiter_id = session.waiter_id;
        self.inboxes.retain(|open| open.waiter_id != waiter_id);
        self.inboxes.push(session);
        let _ = self.advance_inboxes();
    }

    fn advance_inboxes(&mut self) -> bool {
        let mut progressed = false;
        let mut still_open = Vec::with_capacity(self.inboxes.len());
        for mut session in std::mem::take(&mut self.inboxes) {
            match pump_inbox(&mut session, &mut self.kelpie) {
                Ok(changed) => {
                    progressed |= changed;
                    still_open.push(session);
                }
                Err(error) => {
                    eprintln!("kelpied: inbox connection failed: {error}");
                }
            }
        }
        self.inboxes = still_open;
        progressed
    }

    fn serve_one_waiter_retire(
        &mut self,
        mut pending: PendingWaiterRetire,
    ) -> Result<(), DaemonError> {
        let mut outcome = WaiterRetireOutcome {
            cancelled_ask_ids: pending.prepared.cancelled_ask_ids,
            owing_notices: Vec::with_capacity(pending.prepared.owing_notices.len()),
        };
        for notice in pending.prepared.owing_notices {
            let delivered = match notice.prepared {
                Some(prompt) => match self.kelpie.send_cancellation_prompt(&prompt) {
                    Ok(delivered) => delivered,
                    Err(error) => {
                        let response = respond(&pending.request_id, Err(error));
                        write_response(&mut pending.stream, &response)?;
                        return Ok(());
                    }
                },
                None => false,
            };
            outcome.owing_notices.push(WaiterRetireOwingNotice {
                ask_message_id: notice.ask_message_id,
                message_id: notice.message_id,
                delivered,
            });
        }
        let result = waiter_retire_result(pending.logical_agent_id, &outcome);
        write_response(
            &mut pending.stream,
            &respond(&pending.request_id, Ok(result)),
        )?;
        Ok(())
    }

    fn serve_one_rename(&mut self, mut rename: AwaitingRename) -> Result<(), DaemonError> {
        let result = match &rename.state {
            RenameParkState::Snapshot(preflight) => self
                .kelpie
                .rename(preflight.logical_agent_id, &preflight.new_name)
                .map(|identity| {
                    serde_json::json!({
                        "logical_agent_id": identity.logical_agent_id,
                        "incarnation_id": identity.incarnation_id,
                        "public_name": identity.public_name
                    })
                }),
            _ => Err(SliceError::Herdr(HerdrError::Unexpected(
                "inline rename is missing its snapshot intent".into(),
            ))),
        };
        write_response(&mut rename.stream, &respond(&rename.request_id, result))?;
        Ok(())
    }

    fn serve_one_retire(&mut self, mut retire: AwaitingRetireClose) -> Result<(), DaemonError> {
        let result = match &retire.state {
            RetireParkState::Snapshot(preflight) => self
                .kelpie
                .retire(preflight.incarnation_id, &retire.request_id, true)
                .map(|(operation_id, released)| {
                    serde_json::json!({
                        "operation_id": operation_id,
                        "pane_released": released
                    })
                }),
            _ => Err(SliceError::Herdr(HerdrError::Unexpected(
                "inline retire is missing its snapshot intent".into(),
            ))),
        };
        write_response(&mut retire.stream, &respond(&retire.request_id, result))?;
        Ok(())
    }

    /// Accept and serve one request, primarily for deterministic integration tests.
    ///
    /// # Errors
    ///
    /// Returns for socket, request decoding, or response encoding failures.
    pub fn serve_one(&mut self) -> Result<(), DaemonError> {
        self.listener.set_nonblocking(false)?;
        let (stream, _) = self.listener.accept()?;
        match serve_stream(stream, &mut self.kelpie)? {
            Served::Answered => Ok(()),
            // No poll loop runs behind this entry point, so settle inline rather
            // than leaving a caller waiting on a response nothing will send.
            Served::AwaitingStart(mut awaiting) => {
                let settled = settle_start_inline(&mut self.kelpie, &mut awaiting);
                let response = respond(&awaiting.request_id, settled.map(launch_result));
                write_response(&mut awaiting.stream, &response)
            }
            Served::Inbox(session) => {
                self.park_inbox(*session);
                Ok(())
            }
            Served::AwaitingPrompt(mut awaiting) => {
                let delivered = self.kelpie.send_prepared_prompt(&awaiting.prepared);
                fill_prompt_result(&mut self.kelpie, &mut awaiting, delivered.is_ok());
                let mut stream = awaiting.stream.ok_or_else(|| {
                    std::io::Error::other("inline prompt is missing its client stream")
                })?;
                let response = respond(
                    &awaiting.request_id,
                    delivered.map(|()| awaiting.result_json.clone()),
                );
                write_response(&mut stream, &response)
            }
            Served::AwaitingWaiterRetire(pending) => self.serve_one_waiter_retire(*pending),
            Served::AwaitingCancel(pending) => {
                let mut outcome = pending.outcome;
                let mut stream = pending.stream;
                for prompt in pending.prompts {
                    match self.kelpie.send_cancellation_prompt(&prompt.prepared) {
                        Ok(ok) if prompt.waiting => outcome.delivered = ok,
                        Ok(ok) => outcome.owing_delivered = ok,
                        Err(error) => {
                            let response = respond(&pending.request_id, Err(error));
                            return write_response(&mut stream, &response);
                        }
                    }
                }
                let response = respond(&pending.request_id, Ok(cancel_result(outcome)));
                write_response(&mut stream, &response)
            }
            Served::AwaitingRead(read) => {
                let result = finish_client_read(&mut self.kelpie, &read.kind);
                let mut stream = read.stream;
                write_response(&mut stream, &respond(&read.request_id, result))
            }
            Served::AwaitingAdopt(adopt) => {
                let result = match &adopt.state {
                    AdoptParkState::Snapshot(intent) => self.kelpie.adopt(intent).map(|created| {
                        serde_json::json!({
                            "logical_agent_id": created.logical_agent_id,
                            "incarnation_id": created.incarnation_id,
                            "operation_id": created.operation_id,
                            "outcome": "succeeded"
                        })
                    }),
                    _ => Err(SliceError::Herdr(HerdrError::Unexpected(
                        "inline adopt is missing its snapshot intent".into(),
                    ))),
                };
                let mut stream = adopt.stream;
                write_response(&mut stream, &respond(&adopt.request_id, result))
            }
            Served::AwaitingRename(rename) => self.serve_one_rename(*rename),
            Served::AwaitingRetire(retire) => self.serve_one_retire(*retire),
            Served::AwaitingClear(mut awaiting) => loop {
                match advance_clear_state(awaiting.state, &mut self.kelpie) {
                    Ok(ClearDispatch::Awaiting(state)) => {
                        awaiting.state = state;
                        thread::sleep(Duration::from_millis(50));
                    }
                    result => {
                        let result = result.and_then(|progress| match progress {
                            ClearDispatch::Complete(cleared) => Ok(clear_result(cleared)),
                            ClearDispatch::Awaiting(_) => unreachable!(),
                        });
                        let response = respond(&awaiting.request_id, result);
                        break write_response(&mut awaiting.stream, &response);
                    }
                }
            },
        }
    }
}

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.socket_path);
    }
}

/// What serving one connection produced.
#[derive(Debug)]
enum Served {
    /// The response was written and the connection is finished.
    Answered,
    /// A start was submitted; the connection waits for its readiness outcome.
    AwaitingStart(Box<AwaitingStart>),
    /// A clear is waiting for prompt spacing or backend session rotation.
    AwaitingClear(Box<AwaitingClearRequest>),
    /// A socket waiter claimed this connection as its inbox.
    Inbox(Box<InboxSession>),
    /// Ask/tell Herdr write is running off-thread.
    AwaitingPrompt(Box<AwaitingPrompt>),
    /// Cancel notices are running off-thread.
    AwaitingCancel(Box<PendingCancel>),
    AwaitingWaiterRetire(Box<PendingWaiterRetire>),
    AwaitingRead(Box<AwaitingRead>),
    AwaitingAdopt(Box<AwaitingAdopt>),
    AwaitingRename(Box<AwaitingRename>),
    AwaitingRetire(Box<AwaitingRetireClose>),
}

#[derive(Debug)]
struct PendingCancel {
    request_id: String,
    stream: UnixStream,
    outcome: CancelOutcome,
    prompts: Vec<PreparedCancellation>,
}

#[derive(Debug)]
struct PendingWaiterRetire {
    request_id: String,
    stream: UnixStream,
    logical_agent_id: LogicalAgentId,
    prepared: PreparedWaiterRetire,
}

fn log_slow_phase(name: &str, started: &mut Instant) {
    let elapsed = started.elapsed();
    *started = Instant::now();
    if elapsed >= SLOW_POLL {
        eprintln!("kelpied: slow poll phase {name} {}ms", elapsed.as_millis());
    }
}

enum ReadProgress {
    Got,
    WouldBlock,
    Eof,
}

fn read_into_buf(stream: &mut UnixStream, buf: &mut Vec<u8>) -> Result<ReadProgress, DaemonError> {
    let mut chunk = [0_u8; 4096];
    match stream.read(&mut chunk) {
        Ok(0) => Ok(ReadProgress::Eof),
        Ok(n) => {
            buf.extend_from_slice(&chunk[..n]);
            Ok(ReadProgress::Got)
        }
        Err(error)
            if error.kind() == std::io::ErrorKind::WouldBlock
                || error.kind() == std::io::ErrorKind::Interrupted =>
        {
            Ok(ReadProgress::WouldBlock)
        }
        Err(error) => Err(error.into()),
    }
}

fn serve_stream(stream: UnixStream, kelpie: &mut Kelpie) -> Result<Served, DaemonError> {
    // Tests and `serve_one` still read one request to completion on this
    // connection. The live loop parks incomplete reads instead (`pump_reading`).
    stream.set_read_timeout(Some(CLIENT_REQUEST_DEADLINE))?;
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    reader.read_line(&mut line)?;
    let leftover = reader.buffer().to_vec();
    let stream = reader.into_inner();
    serve_request(stream, line.as_bytes(), leftover, kelpie)
}

fn serve_request(
    stream: UnixStream,
    line: &[u8],
    leftover: Vec<u8>,
    kelpie: &mut Kelpie,
) -> Result<Served, DaemonError> {
    let _ = stream.set_nonblocking(false);
    let _ = stream.set_write_timeout(Some(CLIENT_WRITE_TIMEOUT));
    let line = std::str::from_utf8(line)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
    let line = line.trim_end_matches(['\n', '\r']);
    let dispatch_started = Instant::now();
    let method = serde_json::from_str::<ClientRequest>(line)
        .ok()
        .map(|request| request.method.clone());
    let served = serve_parsed_line(stream, line, leftover, kelpie)?;
    if dispatch_started.elapsed() >= SLOW_POLL {
        eprintln!(
            "kelpied: slow method {} {}ms",
            method.as_deref().unwrap_or("?"),
            dispatch_started.elapsed().as_millis()
        );
    }
    Ok(served)
}

#[allow(clippy::too_many_lines, clippy::collapsible_if)]
fn serve_parsed_line(
    mut stream: UnixStream,
    line: &str,
    leftover: Vec<u8>,
    kelpie: &mut Kelpie,
) -> Result<Served, DaemonError> {
    let response = match serde_json::from_str::<ClientRequest>(line) {
        Ok(request) if request.method == "start" || request.method == "handoff" => {
            match prepare_start(request.params, kelpie) {
                Ok(intent) => {
                    let deadline =
                        Instant::now() + Duration::from_millis(intent.readiness_timeout_ms);
                    return Ok(Served::AwaitingStart(Box::new(AwaitingStart {
                        request_id: request.id,
                        intent,
                        declared: None,
                        deadline,
                        stream,
                        phase: StartPhase::Snapshot,
                        herdr_job: None,
                        lease: None,
                        intent_committed: false,
                        attempt_index: 1,
                        busy_deadline: deadline,
                    })));
                }
                Err(error) => {
                    let response = respond(&request.id, Err(error));
                    write_response(&mut stream, &response)?;
                    return Ok(Served::Answered);
                }
            }
        }
        Ok(request) if request.method == "inbox.claim" => match claim_inbox(&request, kelpie) {
            Ok(waiter_id) => {
                let response = respond(
                    &request.id,
                    Ok(serde_json::json!({
                        "logical_agent_id": waiter_id,
                        "claimed": true,
                        "delivery_transport": "socket_inbox",
                    })),
                );
                write_json_line(&mut stream, &response)?;
                stream.set_nonblocking(true)?;
                return Ok(Served::Inbox(Box::new(InboxSession {
                    waiter_id,
                    stream,
                    read_buf: leftover,
                    write_buf: Vec::new(),
                    offered: HashSet::new(),
                    awaiting_write_pause: false,
                })));
            }
            Err(error) => {
                let response = respond(&request.id, Err(error));
                write_response(&mut stream, &response)?;
                return Ok(Served::Answered);
            }
        },
        Ok(request)
            if request.method == "ask" || request.method == "tell" || request.method == "reply" =>
        {
            if let Some((alias, lazy_key)) = pending_alias_bind(&request, kelpie) {
                return Ok(Served::AwaitingRead(Box::new(AwaitingRead {
                    request_id: request.id.clone(),
                    stream,
                    kind: ClientReadKind::Alias {
                        alias,
                        lazy_key,
                        resume: request,
                    },
                })));
            }
            match prepare_client_prompt(&request, kelpie) {
                Ok((result_json, None, _)) => {
                    let response = respond(&request.id, Ok(result_json));
                    write_response(&mut stream, &response)?;
                    return Ok(Served::Answered);
                }
                Ok((result_json, Some(prepared), reply_to)) => {
                    return Ok(Served::AwaitingPrompt(Box::new(AwaitingPrompt {
                        request_id: request.id,
                        stream: Some(stream),
                        prepared,
                        result_json,
                        reply_to,
                        lease: None,
                        intent_committed: false,
                        owner: PromptOwner::Client,
                        reminder: None,
                    })));
                }
                Err(error) => {
                    let response = respond(&request.id, Err(error));
                    write_response(&mut stream, &response)?;
                    return Ok(Served::Answered);
                }
            }
        }
        Ok(request) if request.method == "clear" => {
            if let Some((alias, lazy_key)) = pending_alias_bind(&request, kelpie) {
                return Ok(Served::AwaitingRead(Box::new(AwaitingRead {
                    request_id: request.id.clone(),
                    stream,
                    kind: ClientReadKind::Alias {
                        alias,
                        lazy_key,
                        resume: request,
                    },
                })));
            }
            match begin_clear(request.params, kelpie) {
                Ok(ClearDispatch::Complete(cleared)) => {
                    let response = respond(&request.id, Ok(clear_result(cleared)));
                    write_response(&mut stream, &response)?;
                    return Ok(Served::Answered);
                }
                Ok(ClearDispatch::Awaiting(state)) => {
                    return Ok(Served::AwaitingClear(Box::new(AwaitingClearRequest {
                        request_id: request.id,
                        state,
                        stream,
                        herdr_job: None,
                        lease: None,
                    })));
                }
                Err(error) => {
                    let response = respond(&request.id, Err(error));
                    write_response(&mut stream, &response)?;
                    return Ok(Served::Answered);
                }
            }
        }
        Ok(request) if request.method == "recover" => {
            return Ok(Served::AwaitingRead(Box::new(AwaitingRead {
                request_id: request.id,
                stream,
                kind: ClientReadKind::Recover,
            })));
        }
        Ok(request) if request.method == "attribution" => {
            match prepare_attribution_read(&request, kelpie) {
                Ok(Some(kind)) => {
                    return Ok(Served::AwaitingRead(Box::new(AwaitingRead {
                        request_id: request.id,
                        stream,
                        kind,
                    })));
                }
                Ok(None) => {
                    let response =
                        respond(&request.id, dispatch_attribution(request.params, kelpie));
                    write_response(&mut stream, &response)?;
                    return Ok(Served::Answered);
                }
                Err(error) => {
                    let response = respond(&request.id, Err(error));
                    write_response(&mut stream, &response)?;
                    return Ok(Served::Answered);
                }
            }
        }
        Ok(request) if request.method == "who" => match prepare_who_read(&request, kelpie) {
            Ok(Some(kind)) => {
                return Ok(Served::AwaitingRead(Box::new(AwaitingRead {
                    request_id: request.id,
                    stream,
                    kind,
                })));
            }
            Ok(None) => {
                let response = respond(&request.id, dispatch_who(request.params, kelpie));
                write_response(&mut stream, &response)?;
                return Ok(Served::Answered);
            }
            Err(error) => {
                let response = respond(&request.id, Err(error));
                write_response(&mut stream, &response)?;
                return Ok(Served::Answered);
            }
        },
        Ok(request) if request.method == "report" => {
            let params = serde_json::from_value::<ReportParams>(request.params.clone()).map_err(
                |error| std::io::Error::new(std::io::ErrorKind::InvalidData, error.to_string()),
            )?;
            if params.live {
                return Ok(Served::AwaitingRead(Box::new(AwaitingRead {
                    request_id: request.id,
                    stream,
                    kind: ClientReadKind::Report {
                        active: params.active,
                    },
                })));
            }
            let response = respond(&request.id, dispatch_report(request.params, kelpie));
            write_response(&mut stream, &response)?;
            return Ok(Served::Answered);
        }
        Ok(request) if request.method == "whoami" => match prepare_whoami_read(&request, kelpie) {
            Ok(None) => {
                let response = respond(
                    &request.id,
                    dispatch_whoami(request.params, kelpie, &request.id),
                );
                write_response(&mut stream, &response)?;
                return Ok(Served::Answered);
            }
            Ok(Some(kind)) => {
                return Ok(Served::AwaitingRead(Box::new(AwaitingRead {
                    request_id: request.id,
                    stream,
                    kind,
                })));
            }
            Err(error) => {
                let response = respond(&request.id, Err(error));
                write_response(&mut stream, &response)?;
                return Ok(Served::Answered);
            }
        },
        Ok(request) if request.method == "adopt" => {
            let intent =
                serde_json::from_value::<AdoptIntent>(request.params).map_err(|error| {
                    std::io::Error::new(std::io::ErrorKind::InvalidData, error.to_string())
                })?;
            return Ok(Served::AwaitingAdopt(Box::new(AwaitingAdopt {
                request_id: request.id,
                stream,
                reply: AdoptReply::Rpc,
                resume: None,
                state: AdoptParkState::Snapshot(intent),
                herdr_job: None,
                lease: None,
            })));
        }
        Ok(request) if request.method == "rename" => {
            let params = serde_json::from_value::<RenameParams>(request.params.clone()).map_err(
                |error| std::io::Error::new(std::io::ErrorKind::InvalidData, error.to_string()),
            )?;
            let agent_id = match (params.agent_id, params.alias.as_deref()) {
                (Some(agent_id), None) => Ok(agent_id),
                (None, Some(alias)) => kelpie.resolve_ready_alias(alias).map(|resolved| resolved.0),
                _ => Err(SliceError::Store(StoreError::InvalidRecord(
                    "provide exactly one of agent_id or alias".into(),
                ))),
            };
            match agent_id.and_then(|agent_id| kelpie.prepare_rename(agent_id, &params.name)) {
                Ok(preflight) => {
                    return Ok(Served::AwaitingRename(Box::new(AwaitingRename {
                        request_id: request.id,
                        stream,
                        state: RenameParkState::Snapshot(preflight),
                        herdr_job: None,
                        lease: None,
                    })));
                }
                Err(error) => {
                    let response = respond(&request.id, Err(error));
                    write_response(&mut stream, &response)?;
                    return Ok(Served::Answered);
                }
            }
        }
        Ok(request) if request.method == "retire" => {
            let params = serde_json::from_value::<RetireParams>(request.params.clone()).map_err(
                |error| std::io::Error::new(std::io::ErrorKind::InvalidData, error.to_string()),
            )?;
            if !params.close_pane {
                let response = dispatch(request, kelpie);
                write_response(&mut stream, &response)?;
                return Ok(Served::Answered);
            }
            match kelpie.prepare_retire_close(params.incarnation_id, &params.idempotency_key) {
                Ok(preflight) => {
                    return Ok(Served::AwaitingRetire(Box::new(AwaitingRetireClose {
                        request_id: request.id,
                        stream,
                        state: RetireParkState::Snapshot(preflight),
                        herdr_job: None,
                        lease: None,
                    })));
                }
                Err(error) => {
                    let response = respond(&request.id, Err(error));
                    write_response(&mut stream, &response)?;
                    return Ok(Served::Answered);
                }
            }
        }
        Ok(request) if request.method == "waiter.retire" => {
            let params = serde_json::from_value::<WaiterRetireParams>(request.params.clone())
                .map_err(|error| {
                    std::io::Error::new(std::io::ErrorKind::InvalidData, error.to_string())
                })?;
            let logical_agent_id = match resolve_waiter_retire_target(&params, kelpie) {
                Ok(id) => id,
                Err(error) => {
                    let response = respond(&request.id, Err(error));
                    write_response(&mut stream, &response)?;
                    return Ok(Served::Answered);
                }
            };
            match kelpie.prepare_retire_waiter(logical_agent_id) {
                Ok(prepared) => {
                    return Ok(Served::AwaitingWaiterRetire(Box::new(
                        PendingWaiterRetire {
                            request_id: request.id,
                            stream,
                            logical_agent_id,
                            prepared,
                        },
                    )));
                }
                Err(error) => {
                    let response = respond(&request.id, Err(error));
                    write_response(&mut stream, &response)?;
                    return Ok(Served::Answered);
                }
            }
        }
        Ok(request) if request.method == "cancel" => match prepare_cancel(&request, kelpie) {
            Ok((outcome, prompts)) if prompts.is_empty() => {
                let response = respond(&request.id, Ok(cancel_result(outcome)));
                write_response(&mut stream, &response)?;
                return Ok(Served::Answered);
            }
            Ok((outcome, prompts)) => {
                return Ok(Served::AwaitingCancel(Box::new(PendingCancel {
                    request_id: request.id,
                    stream,
                    outcome,
                    prompts,
                })));
            }
            Err(error) => {
                let response = respond(&request.id, Err(error));
                write_response(&mut stream, &response)?;
                return Ok(Served::Answered);
            }
        },
        Ok(request) => dispatch(request, kelpie),
        Err(error) => ClientResponse {
            id: String::new(),
            result: None,
            error: Some(ClientError {
                class: "invalid_request",
                message: error.to_string(),
                code: None,
            }),
        },
    };
    write_response(&mut stream, &response)?;
    Ok(Served::Answered)
}

fn write_response(stream: &mut UnixStream, response: &ClientResponse) -> Result<(), DaemonError> {
    let _ = stream.set_write_timeout(Some(CLIENT_WRITE_TIMEOUT));
    write_json_line(stream, response)?;
    // Half-close write so line-oriented clients receive a finished response
    // even if they still wait for EOF after the NDJSON line.
    let _ = stream.shutdown(Shutdown::Write);
    Ok(())
}

fn write_json_line(
    stream: &mut UnixStream,
    value: &impl serde::Serialize,
) -> Result<(), DaemonError> {
    serde_json::to_writer(&mut *stream, value)?;
    stream.write_all(b"\n")?;
    stream.flush()?;
    Ok(())
}

fn claim_inbox(request: &ClientRequest, kelpie: &Kelpie) -> Result<LogicalAgentId, SliceError> {
    let params = serde_json::from_value::<InboxClaimParams>(request.params.clone())
        .map_err(|error| SliceError::Store(StoreError::InvalidRecord(error.to_string())))?;
    kelpie
        .store()
        .claim_socket_waiter(params.logical_agent_id)
        .map_err(SliceError::Store)?;
    Ok(params.logical_agent_id)
}

fn pump_inbox(session: &mut InboxSession, kelpie: &mut Kelpie) -> Result<bool, DaemonError> {
    let mut progressed = flush_inbox_write(session)?;
    if !session.write_buf.is_empty() {
        return Ok(progressed);
    }
    pause_after_inbox_write(session);
    progressed |= offer_queued_inbox(session, kelpie)?;
    progressed |= flush_inbox_write(session)?;
    if !session.write_buf.is_empty() {
        return Ok(true);
    }
    pause_after_inbox_write(session);
    let read = read_inbox_acks(session, kelpie);
    let flushed = flush_inbox_write(session)?;
    Ok(read? || flushed || progressed)
}

fn pause_after_inbox_write(session: &mut InboxSession) {
    if session.awaiting_write_pause && session.write_buf.is_empty() {
        crate::test_fault::pause("inbox_after_write_before_ack");
        session.awaiting_write_pause = false;
    }
}

fn enqueue_json_line(buf: &mut Vec<u8>, value: &impl serde::Serialize) -> Result<(), DaemonError> {
    serde_json::to_writer(&mut *buf, value)?;
    buf.push(b'\n');
    Ok(())
}

fn flush_inbox_write(session: &mut InboxSession) -> Result<bool, DaemonError> {
    let mut progressed = false;
    while !session.write_buf.is_empty() {
        match session.stream.write(&session.write_buf) {
            Ok(0) => {
                return Err(std::io::Error::from(std::io::ErrorKind::WriteZero).into());
            }
            Ok(n) => {
                session.write_buf.drain(..n);
                progressed = true;
            }
            Err(error)
                if error.kind() == std::io::ErrorKind::WouldBlock
                    || error.kind() == std::io::ErrorKind::Interrupted =>
            {
                return Ok(progressed);
            }
            Err(error) => return Err(error.into()),
        }
    }
    match session.stream.flush() {
        Ok(()) => Ok(progressed),
        Err(error)
            if error.kind() == std::io::ErrorKind::WouldBlock
                || error.kind() == std::io::ErrorKind::Interrupted =>
        {
            Ok(progressed)
        }
        Err(error) => Err(error.into()),
    }
}

fn offer_queued_inbox(session: &mut InboxSession, kelpie: &Kelpie) -> Result<bool, DaemonError> {
    let queued = kelpie
        .store()
        .queued_socket_inbox_deliveries(session.waiter_id)
        .map_err(|error| std::io::Error::other(format!("queued socket inbox failed: {error}")))?;
    let mut progressed = false;
    for delivery in queued {
        if session.offered.contains(&delivery.message_id) {
            continue;
        }
        crate::test_fault::pause("inbox_after_queued_before_write");
        let event = serde_json::json!({
            "id": uuid::Uuid::now_v7().to_string(),
            "method": "inbox.delivery",
            "params": {
                "message_id": delivery.message_id,
                "kind": delivery.kind,
                "body": delivery.body,
                "reply_to": delivery.reply_to,
                "disposition": delivery.disposition,
                "attempt_number": delivery.attempt_number,
                "sender_agent_id": delivery.sender_agent_id,
                "sender_public_name": delivery.sender_public_name,
            }
        });
        enqueue_json_line(&mut session.write_buf, &event)?;
        session.offered.insert(delivery.message_id);
        session.awaiting_write_pause = true;
        progressed = true;
    }
    Ok(progressed)
}

fn read_inbox_acks(session: &mut InboxSession, kelpie: &mut Kelpie) -> Result<bool, DaemonError> {
    let mut eof = false;
    let mut chunk = [0_u8; 4096];
    match session.stream.read(&mut chunk) {
        Ok(0) => eof = true,
        Ok(n) => session.read_buf.extend_from_slice(&chunk[..n]),
        Err(error)
            if error.kind() == std::io::ErrorKind::WouldBlock
                || error.kind() == std::io::ErrorKind::Interrupted => {}
        Err(error) => return Err(error.into()),
    }
    let mut progressed = false;
    while let Some(index) = session.read_buf.iter().position(|byte| *byte == b'\n') {
        let line = session.read_buf.drain(..=index).collect::<Vec<_>>();
        let line = String::from_utf8_lossy(&line);
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        match serde_json::from_str::<ClientRequest>(line) {
            Ok(request) if request.method == "inbox.ack" => {
                match ack_inbox(&request, session.waiter_id, kelpie) {
                    Ok(result) => {
                        enqueue_json_line(
                            &mut session.write_buf,
                            &respond(&request.id, Ok(result)),
                        )?;
                        progressed = true;
                    }
                    Err(error) => {
                        enqueue_json_line(
                            &mut session.write_buf,
                            &respond(&request.id, Err(error)),
                        )?;
                    }
                }
            }
            Ok(request) => {
                enqueue_json_line(
                    &mut session.write_buf,
                    &respond(
                        &request.id,
                        Err(SliceError::Store(StoreError::InvalidRecord(
                            "inbox connection accepts only inbox.ack".into(),
                        ))),
                    ),
                )?;
            }
            Err(error) => {
                enqueue_json_line(
                    &mut session.write_buf,
                    &ClientResponse {
                        id: String::new(),
                        result: None,
                        error: Some(ClientError {
                            class: "invalid_request",
                            message: error.to_string(),
                            code: None,
                        }),
                    },
                )?;
            }
        }
    }
    if eof {
        return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof).into());
    }
    Ok(progressed)
}

fn ack_inbox(
    request: &ClientRequest,
    waiter_id: LogicalAgentId,
    kelpie: &mut Kelpie,
) -> Result<Value, SliceError> {
    let params = serde_json::from_value::<InboxAckParams>(request.params.clone())
        .map_err(|error| SliceError::Store(StoreError::InvalidRecord(error.to_string())))?;
    crate::test_fault::pause("inbox_after_ack_before_resolve");
    let outcome = kelpie
        .store_mut()
        .ack_socket_inbox_delivery(waiter_id, params.message_id)
        .map_err(SliceError::Store)?;
    Ok(serde_json::json!({
        "message_id": params.message_id,
        "outcome": outcome,
    }))
}

fn settle_start_inline(
    kelpie: &mut Kelpie,
    awaiting: &mut AwaitingStart,
) -> Result<crate::slice::LaunchResult, SliceError> {
    kelpie.launch(&awaiting.intent)
}

fn prepare_start(params: Value, kelpie: &mut Kelpie) -> Result<StartIntent, SliceError> {
    let intent = serde_json::from_value::<StartIntent>(params)
        .map_err(|error| SliceError::Store(StoreError::InvalidRecord(error.to_string())))?;
    kelpie.validate_launch(&intent)?;
    kelpie.validate_handoff(&intent)?;
    Ok(intent)
}

fn launch_result(created: crate::slice::LaunchResult) -> Value {
    serde_json::json!({
        "logical_agent_id": created.logical_agent_id,
        "incarnation_id": created.incarnation_id,
        "runtime_start": {
            "operation_id": created.start_operation_id,
            "outcome": created.start_outcome
        },
        "initial_message": {
            "message_id": created.initial_message_id,
            "operation_id": created.initial_message_operation_id,
            "outcome": created.initial_message_outcome
        }
    })
}

fn dispatch(request: ClientRequest, kelpie: &mut Kelpie) -> ClientResponse {
    let result = match request.method.as_str() {
        "whoami" => dispatch_whoami(request.params, kelpie, &request.id),
        "who" => dispatch_who(request.params, kelpie),
        "recover" => dispatch_recover(kelpie),
        "start" | "handoff" => dispatch_start(request.params, kelpie),
        "adopt" => dispatch_adopt(request.params, kelpie),
        "ask" => dispatch_ask(request.params, kelpie),
        "waiter.register" => dispatch_waiter_register(request.params, kelpie),
        "waiter.retire" => dispatch_waiter_retire(request.params, kelpie),
        "tell" => dispatch_tell(request.params, kelpie),
        "renew" => dispatch_renew(request.params, kelpie),
        "renew.cancel" => serde_json::from_value::<RenewCancelParams>(request.params)
            .map_err(|error| SliceError::Store(StoreError::InvalidRecord(error.to_string())))
            .and_then(|params| {
                kelpie
                    .cancel_renew(params.renew_id, params.requester_agent_id, &params.reason)
                    .map(|notice_id| {
                        serde_json::json!({"renew_id": params.renew_id, "notice_id": notice_id})
                    })
            }),
        "reply" => dispatch_reply(request.params, kelpie),
        "pending" => dispatch_pending(request.params, kelpie),
        "name.info" => dispatch_name_info(request.params, kelpie),
        "ask.info" => dispatch_ask_info(request.params, kelpie),
        "attribution" => dispatch_attribution(request.params, kelpie),
        "report" => dispatch_report(request.params, kelpie),
        "rename" => dispatch_rename(request.params, kelpie),
        "cancel" => dispatch_cancel(request.params, kelpie),
        "reminder.snooze" => dispatch_reminder_snooze(request.params, kelpie),
        "reminder.disable" => dispatch_reminder_disable(request.params, kelpie),
        "notice.create" => serde_json::from_value::<NoticeParams>(request.params)
            .map_err(|error| SliceError::Store(StoreError::InvalidRecord(error.to_string())))
            .and_then(|params| {
                kelpie
                    .store_mut()
                    .create_operator_notice(&params.body)
                    .map(|notice_id| serde_json::json!({"notice_id": notice_id}))
                    .map_err(SliceError::Store)
            }),
        "notice.list" => kelpie
            .store_mut()
            .operator_notices()
            .map(|notices| {
                Value::Array(
                    notices
                        .into_iter()
                        .map(|notice| {
                            serde_json::json!({
                                "notice_id": notice.id,
                                "body": notice.body,
                                "created_at_ms": notice.created_at_ms,
                                "acknowledged": notice.acknowledged
                            })
                        })
                        .collect(),
                )
            })
            .map_err(SliceError::Store),
        "retire" => serde_json::from_value::<RetireParams>(request.params)
            .map_err(|error| SliceError::Store(StoreError::InvalidRecord(error.to_string())))
            .and_then(|params| {
                kelpie
                    .retire(
                        params.incarnation_id,
                        &params.idempotency_key,
                        params.close_pane,
                    )
                    .map(|(operation_id, released)| {
                        serde_json::json!({
                            "operation_id": operation_id,
                            "pane_released": released,
                        })
                    })
            }),
        _ => Err(SliceError::Store(StoreError::InvalidRecord(format!(
            "unknown client method {}",
            request.method
        )))),
    };
    respond(&request.id, result)
}

/// One correlated response, with errors classified by the SPEC taxonomy.
fn respond(request_id: &str, result: Result<Value, SliceError>) -> ClientResponse {
    match result {
        Ok(result) => ClientResponse {
            id: request_id.to_string(),
            result: Some(result),
            error: None,
        },
        Err(error) => ClientResponse {
            id: request_id.to_string(),
            result: None,
            error: Some(classify_error(&error)),
        },
    }
}

fn dispatch_start(params: Value, kelpie: &mut Kelpie) -> Result<Value, SliceError> {
    let intent = serde_json::from_value::<StartIntent>(params)
        .map_err(|error| SliceError::Store(StoreError::InvalidRecord(error.to_string())))?;
    kelpie.launch(&intent).map(launch_result)
}

fn dispatch_adopt(params: Value, kelpie: &mut Kelpie) -> Result<Value, SliceError> {
    let intent = serde_json::from_value::<AdoptIntent>(params)
        .map_err(|error| SliceError::Store(StoreError::InvalidRecord(error.to_string())))?;
    kelpie.adopt(&intent).map(|created| {
        serde_json::json!({
            "logical_agent_id": created.logical_agent_id,
            "incarnation_id": created.incarnation_id,
            "operation_id": created.operation_id,
            "outcome": "succeeded"
        })
    })
}

fn dispatch_whoami(
    params: Value,
    kelpie: &mut Kelpie,
    request_id: &str,
) -> Result<Value, SliceError> {
    let params = serde_json::from_value::<WhoamiParams>(params)
        .map_err(|error| SliceError::Store(StoreError::InvalidRecord(error.to_string())))?;
    match (params.pane_id.as_deref(), params.alias.as_deref()) {
        (Some(pane_id), None) => kelpie.resolve_or_adopt_pane(
            pane_id,
            params.lazy_adopt_key.as_deref().unwrap_or(request_id),
        ),
        (None, Some(alias)) => {
            let (logical_agent_id, incarnation_id) = kelpie.resolve_ready_alias(alias)?;
            let public_name = kelpie
                .store()
                .agent_address(logical_agent_id)
                .map_err(SliceError::Store)?;
            Ok(crate::store::ReadyIdentity {
                logical_agent_id,
                incarnation_id,
                public_name,
            })
        }
        _ => Err(SliceError::Store(StoreError::InvalidRecord(
            "provide either pane_id or alias".into(),
        ))),
    }
    .map(|identity| {
        serde_json::json!({
            "logical_agent_id": identity.logical_agent_id,
            "incarnation_id": identity.incarnation_id,
            "public_name": identity.public_name
        })
    })
}

#[derive(Debug, Clone, Copy)]
enum WhoIdentity {
    Incarnation(IncarnationId),
    SocketWaiter(LogicalAgentId),
}

fn resolve_who_identity(params: &WhoParams, kelpie: &Kelpie) -> Result<WhoIdentity, SliceError> {
    if who_selector_count(params) != 1 {
        return Err(SliceError::Store(StoreError::InvalidRecord(
            "provide exactly one of incarnation_id, agent_id, alias, or pane_id".into(),
        )));
    }
    if let Some(incarnation_id) = params.incarnation_id {
        return Ok(WhoIdentity::Incarnation(incarnation_id));
    }
    if let Some(agent_id) = params.agent_id {
        return match kelpie.store().delivery_transport(agent_id)? {
            crate::domain::DeliveryTransport::HerdrPrompt => kelpie
                .store()
                .newest_incarnation_for_agent(agent_id)
                .map(WhoIdentity::Incarnation)
                .map_err(SliceError::Store),
            crate::domain::DeliveryTransport::SocketInbox => {
                Ok(WhoIdentity::SocketWaiter(agent_id))
            }
        };
    }
    if let Some(alias) = params.alias.as_deref() {
        let ready = kelpie.store().find_ready_alias(alias)?;
        let waiter = kelpie.store().active_socket_waiter_for_alias(alias)?;
        return match (ready, waiter) {
            (Some(_), Some(_)) => Err(SliceError::Store(StoreError::Conflict(format!(
                "alias {alias} is simultaneously held by a Ready incarnation and an active socket waiter"
            )))),
            (Some((_, incarnation)), None) => Ok(WhoIdentity::Incarnation(incarnation)),
            (None, Some(agent)) => Ok(WhoIdentity::SocketWaiter(agent)),
            (None, None) => kelpie
                .store()
                .resolve_ready_alias(alias)
                .map(|(_, incarnation)| WhoIdentity::Incarnation(incarnation))
                .map_err(SliceError::Store),
        };
    }
    let pane_id = params.pane_id.as_deref().unwrap_or_default();
    kelpie
        .store()
        .ready_identity_for_pane(pane_id)
        .map(|identity| WhoIdentity::Incarnation(identity.incarnation_id))
        .map_err(|error| {
            SliceError::Store(match error {
                StoreError::Conflict(_) => StoreError::Conflict(format!(
                    "pane {pane_id} has no Ready Kelpie identity; if it has a live agent, run \
                     kelpie who from that pane to adopt it, or use kelpie adopt with its exact \
                     pane and terminal"
                )),
                other => other,
            })
        })
}

fn who_selector_count(params: &WhoParams) -> usize {
    usize::from(params.incarnation_id.is_some())
        + usize::from(params.agent_id.is_some())
        + usize::from(params.alias.is_some())
        + usize::from(params.pane_id.is_some())
}

fn who_identity_response(
    kelpie: &Kelpie,
    identity: WhoIdentity,
    reason: Option<String>,
) -> Result<Value, SliceError> {
    match identity {
        WhoIdentity::Incarnation(incarnation_id) => {
            let mut result = attribution_response(kelpie, incarnation_id, reason)?;
            result["delivery_transport"] = serde_json::json!("herdr_prompt");
            result["addressable"] =
                serde_json::json!(result["incarnation_state"].as_str() == Some("ready"));
            Ok(result)
        }
        WhoIdentity::SocketWaiter(logical_agent_id) => Ok(serde_json::json!({
            "logical_agent_id": logical_agent_id,
            "incarnation_id": null,
            "public_name": kelpie.store().agent_address(logical_agent_id)?,
            "delivery_transport": "socket_inbox",
            "addressable": kelpie.store().agent_is_addressable(logical_agent_id)?,
            "backend_kind": null,
            "incarnation_state": null,
            "requested": null,
            "observed": null,
            "observations": [],
        })),
    }
}

fn dispatch_who(params: Value, kelpie: &mut Kelpie) -> Result<Value, SliceError> {
    let params = serde_json::from_value::<WhoParams>(params)
        .map_err(|error| SliceError::Store(StoreError::InvalidRecord(error.to_string())))?;
    if params.history {
        let (Some(alias), None, None, None, false) = (
            params.alias.as_deref(),
            params.agent_id,
            params.incarnation_id,
            params.pane_id.as_deref(),
            params.refresh,
        ) else {
            return Err(SliceError::Store(StoreError::InvalidRecord(
                "who history requires exactly one alias and does not accept refresh".into(),
            )));
        };
        return dispatch_name_info(serde_json::json!({"name": alias}), kelpie);
    }
    let identity = resolve_who_identity(&params, kelpie)?;
    if params.refresh && matches!(identity, WhoIdentity::SocketWaiter(_)) {
        return Err(SliceError::Store(StoreError::InvalidRecord(
            "who --refresh requires an incarnation; socket waiters have no attribution".into(),
        )));
    }
    let reason = match identity {
        WhoIdentity::Incarnation(incarnation_id) if params.refresh => {
            kelpie.refresh_attribution(incarnation_id)?
        }
        _ => None,
    };
    who_identity_response(kelpie, identity, reason)
}

/// Re-read one ask's durable content and parties by its message id — the
/// amnesia-recovery read behind a reminder's reply-to id. Read-only.
fn dispatch_ask_info(params: Value, kelpie: &mut Kelpie) -> Result<Value, SliceError> {
    let params = serde_json::from_value::<AskInfoParams>(params)
        .map_err(|error| SliceError::Store(StoreError::InvalidRecord(error.to_string())))?;
    let info = kelpie.ask_info(params.ask_message_id)?;
    Ok(serde_json::json!({
        "ask_message_id": info.ask_message_id,
        "body": info.body,
        "asker": {
            "agent_id": info.asker_agent_id,
            "name": info.asker_name,
        },
        "responder": {
            "agent_id": info.responder_agent_id,
            "name": info.responder_name,
        },
        "state": info.state,
        "created_at_ms": info.created_at_ms,
        "last_activity_at_ms": info.last_activity_at_ms,
        "cancellation_reason": info.cancellation_reason,
        "delivery": {
            "transport": info.delivery.transport,
            "outcome": info.delivery.outcome,
            "attempt_number": info.delivery.attempt_number,
            "scheduled_at_ms": info.delivery.scheduled_at_ms,
            "attempted_at_ms": info.delivery.attempted_at_ms,
            "resolved_at_ms": info.delivery.resolved_at_ms,
        },
        "replies": info.replies.iter().map(|reply| serde_json::json!({
            "message_id": reply.message_id,
            "body": reply.body,
            "disposition": reply.disposition,
            "created_at_ms": reply.created_at_ms,
            "delivery": {
                "transport": reply.delivery.transport,
                "outcome": reply.delivery.outcome,
                "attempt_number": reply.delivery.attempt_number,
                "scheduled_at_ms": reply.delivery.scheduled_at_ms,
                "attempted_at_ms": reply.delivery.attempted_at_ms,
                "resolved_at_ms": reply.delivery.resolved_at_ms,
            }
        })).collect::<Vec<_>>(),
    }))
}

/// Report every logical agent holding a public name and every unresolved ask
/// touching them, with both parties resolved to names and liveness. Read-only:
/// this is the diagnosis behind create-new refusals, in one command.
fn dispatch_name_info(params: Value, kelpie: &mut Kelpie) -> Result<Value, SliceError> {
    let params = serde_json::from_value::<NameInfoParams>(params)
        .map_err(|error| SliceError::Store(StoreError::InvalidRecord(error.to_string())))?;
    let info = kelpie.name_info(&params.name)?;
    let claimants: Vec<Value> = info
        .claimants
        .iter()
        .map(|claimant| {
            serde_json::json!({
                "logical_agent_id": claimant.logical_agent_id,
                "created_at_ms": claimant.created_at_ms,
                "delivery_transport": claimant.delivery_transport,
                "live": claimant.has_ready_incarnation,
                "addressable": claimant.is_addressable,
                "unresolved_count": claimant.unresolved_count,
            })
        })
        .collect();
    let unresolved: Vec<Value> = info
        .unresolved
        .iter()
        .map(|obligation| {
            serde_json::json!({
                "ask_message_id": obligation.ask_message_id,
                "state": obligation.state,
                "created_at_ms": obligation.created_at_ms,
                "last_activity_at_ms": obligation.last_activity_at_ms,
                "asker": {
                    "agent_id": obligation.asker_agent_id,
                    "name": obligation.asker_name,
                    "live": obligation.asker_live,
                },
                "responder": {
                    "agent_id": obligation.responder_agent_id,
                    "name": obligation.responder_name,
                    "live": obligation.responder_live,
                },
            })
        })
        .collect();
    Ok(serde_json::json!({
        "name": info.public_name,
        "claimants": claimants,
        "unresolved": unresolved,
    }))
}

/// Report requested and observed attribution for one exact incarnation.
///
/// Requested configuration and observed execution metadata are reported under
/// separate keys and are never merged, so requested values can never be read as
/// proof of what served a turn.
fn dispatch_attribution(params: Value, kelpie: &mut Kelpie) -> Result<Value, SliceError> {
    let params = serde_json::from_value::<WhoParams>(params)
        .map_err(|error| SliceError::Store(StoreError::InvalidRecord(error.to_string())))?;
    let incarnation_id = resolve_attribution_incarnation(&params, kelpie)?;
    let reason = if params.refresh {
        kelpie.refresh_attribution(incarnation_id)?
    } else {
        None
    };
    attribution_response(kelpie, incarnation_id, reason)
}

fn resolve_attribution_incarnation(
    params: &WhoParams,
    kelpie: &Kelpie,
) -> Result<IncarnationId, SliceError> {
    let selectors = usize::from(params.incarnation_id.is_some())
        + usize::from(params.agent_id.is_some())
        + usize::from(params.alias.is_some())
        + usize::from(params.pane_id.is_some());
    if selectors != 1 {
        return Err(SliceError::Store(StoreError::InvalidRecord(
            "provide exactly one of incarnation_id, agent_id, alias, or pane_id".into(),
        )));
    }
    if let Some(incarnation_id) = params.incarnation_id {
        return Ok(incarnation_id);
    }
    if let Some(agent_id) = params.agent_id {
        return kelpie
            .store()
            .newest_incarnation_for_agent(agent_id)
            .map_err(SliceError::Store);
    }
    if let Some(alias) = params.alias.as_deref() {
        return kelpie.resolve_ready_alias(alias).map(|resolved| resolved.1);
    }
    let pane_id = params.pane_id.as_deref().unwrap_or_default();
    kelpie
        .store()
        .ready_identity_for_pane(pane_id)
        .map(|identity| identity.incarnation_id)
        .map_err(SliceError::Store)
}

fn attribution_response(
    kelpie: &Kelpie,
    incarnation_id: IncarnationId,
    reason: Option<String>,
) -> Result<Value, SliceError> {
    let evidence = kelpie.store().attribution_evidence(incarnation_id)?;
    let mut result = attribution_result(&evidence);
    if let Some(reason) = reason {
        result["undetermined_because"] = Value::String(reason);
    }
    Ok(result)
}

fn prepare_attribution_read(
    request: &ClientRequest,
    kelpie: &Kelpie,
) -> Result<Option<ClientReadKind>, SliceError> {
    let params = serde_json::from_value::<WhoParams>(request.params.clone())
        .map_err(|error| SliceError::Store(StoreError::InvalidRecord(error.to_string())))?;
    let incarnation_id = resolve_attribution_incarnation(&params, kelpie)?;
    if params.refresh && kelpie.attribution_refresh_needs_snapshot(incarnation_id)? {
        Ok(Some(ClientReadKind::Attribution { incarnation_id }))
    } else {
        Ok(None)
    }
}

fn attribution_result(evidence: &crate::store::AttributionEvidence) -> Value {
    let observation = |recorded: &crate::store::RecordedObservation| {
        serde_json::json!({
            "recorded_at_ms": recorded.recorded_at_ms,
            "adapter": recorded.observed.adapter,
            "model": recorded.observed.model,
            "provider": recorded.observed.provider,
            "effort": recorded.observed.effort,
        })
    };
    // backend_args sits inside `requested` because that is what it is: the
    // argument vector a launch asked for, stored verbatim. Kelpie does not
    // interpret it, and no backend reports whether it was honored.
    let mut requested = serde_json::to_value(&evidence.requested).unwrap_or_default();
    requested["backend_args"] = serde_json::json!(evidence.requested_backend_args);
    serde_json::json!({
        "logical_agent_id": evidence.logical_agent_id,
        "incarnation_id": evidence.incarnation_id,
        "public_name": evidence.public_name,
        "backend_kind": evidence.backend_kind,
        "incarnation_state": evidence.incarnation_state,
        "requested": requested,
        "observed": evidence.latest().map(observation),
        "observations": evidence
            .observations
            .iter()
            .map(observation)
            .collect::<Vec<_>>(),
    })
}

/// Name one agent by id or live alias, and the name it should answer to.
#[derive(Debug, Deserialize)]
struct RenameParams {
    #[serde(default)]
    agent_id: Option<LogicalAgentId>,
    #[serde(default)]
    alias: Option<String>,
    name: String,
}

/// Move a Ready agent to a new public name as one operation.
fn dispatch_rename(params: Value, kelpie: &mut Kelpie) -> Result<Value, SliceError> {
    let params = serde_json::from_value::<RenameParams>(params)
        .map_err(|error| SliceError::Store(StoreError::InvalidRecord(error.to_string())))?;
    let agent_id = match (params.agent_id, params.alias.as_deref()) {
        (Some(agent_id), None) => agent_id,
        (None, Some(alias)) => kelpie.resolve_ready_alias(alias)?.0,
        _ => {
            return Err(SliceError::Store(StoreError::InvalidRecord(
                "provide exactly one of agent_id or alias".into(),
            )));
        }
    };
    kelpie.rename(agent_id, &params.name).map(|identity| {
        serde_json::json!({
            "logical_agent_id": identity.logical_agent_id,
            "incarnation_id": identity.incarnation_id,
            "public_name": identity.public_name,
        })
    })
}

#[derive(Debug, Deserialize)]
struct ReportParams {
    /// Attach Herdr's current agent status, taken at report time.
    #[serde(default)]
    live: bool,
    /// Keep only agents that still exist, plus the lineage that explains them.
    #[serde(default)]
    active: bool,
}

/// Report every durable node and edge, optionally beside a Herdr snapshot.
///
/// Agents that still exist, plus the lineage that explains them.
///
/// "Exists" means the newest incarnation is ready, starting, or unknown.
/// Ancestors of a kept agent are kept too: a child with no parent line loses
/// the lineage that says who started it.
fn active_agents(report: &crate::store::FleetReport) -> Vec<&crate::store::ReportAgent> {
    let mut wanted: Vec<String> = report
        .agents
        .iter()
        .filter(|agent| {
            agent.incarnations.first().is_some_and(|incarnation| {
                matches!(
                    incarnation.state,
                    crate::domain::IncarnationState::Ready
                        | crate::domain::IncarnationState::Starting
                        | crate::domain::IncarnationState::Unknown
                )
            })
        })
        .map(|agent| agent.id.to_string())
        .collect();
    // Walk parents until the set stops growing. Parentage is data and can cycle,
    // so the walk is bounded by the agent count rather than trusting the shape.
    for _ in 0..report.agents.len() {
        let mut added = false;
        for agent in &report.agents {
            let Some(parent) = agent.parent_agent_id.as_ref().map(ToString::to_string) else {
                continue;
            };
            if wanted.contains(&agent.id.to_string()) && !wanted.contains(&parent) {
                wanted.push(parent);
                added = true;
            }
        }
        if !added {
            break;
        }
    }
    report
        .agents
        .iter()
        .filter(|agent| wanted.contains(&agent.id.to_string()))
        .collect()
}

fn cycle_due_at_ms(renew: &crate::store::ReportRenew) -> i64 {
    if renew.phase == crate::domain::RenewPhase::Scheduled
        && let Some(remaining) = renew.active_remaining_ms
    {
        return crate::store::store_clock_ms()
            .unwrap_or(renew.scheduled_at_ms)
            .saturating_add(remaining);
    }
    renew.scheduled_at_ms
}

fn report_incarnation(
    incarnation: &crate::store::ReportIncarnation,
    live: Option<&crate::slice::LiveStatus>,
) -> Value {
    let mut value = serde_json::json!({
                        "incarnation_id": incarnation.id,
                        "state": incarnation.state,
                        "backend_kind": incarnation.backend_kind,
                        "working_directory": incarnation.working_directory,
                        "herdr_session": incarnation.herdr_session,
                        "intended_pane_id": incarnation.intended_pane_id,
                        "expected_terminal_id": incarnation.expected_terminal_id,
                        "observed_pane_id": incarnation.observed_pane_id,
                        "observed_terminal_id": incarnation.observed_terminal_id,
                        "requested": {
                            "model": incarnation.requested.model,
                            "provider": incarnation.requested.provider,
                            "effort": incarnation.requested.effort,
                            "backend_args": incarnation.requested_backend_args,
                        },
                        "created_at_ms": incarnation.created_at_ms,
                        // Null until a rotation is observed. Never defaulted to
                        // created_at_ms: that would report the incarnation's age
                        // as the conversation's, which is wrong by exactly the
                        // amount the measurement exists to find.
                        "native_session_rotated_at_ms": incarnation.native_session_rotated_at_ms,
                        "terminal_at_ms": incarnation.terminal_at_ms,
                        "terminal_reason": incarnation.terminal_reason,
        "latest_operation": incarnation.latest_operation.as_ref().map(
            |(id, kind, outcome)| serde_json::json!({
                "operation_id": id,
                "kind": kind,
                "outcome": outcome,
            }),
        ),
        // Null means no cycle is armed. A caller cannot infer this from
        // anything else here, and for a supervised root it is the difference
        // between running and running unattended.
        "renew": incarnation.renew.as_ref().map(|renew| serde_json::json!({
            "renew_id": renew.id,
            "phase": renew.phase,
            "cycle": renew.cycle,
            "every_ms": renew.every_ms,
            // For a scheduled `--every` cycle this is remaining active occupancy
            // projected onto the wall clock, so `next-in` does not run down
            // while the incarnation is idle. For a one-shot it is the arming
            // due time. For a cycle already in flight it is when that cycle
            // left `scheduled`.
            "cycle_due_at_ms": cycle_due_at_ms(renew),
        })),
    });
    if let Some(live) = live {
        value["live"] = live
            .status_for(
                incarnation.observed_pane_id.as_deref(),
                incarnation.observed_terminal_id.as_deref(),
            )
            .map_or(Value::Null, |status| serde_json::json!(status));
    }
    value
}

/// Nothing is interpreted. States are reported as recorded, and whether one is
/// a problem is the consumer's policy rather than Kelpie's.
fn dispatch_report(params: Value, kelpie: &mut Kelpie) -> Result<Value, SliceError> {
    let params = serde_json::from_value::<ReportParams>(params)
        .map_err(|error| SliceError::Store(StoreError::InvalidRecord(error.to_string())))?;
    let live = if params.live {
        Some(kelpie.live_agent_status()?)
    } else {
        None
    };
    render_report(kelpie, params.active, live.as_ref())
}

fn render_report(
    kelpie: &Kelpie,
    active: bool,
    live: Option<&LiveStatus>,
) -> Result<Value, SliceError> {
    let report = kelpie.store().report().map_err(SliceError::Store)?;

    let keep = if active {
        active_agents(&report)
    } else {
        report.agents.iter().collect()
    };

    let agents: Vec<Value> = keep
        .iter()
        .map(|agent| {
            let incarnations: Vec<Value> = agent
                .incarnations
                .iter()
                .map(|incarnation| report_incarnation(incarnation, live))
                .collect();
            serde_json::json!({
                "agent_id": agent.id,
                "public_name": agent.public_name,
                "parent_agent_id": agent.parent_agent_id,
                "explicitly_parentless": agent.explicitly_parentless,
                "created_at_ms": agent.created_at_ms,
                "incarnations": incarnations,
            })
        })
        .collect();

    // Names shared by more than one agent. Counting identical strings is
    // arithmetic, not a verdict; whether a collision matters is caller policy.
    let mut alias_collisions = serde_json::Map::new();
    for agent in &keep {
        let shared: Vec<Value> = keep
            .iter()
            .filter(|other| other.public_name == agent.public_name)
            .map(|other| serde_json::json!(other.id))
            .collect();
        if shared.len() > 1 {
            alias_collisions.insert(agent.public_name.clone(), Value::Array(shared));
        }
    }

    let obligations: Vec<Value> = report
        .obligations
        .iter()
        .filter(|obligation| {
            !active
                || keep
                    .iter()
                    .any(|agent| agent.id == obligation.owing_agent_id)
        })
        .map(|obligation| {
            serde_json::json!({
                "ask_message_id": obligation.ask_message_id,
                "owing_agent_id": obligation.owing_agent_id,
                "waiting_agent_id": obligation.waiting_agent_id,
                "state": obligation.state,
                "created_at_ms": obligation.created_at_ms,
                "last_activity_at_ms": obligation.last_activity_at_ms,
                "resolving_message_id": obligation.resolving_message_id,
            })
        })
        .collect();

    let mut result = serde_json::json!({
        "generated_at_ms": report.generated_at_ms,
        "agents": agents,
        "obligations": obligations,
        "alias_collisions": Value::Object(alias_collisions),
    });
    if live.is_some() {
        result["live_snapshot_at_ms"] = serde_json::json!(report.generated_at_ms);
    }
    Ok(result)
}

fn recover_result(report: crate::store::RecoveryReport) -> Value {
    serde_json::json!({
        "starts_recovered": report.starts_recovered,
        "outcomes_marked_unknown": report.outcomes_marked_unknown,
        "untouched_pending_intents": report.untouched_pending_intents,
        "unattempted_clears_failed": report.unattempted_clears_failed,
        "retirements_completed": report.retirements_completed,
        "retirements_still_live": report.retirements_still_live,
        "incarnations_marked_lost": report.incarnations_marked_lost,
        "native_sessions_refreshed": report.native_sessions_refreshed
    })
}

fn dispatch_recover(kelpie: &mut Kelpie) -> Result<Value, SliceError> {
    kelpie.recover().map(recover_result)
}

fn finish_client_read(kelpie: &mut Kelpie, kind: &ClientReadKind) -> Result<Value, SliceError> {
    match kind {
        ClientReadKind::Recover => dispatch_recover(kelpie),
        ClientReadKind::Report { active } => {
            let live = kelpie.live_agent_status()?;
            render_report(kelpie, *active, Some(&live))
        }
        ClientReadKind::Whoami { pane_id, lazy_key } => kelpie
            .resolve_or_adopt_pane(pane_id, lazy_key)
            .map(|identity| {
                serde_json::json!({
                    "logical_agent_id": identity.logical_agent_id,
                    "incarnation_id": identity.incarnation_id,
                    "public_name": identity.public_name
                })
            }),
        ClientReadKind::Alias {
            alias, lazy_key, ..
        } => kelpie.resolve_or_adopt_alias(alias, lazy_key).map(
            |(logical_agent_id, incarnation_id)| {
                serde_json::json!({
                    "logical_agent_id": logical_agent_id,
                    "incarnation_id": incarnation_id,
                    "public_name": alias
                })
            },
        ),
        ClientReadKind::Attribution { incarnation_id } => {
            let reason = kelpie.refresh_attribution(*incarnation_id)?;
            attribution_response(kelpie, *incarnation_id, reason)
        }
        ClientReadKind::WhoAttribution { incarnation_id } => {
            let reason = kelpie.refresh_attribution(*incarnation_id)?;
            who_identity_response(kelpie, WhoIdentity::Incarnation(*incarnation_id), reason)
        }
        ClientReadKind::WhoPane {
            pane_id,
            lazy_key,
            refresh,
        } => {
            let identity = kelpie.resolve_or_adopt_pane(pane_id, lazy_key)?;
            let reason = if *refresh {
                kelpie.refresh_attribution(identity.incarnation_id)?
            } else {
                None
            };
            who_identity_response(
                kelpie,
                WhoIdentity::Incarnation(identity.incarnation_id),
                reason,
            )
        }
    }
}

fn prepare_who_read(
    request: &ClientRequest,
    kelpie: &Kelpie,
) -> Result<Option<ClientReadKind>, SliceError> {
    let params = serde_json::from_value::<WhoParams>(request.params.clone())
        .map_err(|error| SliceError::Store(StoreError::InvalidRecord(error.to_string())))?;
    if who_selector_count(&params) != 1 {
        return Err(SliceError::Store(StoreError::InvalidRecord(
            "provide exactly one of incarnation_id, agent_id, alias, or pane_id".into(),
        )));
    }
    if params.history {
        if params.alias.is_none()
            || params.agent_id.is_some()
            || params.incarnation_id.is_some()
            || params.pane_id.is_some()
            || params.refresh
        {
            return Err(SliceError::Store(StoreError::InvalidRecord(
                "who history requires exactly one alias and does not accept refresh".into(),
            )));
        }
        return Ok(None);
    }
    if let Some(pane_id) = params.pane_id.as_deref()
        && params.lazy_adopt_key.is_some()
        && kelpie
            .store()
            .find_ready_identity_for_pane(pane_id)?
            .is_none()
    {
        return Ok(Some(ClientReadKind::WhoPane {
            pane_id: pane_id.to_string(),
            lazy_key: params
                .lazy_adopt_key
                .clone()
                .unwrap_or_else(|| request.id.clone()),
            refresh: params.refresh,
        }));
    }
    let identity = resolve_who_identity(&params, kelpie)?;
    if let WhoIdentity::Incarnation(incarnation_id) = identity
        && params.refresh
        && kelpie.attribution_refresh_needs_snapshot(incarnation_id)?
    {
        return Ok(Some(ClientReadKind::WhoAttribution { incarnation_id }));
    }
    Ok(None)
}

fn prepare_whoami_read(
    request: &ClientRequest,
    kelpie: &Kelpie,
) -> Result<Option<ClientReadKind>, SliceError> {
    let params = serde_json::from_value::<WhoamiParams>(request.params.clone())
        .map_err(|error| SliceError::Store(StoreError::InvalidRecord(error.to_string())))?;
    match (params.pane_id.as_deref(), params.alias.as_deref()) {
        (Some(pane_id), None) => {
            if kelpie
                .store()
                .find_ready_identity_for_pane(pane_id)?
                .is_some()
            {
                Ok(None)
            } else {
                Ok(Some(ClientReadKind::Whoami {
                    pane_id: pane_id.to_string(),
                    lazy_key: params
                        .lazy_adopt_key
                        .clone()
                        .unwrap_or_else(|| request.id.clone()),
                }))
            }
        }
        (None, Some(_)) => Ok(None),
        _ => Err(SliceError::Store(StoreError::InvalidRecord(
            "provide either pane_id or alias".into(),
        ))),
    }
}

fn dispatch_waiter_register(params: Value, kelpie: &mut Kelpie) -> Result<Value, SliceError> {
    let params = serde_json::from_value::<WaiterRegisterParams>(params)
        .map_err(|error| SliceError::Store(StoreError::InvalidRecord(error.to_string())))?;
    let created = kelpie
        .store_mut()
        .register_socket_waiter(&params.public_name, params.parent, &params.idempotency_key)
        .map_err(SliceError::Store)?;
    Ok(serde_json::json!({
        "logical_agent_id": created.logical_agent_id,
        "public_name": params.public_name,
        "delivery_transport": "socket_inbox",
    }))
}

fn dispatch_waiter_retire(params: Value, kelpie: &mut Kelpie) -> Result<Value, SliceError> {
    let params = serde_json::from_value::<WaiterRetireParams>(params)
        .map_err(|error| SliceError::Store(StoreError::InvalidRecord(error.to_string())))?;
    let logical_agent_id = resolve_waiter_retire_target(&params, kelpie)?;
    let ended = kelpie.retire_waiter(logical_agent_id)?;
    Ok(waiter_retire_result(logical_agent_id, &ended))
}

fn resolve_waiter_retire_target(
    params: &WaiterRetireParams,
    kelpie: &Kelpie,
) -> Result<LogicalAgentId, SliceError> {
    match (params.logical_agent_id, params.alias.as_deref()) {
        (Some(id), None) => Ok(id),
        (None, Some(alias)) => {
            let ready = kelpie.store().find_ready_alias(alias)?;
            let waiter = kelpie.store().active_socket_waiter_for_alias(alias)?;
            match (ready, waiter) {
                (None, Some(waiter)) => Ok(waiter),
                (Some(_), Some(_)) => Err(SliceError::Store(StoreError::Conflict(format!(
                    "alias {alias} is simultaneously held by a Ready incarnation and an active socket waiter"
                )))),
                _ => Err(SliceError::Store(StoreError::Conflict(format!(
                    "no unique active socket waiter for alias {alias}"
                )))),
            }
        }
        _ => Err(SliceError::Store(StoreError::InvalidRecord(
            "provide exactly one of logical_agent_id or alias".into(),
        ))),
    }
}

fn waiter_retire_result(logical_agent_id: LogicalAgentId, ended: &WaiterRetireOutcome) -> Value {
    serde_json::json!({
        "logical_agent_id": logical_agent_id,
        "targeting_ended": true,
        "cancelled_ask_ids": ended.cancelled_ask_ids,
        "owing_notices": ended
            .owing_notices
            .iter()
            .map(|notice| {
                serde_json::json!({
                    "ask_message_id": notice.ask_message_id,
                    "message_id": notice.message_id,
                    "owing_response": if notice.delivered { "delivered" } else { "recorded" },
                })
            })
            .collect::<Vec<_>>(),
    })
}

fn write_parked_prompt(mut awaiting: AwaitingPrompt, delivered: Result<(), SliceError>) {
    let Some(mut stream) = awaiting.stream.take() else {
        return;
    };
    let response = respond(
        &awaiting.request_id,
        delivered.map(|()| awaiting.result_json.clone()),
    );
    if let Err(error) = write_response(&mut stream, &response) {
        eprintln!("kelpied: parked prompt response failed: {error}");
    }
}

fn drop_lease(lease: Option<std::sync::mpsc::Sender<LeaseCmd>>) {
    if let Some(lease) = lease {
        let _ = lease.send(LeaseCmd::Drop);
    }
}

fn fill_prompt_result(kelpie: &mut Kelpie, awaiting: &mut AwaitingPrompt, delivered_ok: bool) {
    if !delivered_ok {
        return;
    }
    if let Ok(outcome) = kelpie
        .store_mut()
        .delivery_outcome(awaiting.prepared.operation_id)
    {
        awaiting.result_json["delivery_outcome"] = serde_json::json!(outcome);
    }
    if let Some(reply_to) = awaiting.reply_to
        && let Ok(state) = kelpie.store_mut().obligation_state(reply_to)
    {
        awaiting.result_json["obligation_state"] = serde_json::json!(state);
    }
}

#[allow(clippy::too_many_lines)]
fn prepare_client_prompt(
    request: &ClientRequest,
    kelpie: &mut Kelpie,
) -> Result<(Value, Option<PreparedPrompt>, Option<MessageId>), SliceError> {
    if request.method == "ask" {
        let params = serde_json::from_value::<AskParams>(request.params.clone())
            .map_err(|error| SliceError::Store(StoreError::InvalidRecord(error.to_string())))?;
        if params.due_at_ms.is_some() {
            return Err(SliceError::Store(StoreError::InvalidRecord(
                "ask does not accept due_at_ms: an ask is delivered now. Use remind_after_ms for \
                 an unanswered ask, or tell for a message that should arrive later"
                    .into(),
            )));
        }
        let reminder_interval = ask_reminder_interval(&params)?;
        let (recipient, recipient_incarnation) = resolve_recipient(
            kelpie,
            params.recipient,
            params.recipient_incarnation,
            params.recipient_alias.as_deref(),
            &format!("{}:lazy-adopt:recipient", params.idempotency_key),
        )?;
        let (created, prepared) = kelpie.record_ask(
            params.sender,
            recipient,
            recipient_incarnation,
            &params.body,
            &params.idempotency_key,
            None,
            reminder_interval,
            params.from_operator,
        )?;
        let mut result = serde_json::json!({
            "message_id": created.message_id,
            "operation_id": created.operation_id,
            "recipient": recipient,
            "recipient_incarnation": recipient_incarnation,
            "waiting_agent_id": params.sender,
        });
        if let Some(remind_after_ms) = reminder_interval {
            result["remind_after_ms"] = serde_json::json!(remind_after_ms);
        } else {
            result["reminders"] = serde_json::json!("disabled");
        }
        if prepared.is_none() {
            result["delivery_outcome"] =
                serde_json::json!(kelpie.store_mut().delivery_outcome(created.operation_id)?);
        }
        Ok((result, prepared, None))
    } else if request.method == "tell" {
        let params = serde_json::from_value::<TellParams>(request.params.clone())
            .map_err(|error| SliceError::Store(StoreError::InvalidRecord(error.to_string())))?;
        match resolve_tell_recipient(
            kelpie,
            params.recipient,
            params.recipient_incarnation,
            params.recipient_alias.as_deref(),
            &format!("{}:lazy-adopt:recipient", params.idempotency_key),
        )? {
            TellRecipient::Herdr(recipient, recipient_incarnation) => {
                let (created, prepared) = kelpie.record_tell(
                    params.sender,
                    recipient,
                    recipient_incarnation,
                    &params.body,
                    &params.idempotency_key,
                    params.due_at_ms,
                )?;
                let mut result = serde_json::json!({
                    "message_id": created.message_id,
                    "operation_id": created.operation_id,
                    "recipient": recipient,
                    "recipient_incarnation": recipient_incarnation,
                    "delivery_transport": "herdr_prompt",
                });
                if prepared.is_none() {
                    result["delivery_outcome"] = serde_json::json!(
                        kelpie.store_mut().delivery_outcome(created.operation_id)?
                    );
                }
                if let Some(due_at_ms) = params.due_at_ms {
                    result["due_at_ms"] = serde_json::json!(due_at_ms);
                }
                Ok((result, prepared, None))
            }
            TellRecipient::SocketInbox(recipient) => {
                let created = kelpie.record_socket_tell(
                    params.sender,
                    recipient,
                    &params.body,
                    &params.idempotency_key,
                    params.due_at_ms,
                )?;
                let mut result = serde_json::json!({
                    "message_id": created.message_id,
                    "operation_id": null,
                    "recipient": recipient,
                    "recipient_incarnation": null,
                    "delivery_transport": "socket_inbox",
                    "delivery_outcome": kelpie
                        .store_mut()
                        .delivery_outcome_for_message(created.message_id)?,
                });
                if let Some(due_at_ms) = params.due_at_ms {
                    result["due_at_ms"] = serde_json::json!(due_at_ms);
                }
                Ok((result, None, None))
            }
        }
    } else {
        let params = serde_json::from_value::<ReplyParams>(request.params.clone())
            .map_err(|error| SliceError::Store(StoreError::InvalidRecord(error.to_string())))?;
        let (created, prepared) = kelpie.record_reply(
            params.reply_to,
            params.requester_agent_id,
            &params.body,
            params.disposition,
            &params.idempotency_key,
        )?;
        let mut result = serde_json::json!({
            "message_id": created.message_id,
            "operation_id": created.operation_id,
            "recipient_incarnation": created.recipient_incarnation,
            "disposition": created.disposition,
        });
        if prepared.is_none() {
            let delivery_outcome = match created.operation_id {
                Some(operation_id) => kelpie.store_mut().delivery_outcome(operation_id)?,
                None => kelpie
                    .store_mut()
                    .delivery_outcome_for_message(created.message_id)?,
            };
            result["delivery_outcome"] = serde_json::json!(delivery_outcome);
            result["obligation_state"] =
                serde_json::json!(kelpie.store_mut().obligation_state(params.reply_to)?);
        }
        Ok((result, prepared, Some(params.reply_to)))
    }
}

fn dispatch_ask(params: Value, kelpie: &mut Kelpie) -> Result<Value, SliceError> {
    let params = serde_json::from_value::<AskParams>(params)
        .map_err(|error| SliceError::Store(StoreError::InvalidRecord(error.to_string())))?;
    if params.due_at_ms.is_some() {
        // A postponed ask creates an obligation the recipient cannot see: owed
        // on the server, absent from their pane, and indistinguishable from a
        // delivered ask nobody answered. Refuse rather than accept a request
        // whose two halves disagree about when the work was requested.
        return Err(SliceError::Store(StoreError::InvalidRecord(
            "ask does not accept due_at_ms: an ask is delivered now. Use remind_after_ms for \
             an unanswered ask, or tell for a message that should arrive later"
                .into(),
        )));
    }
    let reminder_interval = ask_reminder_interval(&params)?;
    let (recipient, recipient_incarnation) = resolve_recipient(
        kelpie,
        params.recipient,
        params.recipient_incarnation,
        params.recipient_alias.as_deref(),
        &format!("{}:lazy-adopt:recipient", params.idempotency_key),
    )?;
    let created = kelpie.ask(
        params.sender,
        recipient,
        recipient_incarnation,
        &params.body,
        &params.idempotency_key,
        None,
        reminder_interval,
        params.from_operator,
    )?;
    let delivery_outcome = kelpie
        .store_mut()
        .delivery_outcome(created.operation_id)
        .map_err(SliceError::Store)?;
    let mut result = serde_json::json!({
        "message_id": created.message_id,
        "operation_id": created.operation_id,
        "recipient": recipient,
        "recipient_incarnation": recipient_incarnation,
        "waiting_agent_id": params.sender,
        "delivery_outcome": delivery_outcome
    });
    if let Some(due_at_ms) = params.due_at_ms {
        result["due_at_ms"] = serde_json::json!(due_at_ms);
    }
    if let Some(remind_after_ms) = reminder_interval {
        result["remind_after_ms"] = serde_json::json!(remind_after_ms);
    } else {
        result["reminders"] = serde_json::json!("disabled");
    }
    Ok(result)
}

fn ask_reminder_interval(params: &AskParams) -> Result<Option<i64>, SliceError> {
    if params.no_remind && params.remind_after_ms.is_some() {
        return Err(SliceError::Store(StoreError::InvalidRecord(
            "ask accepts only one of remind_after_ms or no_remind".into(),
        )));
    }
    Ok((!params.no_remind).then_some(
        params
            .remind_after_ms
            .unwrap_or(DEFAULT_REMINDER_INTERVAL_MS),
    ))
}

fn dispatch_tell(params: Value, kelpie: &mut Kelpie) -> Result<Value, SliceError> {
    let params = serde_json::from_value::<TellParams>(params)
        .map_err(|error| SliceError::Store(StoreError::InvalidRecord(error.to_string())))?;
    let mut result = match resolve_tell_recipient(
        kelpie,
        params.recipient,
        params.recipient_incarnation,
        params.recipient_alias.as_deref(),
        &format!("{}:lazy-adopt:recipient", params.idempotency_key),
    )? {
        TellRecipient::Herdr(recipient, recipient_incarnation) => {
            let created = kelpie.tell(
                params.sender,
                recipient,
                recipient_incarnation,
                &params.body,
                &params.idempotency_key,
                params.due_at_ms,
            )?;
            let delivery_outcome = kelpie
                .store_mut()
                .delivery_outcome(created.operation_id)
                .map_err(SliceError::Store)?;
            serde_json::json!({
                "message_id": created.message_id,
                "operation_id": created.operation_id,
                "recipient": recipient,
                "recipient_incarnation": recipient_incarnation,
                "delivery_transport": "herdr_prompt",
                "delivery_outcome": delivery_outcome
            })
        }
        TellRecipient::SocketInbox(recipient) => {
            let created = kelpie.record_socket_tell(
                params.sender,
                recipient,
                &params.body,
                &params.idempotency_key,
                params.due_at_ms,
            )?;
            let delivery_outcome = kelpie
                .store_mut()
                .delivery_outcome_for_message(created.message_id)
                .map_err(SliceError::Store)?;
            serde_json::json!({
                "message_id": created.message_id,
                "operation_id": null,
                "recipient": recipient,
                "recipient_incarnation": null,
                "delivery_transport": "socket_inbox",
                "delivery_outcome": delivery_outcome
            })
        }
    };
    if let Some(due_at_ms) = params.due_at_ms {
        result["due_at_ms"] = serde_json::json!(due_at_ms);
    }
    Ok(result)
}

/// When a renew's first cycle comes due.
///
/// An explicit due time is obeyed. Otherwise `--every 45m` means the first
/// cycle is 45 minutes of observed `working`/`blocked` occupancy away, not now:
/// an agent arms a policy once it has read itself in, so clearing on arming
/// would discard exactly the context that was just paid for and re-read it a
/// minute later. A one-shot with no due time is a request to renew now, and
/// stays that way.
fn first_cycle_at_ms(due_at_ms: Option<i64>, every_ms: Option<i64>, now_ms: i64) -> i64 {
    match (due_at_ms, every_ms) {
        (Some(due_at_ms), _) => due_at_ms,
        (None, Some(every_ms)) => now_ms.saturating_add(every_ms),
        (None, None) => now_ms,
    }
}

fn begin_clear(params: Value, kelpie: &mut Kelpie) -> Result<ClearDispatch, SliceError> {
    let params = serde_json::from_value::<ClearParams>(params)
        .map_err(|error| SliceError::Store(StoreError::InvalidRecord(error.to_string())))?;
    let (recipient, recipient_incarnation) = resolve_recipient(
        kelpie,
        params.recipient,
        params.recipient_incarnation,
        params.recipient_alias.as_deref(),
        &format!("{}:lazy-adopt:recipient", params.idempotency_key),
    )?;
    let not_before_ms = kelpie.clear_not_before_ms(recipient_incarnation)?;
    Ok(ClearDispatch::Awaiting(AwaitingClearState::Settling {
        clear: ResolvedClear {
            recipient,
            recipient_incarnation,
            idempotency_key: params.idempotency_key,
        },
        not_before_ms,
    }))
}

fn advance_clear_state(
    state: AwaitingClearState,
    kelpie: &mut Kelpie,
) -> Result<ClearDispatch, SliceError> {
    match state {
        AwaitingClearState::Settling {
            clear,
            not_before_ms,
        } => {
            let latest_not_before_ms = kelpie.clear_not_before_ms(clear.recipient_incarnation)?;
            let not_before_ms = not_before_ms.max(latest_not_before_ms);
            if crate::store::store_clock_ms().map_err(SliceError::Store)? < not_before_ms {
                return Ok(ClearDispatch::Awaiting(AwaitingClearState::Settling {
                    clear,
                    not_before_ms,
                }));
            }
            match kelpie.submit_clear(
                clear.recipient,
                clear.recipient_incarnation,
                &clear.idempotency_key,
            )? {
                ClearSubmission::Complete(result) => Ok(ClearDispatch::Complete(result)),
                ClearSubmission::Awaiting(awaiting) => Ok(ClearDispatch::Awaiting(
                    AwaitingClearState::Rotation(awaiting),
                )),
            }
        }
        AwaitingClearState::Rotation(awaiting) => match kelpie.advance_clear(&awaiting)? {
            Some(result) => Ok(ClearDispatch::Complete(result)),
            None => Ok(ClearDispatch::Awaiting(AwaitingClearState::Rotation(
                awaiting,
            ))),
        },
        AwaitingClearState::Probe { .. } | AwaitingClearState::Sending { .. } => {
            Ok(ClearDispatch::Awaiting(state))
        }
    }
}

fn clear_result(cleared: ClearResult) -> Value {
    serde_json::json!({
        "operation_id": cleared.operation_id,
        "recipient": cleared.recipient,
        "recipient_incarnation": cleared.recipient_incarnation,
        "outcome": cleared.outcome,
    })
}

fn dispatch_renew(params: Value, kelpie: &mut Kelpie) -> Result<Value, SliceError> {
    let params = serde_json::from_value::<RenewParams>(params)
        .map_err(|error| SliceError::Store(StoreError::InvalidRecord(error.to_string())))?;
    // Exact IDs only. Every other operation may take an alias because the worst
    // an alias costs them is one misdelivered message; a renew aimed at the
    // wrong agent clears its conversation once a cycle and cannot be undone
    // from the outside, so the target is never resolved from a live name here.
    let (Some(recipient), Some(recipient_incarnation)) =
        (params.recipient, params.recipient_incarnation)
    else {
        return Err(SliceError::Store(StoreError::InvalidRecord(
            "renew requires recipient and recipient_incarnation; it does not resolve an alias, \
             because a policy aimed at the wrong agent clears its conversation every cycle"
                .into(),
        )));
    };
    if params.every_ms.is_some() && params.due_at_ms.is_some() {
        return Err(SliceError::Store(StoreError::InvalidRecord(
            "renew accepts either a one-shot due time or every_ms, not both: every_ms re-arms \
             itself after each cycle"
                .into(),
        )));
    }
    let scheduled_at_ms = first_cycle_at_ms(
        params.due_at_ms,
        params.every_ms,
        crate::store::store_clock_ms().map_err(SliceError::Store)?,
    );
    let renew_id = kelpie.renew(
        params.requester,
        recipient,
        recipient_incarnation,
        &params.prepare_prompt,
        &params.prompt,
        params.on_timeout,
        params.prepare_timeout_ms,
        params.every_ms,
        scheduled_at_ms,
    )?;
    let mut result = serde_json::json!({
        "renew_id": renew_id,
        "recipient": recipient,
        "recipient_incarnation": recipient_incarnation,
        "scheduled_at_ms": scheduled_at_ms,
        "on_timeout": params.on_timeout,
        "phase": "scheduled"
    });
    if let Some(every_ms) = params.every_ms {
        result["every_ms"] = serde_json::json!(every_ms);
    }
    Ok(result)
}

fn pending_alias_bind(request: &ClientRequest, kelpie: &Kelpie) -> Option<(String, String)> {
    let (alias, idempotency_key) = match request.method.as_str() {
        "ask" => {
            let params = serde_json::from_value::<AskParams>(request.params.clone()).ok()?;
            if params.recipient.is_some()
                || params.recipient_incarnation.is_some()
                || params.due_at_ms.is_some()
            {
                return None;
            }
            (params.recipient_alias?, params.idempotency_key)
        }
        "tell" => {
            let params = serde_json::from_value::<TellParams>(request.params.clone()).ok()?;
            if params.recipient.is_some() || params.recipient_incarnation.is_some() {
                return None;
            }
            (params.recipient_alias?, params.idempotency_key)
        }
        "clear" => {
            let params = serde_json::from_value::<ClearParams>(request.params.clone()).ok()?;
            if params.recipient.is_some() || params.recipient_incarnation.is_some() {
                return None;
            }
            (params.recipient_alias?, params.idempotency_key)
        }
        _ => return None,
    };
    if request.method == "tell"
        && matches!(
            kelpie.store().active_socket_waiter_for_alias(&alias),
            Ok(Some(_))
        )
    {
        return None;
    }
    matches!(kelpie.store().find_ready_alias(&alias), Ok(None))
        .then(|| (alias, format!("{idempotency_key}:lazy-adopt:recipient")))
}

#[derive(Debug, Clone, Copy)]
enum TellRecipient {
    Herdr(LogicalAgentId, IncarnationId),
    SocketInbox(LogicalAgentId),
}

fn resolve_tell_recipient(
    kelpie: &mut Kelpie,
    recipient: Option<LogicalAgentId>,
    recipient_incarnation: Option<IncarnationId>,
    recipient_alias: Option<&str>,
    lazy_adopt_key: &str,
) -> Result<TellRecipient, SliceError> {
    match (recipient, recipient_incarnation, recipient_alias) {
        (Some(recipient), Some(incarnation), None) => {
            Ok(TellRecipient::Herdr(recipient, incarnation))
        }
        (None, None, Some(alias)) => {
            let ready = kelpie.store().find_ready_alias(alias)?;
            let waiter = kelpie.store().active_socket_waiter_for_alias(alias)?;
            match (ready, waiter) {
                (Some(_), Some(_)) => Err(SliceError::Store(StoreError::Conflict(format!(
                    "alias {alias} is simultaneously held by a Ready incarnation and an active socket waiter"
                )))),
                (Some((agent, incarnation)), None) => Ok(TellRecipient::Herdr(agent, incarnation)),
                (None, Some(agent)) => Ok(TellRecipient::SocketInbox(agent)),
                (None, None) => kelpie
                    .resolve_or_adopt_alias(alias, lazy_adopt_key)
                    .map(|(agent, incarnation)| TellRecipient::Herdr(agent, incarnation)),
            }
        }
        _ => Err(SliceError::Store(StoreError::InvalidRecord(
            "provide either exact recipient and recipient_incarnation, or recipient_alias".into(),
        ))),
    }
}

fn resolve_recipient(
    kelpie: &mut Kelpie,
    recipient: Option<LogicalAgentId>,
    recipient_incarnation: Option<IncarnationId>,
    recipient_alias: Option<&str>,
    lazy_adopt_key: &str,
) -> Result<(LogicalAgentId, IncarnationId), SliceError> {
    match (recipient, recipient_incarnation, recipient_alias) {
        (Some(recipient), Some(incarnation), None) => Ok((recipient, incarnation)),
        (None, None, Some(alias)) => kelpie.resolve_or_adopt_alias(alias, lazy_adopt_key),
        _ => Err(SliceError::Store(StoreError::InvalidRecord(
            "provide either exact recipient and recipient_incarnation, or recipient_alias".into(),
        ))),
    }
}

fn dispatch_reply(params: Value, kelpie: &mut Kelpie) -> Result<Value, SliceError> {
    let params = serde_json::from_value::<ReplyParams>(params)
        .map_err(|error| SliceError::Store(StoreError::InvalidRecord(error.to_string())))?;
    let created = kelpie.reply(
        params.reply_to,
        params.requester_agent_id,
        &params.body,
        params.disposition,
        &params.idempotency_key,
    )?;
    let delivery_outcome = match created.operation_id {
        Some(operation_id) => kelpie
            .store_mut()
            .delivery_outcome(operation_id)
            .map_err(SliceError::Store)?,
        None => kelpie
            .store_mut()
            .delivery_outcome_for_message(created.message_id)
            .map_err(SliceError::Store)?,
    };
    let obligation_state = kelpie
        .store_mut()
        .obligation_state(params.reply_to)
        .map_err(SliceError::Store)?;
    Ok(serde_json::json!({
        "message_id": created.message_id,
        "operation_id": created.operation_id,
        "recipient_incarnation": created.recipient_incarnation,
        "disposition": created.disposition,
        "delivery_outcome": delivery_outcome,
        "obligation_state": obligation_state
    }))
}

fn dispatch_pending(params: Value, kelpie: &Kelpie) -> Result<Value, SliceError> {
    let params = serde_json::from_value::<PendingParams>(params)
        .map_err(|error| SliceError::Store(StoreError::InvalidRecord(error.to_string())))?;
    let cancelled = kelpie.cancelled_while_away(params.agent_id)?;
    let cancelled_owing = kelpie.cancelled_owing_while_away(params.agent_id)?;
    let pending = kelpie.pending(params.agent_id)?;
    let mut obligations: Vec<Value> = pending
        .into_iter()
        .map(|obligation| {
            serde_json::json!({
                "ask_message_id": obligation.ask_message_id,
                "waiting_agent_id": obligation.waiting_agent_id,
                "state": obligation.state
            })
        })
        .collect();
    // What happened to this agent's waits while it had no Ready
    // incarnation: the first check after revival sees it.
    obligations.extend(cancelled.into_iter().map(|entry| {
        serde_json::json!({
            "ask_message_id": entry.ask_message_id,
            "state": "cancelled",
            "audience": "waiting",
            "cancellation_reason": entry.reason,
            "cancellation_requester_agent_id": entry.cancelled_by,
            "cancelled_at_ms": entry.cancelled_at_ms,
        })
    }));
    // Asks this agent was answering, cancelled while it had no Ready
    // binding: the stop-notice it never received.
    obligations.extend(cancelled_owing.into_iter().map(|entry| {
        serde_json::json!({
            "ask_message_id": entry.ask_message_id,
            "state": "cancelled",
            "audience": "owing",
            "cancellation_reason": entry.reason,
            "cancellation_requester_agent_id": entry.cancelled_by,
            "cancelled_at_ms": entry.cancelled_at_ms,
        })
    }));
    Ok(Value::Array(obligations))
}

fn prepare_cancel(
    request: &ClientRequest,
    kelpie: &mut Kelpie,
) -> Result<(CancelOutcome, Vec<PreparedCancellation>), SliceError> {
    let params = serde_json::from_value::<CancelParams>(request.params.clone())
        .map_err(|error| SliceError::Store(StoreError::InvalidRecord(error.to_string())))?;
    kelpie.record_cancel(
        params.requester_agent_id,
        params.ask_message_id,
        &params.reason,
    )
}

fn cancel_result(outcome: CancelOutcome) -> Value {
    serde_json::json!({
        "state": "cancelled",
        "response": if outcome.delivered { "delivered" } else { "recorded" },
        "message_id": outcome.message_id.map(|id| id.to_string()),
        "owing_response": if outcome.owing_delivered { "delivered" } else { "recorded" },
        "owing_message_id": outcome.owing_message_id.map(|id| id.to_string()),
    })
}

fn dispatch_cancel(params: Value, kelpie: &mut Kelpie) -> Result<Value, SliceError> {
    let params = serde_json::from_value::<CancelParams>(params)
        .map_err(|error| SliceError::Store(StoreError::InvalidRecord(error.to_string())))?;
    kelpie
        .cancel(
            params.requester_agent_id,
            params.ask_message_id,
            &params.reason,
        )
        .map(|outcome| {
            serde_json::json!({
                "state": "cancelled",
                "response": if outcome.delivered { "delivered" } else { "recorded" },
                "message_id": outcome.message_id.map(|id| id.to_string()),
                "owing_response": if outcome.owing_delivered { "delivered" } else { "recorded" },
                "owing_message_id": outcome.owing_message_id.map(|id| id.to_string()),
            })
        })
}

fn dispatch_reminder_snooze(params: Value, kelpie: &mut Kelpie) -> Result<Value, SliceError> {
    let params = serde_json::from_value::<ReminderSnoozeParams>(params)
        .map_err(|error| SliceError::Store(StoreError::InvalidRecord(error.to_string())))?;
    kelpie.snooze_reminder(
        params.requester_agent_id,
        params.ask_message_id,
        params.until_ms,
    )?;
    Ok(serde_json::json!({"state": "snoozed", "until_ms": params.until_ms}))
}

fn dispatch_reminder_disable(params: Value, kelpie: &mut Kelpie) -> Result<Value, SliceError> {
    let params = serde_json::from_value::<ReminderDisableParams>(params)
        .map_err(|error| SliceError::Store(StoreError::InvalidRecord(error.to_string())))?;
    kelpie.disable_reminder(params.requester_agent_id, params.ask_message_id)?;
    Ok(serde_json::json!({"state": "disabled"}))
}

fn classify_error(error: &SliceError) -> ClientError {
    let class = match error {
        SliceError::PaneOccupied { .. }
        | SliceError::LiveConflict(_)
        | SliceError::Store(StoreError::Conflict(_))
        | SliceError::ClearUnproven { .. } => "conflict",
        SliceError::Store(StoreError::InvalidRecord(_)) => "invalid_request",
        SliceError::Store(StoreError::UnsafeLocation(_) | StoreError::Sql(_))
        | SliceError::Herdr(HerdrError::Malformed(_) | HerdrError::Unexpected(_)) => "internal",
        SliceError::Herdr(HerdrError::Incompatible { .. })
        | SliceError::UnsupportedBackend { .. } => "incompatible_runtime",
        SliceError::Herdr(HerdrError::Unavailable(_)) => "unavailable",
        SliceError::Herdr(HerdrError::Rejected { code, .. }) if code.contains("not_found") => {
            "target_unavailable"
        }
        SliceError::Herdr(HerdrError::Rejected { .. }) => "rejected",
        SliceError::Herdr(HerdrError::ReadinessTimeout(_))
        | SliceError::ClearRotationTimeout { .. } => "timeout",
        SliceError::UnknownOutcome { .. } => "unknown_outcome",
    };
    // Kelpie-owned codes are decided here; Herdr's own code is passed through
    // unaltered so a caller branches on evidence rather than on prose.
    let code = match error {
        SliceError::PaneOccupied { .. } => Some("pane_occupied".to_string()),
        SliceError::UnsupportedBackend { .. } => Some("renew_unsupported_backend".to_string()),
        SliceError::ClearRotationTimeout { .. } => Some("clear_rotation_timeout".to_string()),
        SliceError::ClearUnproven { .. } => Some("clear_unproven".to_string()),
        SliceError::Herdr(HerdrError::Rejected { code, .. })
        | SliceError::UnknownOutcome {
            source: HerdrError::Rejected { code, .. },
            ..
        } => Some(code.clone()),
        _ => None,
    };
    ClientError {
        class,
        message: error.to_string(),
        code,
    }
}

#[derive(Debug, Deserialize)]
struct WaiterRegisterParams {
    public_name: String,
    parent: Parent,
    idempotency_key: String,
}

#[derive(Debug, Deserialize)]
struct WaiterRetireParams {
    #[serde(default)]
    logical_agent_id: Option<LogicalAgentId>,
    #[serde(default)]
    alias: Option<String>,
}

#[derive(Debug, Deserialize)]
struct InboxClaimParams {
    logical_agent_id: LogicalAgentId,
}

#[derive(Debug, Deserialize)]
struct InboxAckParams {
    message_id: MessageId,
}

#[derive(Debug, Deserialize)]
struct AskParams {
    sender: LogicalAgentId,
    #[serde(default)]
    recipient: Option<LogicalAgentId>,
    #[serde(default)]
    recipient_incarnation: Option<IncarnationId>,
    /// Resolve the current Ready agent for this public-name alias at send time.
    #[serde(default)]
    recipient_alias: Option<String>,
    body: String,
    idempotency_key: String,
    /// Rejected when present. Kept in the shape so an old caller gets a stable
    /// error instead of having its scheduling silently ignored.
    #[serde(default)]
    due_at_ms: Option<i64>,
    #[serde(default)]
    remind_after_ms: Option<i64>,
    #[serde(default)]
    no_remind: bool,
    /// Message-sender attribution only. The waiting agent is still `sender`.
    #[serde(default)]
    from_operator: bool,
}

#[derive(Debug, Deserialize)]
struct ReminderSnoozeParams {
    requester_agent_id: LogicalAgentId,
    ask_message_id: MessageId,
    until_ms: i64,
}

#[derive(Debug, Deserialize)]
struct ReminderDisableParams {
    requester_agent_id: LogicalAgentId,
    ask_message_id: MessageId,
}

#[derive(Debug, Deserialize)]
struct TellParams {
    sender: LogicalAgentId,
    #[serde(default)]
    recipient: Option<LogicalAgentId>,
    #[serde(default)]
    recipient_incarnation: Option<IncarnationId>,
    /// Resolve the current Ready agent for this public-name alias at send time.
    #[serde(default)]
    recipient_alias: Option<String>,
    body: String,
    idempotency_key: String,
    #[serde(default)]
    due_at_ms: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct ClearParams {
    #[serde(default)]
    recipient: Option<LogicalAgentId>,
    #[serde(default)]
    recipient_incarnation: Option<IncarnationId>,
    #[serde(default)]
    recipient_alias: Option<String>,
    idempotency_key: String,
}

#[derive(Debug, Deserialize)]
struct RenewParams {
    requester: LogicalAgentId,
    #[serde(default)]
    recipient: Option<LogicalAgentId>,
    #[serde(default)]
    recipient_incarnation: Option<IncarnationId>,
    /// Delivered as an ask; its final reply is the ready signal.
    prepare_prompt: String,
    /// Injected after the clear. With `every_ms` this runs on every cycle for
    /// the life of the agent, so it must be reentrant.
    prompt: String,
    /// Required. There is no safe default disposition for a prepare timeout.
    on_timeout: RenewTimeout,
    prepare_timeout_ms: i64,
    #[serde(default)]
    every_ms: Option<i64>,
    #[serde(default)]
    due_at_ms: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct RenewCancelParams {
    renew_id: RenewId,
    /// The agent asking. Must be the policy's requester or its target.
    requester_agent_id: LogicalAgentId,
    reason: String,
}

#[derive(Debug, Deserialize)]
struct ReplyParams {
    reply_to: MessageId,
    requester_agent_id: LogicalAgentId,
    body: String,
    disposition: ReplyDisposition,
    idempotency_key: String,
}

#[derive(Debug, Deserialize)]
struct WhoamiParams {
    #[serde(default)]
    pane_id: Option<String>,
    #[serde(default)]
    alias: Option<String>,
    #[serde(default)]
    lazy_adopt_key: Option<String>,
}

#[derive(Debug, Deserialize)]
struct PendingParams {
    agent_id: LogicalAgentId,
}

#[derive(Debug, Deserialize)]
struct AskInfoParams {
    ask_message_id: MessageId,
}

#[derive(Debug, Deserialize)]
struct NameInfoParams {
    name: String,
}

/// Unified identity selector used by `who`.
#[derive(Debug, Deserialize)]
struct WhoParams {
    #[serde(default)]
    incarnation_id: Option<IncarnationId>,
    #[serde(default)]
    agent_id: Option<LogicalAgentId>,
    #[serde(default)]
    alias: Option<String>,
    #[serde(default)]
    pane_id: Option<String>,
    #[serde(default)]
    history: bool,
    #[serde(default)]
    refresh: bool,
    #[serde(default)]
    lazy_adopt_key: Option<String>,
}

#[derive(Debug, Deserialize)]
struct CancelParams {
    requester_agent_id: LogicalAgentId,
    ask_message_id: MessageId,
    reason: String,
}

#[derive(Debug, Deserialize)]
struct NoticeParams {
    body: String,
}

#[derive(Debug, Deserialize)]
struct RetireParams {
    incarnation_id: IncarnationId,
    idempotency_key: String,
    /// Release the pane too. Opt-in, because it ends a live process.
    #[serde(default)]
    close_pane: bool,
}

#[cfg(test)]
mod tests {
    use std::io::{BufRead, BufReader, Write};
    use std::os::unix::net::{UnixListener, UnixStream};
    use std::thread;
    use std::time::{Duration, Instant};

    use crate::domain::{InitialMessageIntent, InitialMessageKind, Parent, StartIntent};
    use crate::herdr::HerdrClient;
    use crate::store::Store;

    use super::*;

    #[test]
    fn a_policy_waits_out_its_own_interval_before_its_first_cycle() {
        let every_ms = 45 * 60 * 1_000;
        assert_eq!(
            first_cycle_at_ms(None, Some(every_ms), 1_000),
            1_000 + every_ms,
            "arming a policy is not a request to clear right now"
        );
        // A one-shot with no due time still means now. An explicit due time is
        // a one-shot; combining it with `--every` is refused before this runs.
        assert_eq!(first_cycle_at_ms(None, None, 1_000), 1_000);
        assert_eq!(first_cycle_at_ms(Some(50), None, 1_000), 50);
    }

    fn ask_params(remind_after_ms: Option<i64>, no_remind: bool) -> AskParams {
        AskParams {
            sender: LogicalAgentId::new(),
            recipient: None,
            recipient_incarnation: None,
            recipient_alias: Some("worker".into()),
            body: "question".into(),
            idempotency_key: "ask".into(),
            due_at_ms: None,
            remind_after_ms,
            no_remind,
            from_operator: false,
        }
    }

    #[test]
    fn asks_default_to_five_minute_reminders_with_explicit_override_and_opt_out() {
        assert_eq!(
            ask_reminder_interval(&ask_params(None, false)).expect("default"),
            Some(300_000)
        );
        assert_eq!(
            ask_reminder_interval(&ask_params(Some(600_000), false)).expect("override"),
            Some(600_000)
        );
        assert_eq!(
            ask_reminder_interval(&ask_params(None, true)).expect("disabled"),
            None
        );
        assert!(ask_reminder_interval(&ask_params(Some(1), true)).is_err());
    }

    fn test_intent(name: &str, terminal: &str, key: &str) -> StartIntent {
        StartIntent {
            public_name: name.into(),
            logical_agent_id: None,
            parent: Parent::Parentless,
            herdr_session: "test".into(),
            pane_id: "w1:p1".into(),
            expected_terminal_id: terminal.into(),
            backend_kind: "codex".into(),
            backend_args: vec![],
            initial_message: InitialMessageIntent {
                sender: None,
                kind: InitialMessageKind::Tell,
                body: "work".into(),
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

    fn send_request(socket: &Path, request: &Value) -> Value {
        let mut stream = UnixStream::connect(socket).expect("connect client");
        serde_json::to_writer(&mut stream, request).expect("write request");
        stream.write_all(b"\n").expect("finish request");
        let mut line = String::new();
        BufReader::new(stream)
            .read_line(&mut line)
            .expect("read response");
        serde_json::from_str(&line).expect("response JSON")
    }

    #[test]
    fn attribution_reports_requested_and_observed_without_merging_them() {
        let directory = tempfile::tempdir().expect("tempdir");
        let socket = directory.path().join("kelpie.sock");
        let mut store = Store::in_memory().expect("store");
        let mut intent = test_intent("reviewer", "term-a", "attr-socket");
        intent.requested_model = Some("requested-only".into());
        let declared = store.declare_start(&intent).expect("declare");
        let session = serde_json::json!({"agent":"grok","kind":"id","value":"sess-1"});
        store
            .record_observed_attribution(
                declared.incarnation_id,
                Some(&session),
                &crate::attribution::observe(
                    "grok",
                    Some(&session),
                    &crate::attribution::SessionRoots::default(),
                ),
            )
            .expect("observe");
        let kelpie = Kelpie::new(
            store,
            HerdrClient::new(
                directory.path().join("unused-herdr.sock"),
                Duration::from_secs(1),
            ),
        );
        let mut daemon = Daemon::bind(&socket, kelpie).expect("bind daemon");
        let server = thread::spawn(move || {
            for _ in 0..3 {
                daemon.serve_one().expect("serve request");
            }
        });

        let response = send_request(
            &socket,
            &serde_json::json!({
                "id": "attr-1",
                "method": "attribution",
                "params": {"incarnation_id": declared.incarnation_id},
            }),
        );
        let result = &response["result"];
        assert_eq!(result["public_name"], "reviewer");
        assert_eq!(result["backend_kind"], "codex");
        // Requested stays in its own object and never becomes observed evidence.
        assert_eq!(result["requested"]["model"], "requested-only");
        assert_eq!(result["observed"]["adapter"], "grok");
        assert_eq!(result["observed"]["model"]["status"], "undetermined");
        assert!(result["observed"]["model"].get("value").is_none());
        assert_eq!(result["observations"].as_array().expect("history").len(), 1);

        // The same agent id resolves to its newest incarnation.
        let by_agent = send_request(
            &socket,
            &serde_json::json!({
                "id": "attr-2",
                "method": "attribution",
                "params": {"agent_id": declared.logical_agent_id},
            }),
        );
        assert_eq!(
            by_agent["result"]["incarnation_id"],
            result["incarnation_id"]
        );

        // Two selectors are ambiguous and fail closed before any lookup.
        let ambiguous = send_request(
            &socket,
            &serde_json::json!({
                "id": "attr-3",
                "method": "attribution",
                "params": {
                    "agent_id": declared.logical_agent_id,
                    "incarnation_id": declared.incarnation_id,
                },
            }),
        );
        assert_eq!(ambiguous["error"]["class"], "invalid_request");
        server.join().expect("daemon thread");
    }

    #[test]
    fn attribution_distinguishes_no_observation_from_an_absent_incarnation() {
        let directory = tempfile::tempdir().expect("tempdir");
        let socket = directory.path().join("kelpie.sock");
        let mut store = Store::in_memory().expect("store");
        let declared = store
            .declare_start(&test_intent("fresh", "term-a", "attr-none"))
            .expect("declare");
        let kelpie = Kelpie::new(
            store,
            HerdrClient::new(
                directory.path().join("unused-herdr.sock"),
                Duration::from_secs(1),
            ),
        );
        let mut daemon = Daemon::bind(&socket, kelpie).expect("bind daemon");
        let server = thread::spawn(move || {
            for _ in 0..2 {
                daemon.serve_one().expect("serve request");
            }
        });

        let none = send_request(
            &socket,
            &serde_json::json!({
                "id": "none-1",
                "method": "attribution",
                "params": {"incarnation_id": declared.incarnation_id},
            }),
        );
        assert!(none["result"]["observed"].is_null());
        assert_eq!(none["result"]["observations"], serde_json::json!([]));
        assert!(none["error"].is_null());

        let absent = send_request(
            &socket,
            &serde_json::json!({
                "id": "none-2",
                "method": "attribution",
                "params": {"incarnation_id": IncarnationId::new()},
            }),
        );
        assert!(absent["result"].is_null());
        assert_eq!(absent["error"]["class"], "conflict");
        server.join().expect("daemon thread");
    }

    #[test]
    fn who_resolves_a_live_pane_agent_or_socket_waiter_and_keeps_name_history() {
        let directory = tempfile::tempdir().expect("tempdir");
        let mut store = Store::in_memory().expect("store");
        let pane = seed_ready(&mut store, "reviewer", "w1:p1", "term-a", "who-pane");
        let waiter = store
            .register_socket_waiter("botserver", Parent::Parentless, "who-waiter")
            .expect("waiter");
        let mut kelpie = Kelpie::new(
            store,
            HerdrClient::new(
                directory.path().join("unused-herdr.sock"),
                Duration::from_secs(1),
            ),
        );

        let by_agent = dispatch_who(
            serde_json::json!({"agent_id": pane.logical_agent_id}),
            &mut kelpie,
        )
        .expect("pane agent");
        assert_eq!(by_agent["incarnation_id"], pane.incarnation_id.to_string());
        assert_eq!(by_agent["delivery_transport"], "herdr_prompt");

        let by_name = dispatch_who(serde_json::json!({"alias": "botserver"}), &mut kelpie)
            .expect("socket waiter");
        assert_eq!(
            by_name["logical_agent_id"],
            waiter.logical_agent_id.to_string()
        );
        assert!(by_name["incarnation_id"].is_null());
        assert_eq!(by_name["delivery_transport"], "socket_inbox");
        assert!(
            dispatch_who(
                serde_json::json!({"alias": "botserver", "refresh": true}),
                &mut kelpie,
            )
            .expect_err("waiters have no attribution")
            .to_string()
            .contains("no attribution")
        );

        let history = dispatch_who(
            serde_json::json!({"alias": "botserver", "history": true}),
            &mut kelpie,
        )
        .expect("history");
        assert_eq!(history["name"], "botserver");
        assert_eq!(history["claimants"].as_array().expect("claimants").len(), 1);

        let explicit_missing_pane = ClientRequest {
            id: "explicit-pane".into(),
            method: "who".into(),
            params: serde_json::json!({"pane_id": "w9:p9"}),
        };
        assert!(
            prepare_who_read(&explicit_missing_pane, &kelpie)
                .expect_err("explicit pane is read-only")
                .to_string()
                .contains("w9:p9")
        );
        let ambiguous = ClientRequest {
            id: "ambiguous".into(),
            method: "who".into(),
            params: serde_json::json!({
                "pane_id": "w9:p9",
                "alias": "botserver",
                "lazy_adopt_key": "must-not-adopt"
            }),
        };
        assert!(
            prepare_who_read(&ambiguous, &kelpie)
                .expect_err("two selectors")
                .to_string()
                .contains("exactly one")
        );
        assert!(
            dispatch_who(serde_json::json!({"alias": "nobody"}), &mut kelpie)
                .expect_err("unknown alias")
                .to_string()
                .contains("live Herdr agent may hold that name unadopted")
        );
    }

    #[test]
    fn a_client_hanging_up_does_not_stop_the_daemon() {
        let directory = tempfile::tempdir().expect("tempdir");
        let socket = directory.path().join("kelpie.sock");
        let kelpie = Kelpie::new(
            Store::in_memory().expect("store"),
            HerdrClient::new(
                directory.path().join("unused-herdr.sock"),
                Duration::from_secs(1),
            ),
        );
        let mut daemon = Daemon::bind(&socket, kelpie).expect("bind daemon");

        // A caller that times out writes its request and vanishes before the
        // response. Serving it must fail without ending the daemon.
        let mut abandoned = UnixStream::connect(&socket).expect("connect");
        abandoned
            .write_all(b"{\"id\":\"gone\",\"method\":\"pending\",\"params\":{}}\n")
            .expect("write request");
        abandoned
            .shutdown(Shutdown::Both)
            .expect("hang up before reading");
        drop(abandoned);
        let hung_up_by = Instant::now();
        loop {
            let _ = daemon.poll().expect("poll survives a hangup");
            if daemon.reading.is_empty() {
                break;
            }
            assert!(
                hung_up_by.elapsed() < Duration::from_secs(2),
                "hangup client never left reading"
            );
        }

        // The next client is served normally, proving the loop is still alive.
        let next = socket.clone();
        let client = thread::spawn(move || {
            send_request(
                &next,
                &serde_json::json!({"id": "after", "method": "not-a-method", "params": {}}),
            )
        });
        while !client.is_finished() {
            daemon.poll().expect("poll serves the next client");
        }
        let response = client.join().expect("client thread");
        assert_eq!(response["id"], "after");
        assert_eq!(response["error"]["class"], "invalid_request");
    }

    #[test]
    fn local_socket_returns_correlated_stable_error() {
        let directory = tempfile::tempdir().expect("tempdir");
        let socket = directory.path().join("kelpie.sock");
        let kelpie = Kelpie::new(
            Store::in_memory().expect("store"),
            HerdrClient::new(
                directory.path().join("unused-herdr.sock"),
                Duration::from_secs(1),
            ),
        );
        let mut daemon = Daemon::bind(&socket, kelpie).expect("bind daemon");
        let server = thread::spawn(move || daemon.serve_one().expect("serve request"));

        let mut stream = UnixStream::connect(&socket).expect("connect client");
        stream
            .write_all(b"{\"id\":\"request-1\",\"method\":\"not-a-method\",\"params\":{}}\n")
            .expect("write request");
        let mut line = String::new();
        BufReader::new(stream)
            .read_line(&mut line)
            .expect("read response");
        let response: Value = serde_json::from_str(&line).expect("response JSON");
        assert_eq!(response["id"], "request-1");
        assert_eq!(response["error"]["class"], "invalid_request");
        server.join().expect("daemon thread");
    }

    #[test]
    fn the_socket_refuses_a_postponed_ask_rather_than_ignoring_the_schedule() {
        let directory = tempfile::tempdir().expect("tempdir");
        let socket = directory.path().join("kelpie.sock");
        let mut store = Store::in_memory().expect("store");
        let waiting = store
            .declare_start(&test_intent("waiting", "term-a", "due-ask-waiting"))
            .expect("waiting");
        let kelpie = Kelpie::new(
            store,
            HerdrClient::new(
                directory.path().join("unused-herdr.sock"),
                Duration::from_secs(1),
            ),
        );
        let mut daemon = Daemon::bind(&socket, kelpie).expect("bind daemon");
        let server = thread::spawn(move || daemon.serve_one().expect("serve request"));

        // The raw protocol must refuse too. Silently dropping the schedule would
        // send immediately while the caller believes it was deferred.
        let response = send_request(
            &socket,
            &serde_json::json!({
                "id":"due-ask","method":"ask","params":{
                    "sender": waiting.logical_agent_id,
                    "recipient_alias":"owing",
                    "body":"review",
                    "idempotency_key":"due-ask-key",
                    "due_at_ms": 1_786_800_908_000_i64
                }
            }),
        );
        assert_eq!(response["error"]["class"], "invalid_request", "{response}");
        let message = response["error"]["message"].as_str().unwrap_or_default();
        assert!(message.contains("remind_after_ms"), "{message}");
        server.join().expect("daemon thread");
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn local_socket_cancel_accepts_any_requester_and_refuses_terminal_state() {
        let directory = tempfile::tempdir().expect("tempdir");
        let socket = directory.path().join("kelpie.sock");
        let mut store = Store::in_memory().expect("store");
        let waiting = store
            .declare_start(&test_intent("waiting", "term-a", "socket-waiting"))
            .expect("waiting");
        let owing = store
            .declare_start(&test_intent("owing", "term-b", "socket-owing"))
            .expect("owing");
        let spoof = store
            .declare_start(&test_intent("spoof", "term-c", "socket-spoof"))
            .expect("spoof");
        let ask = store
            .create_ask(
                waiting.logical_agent_id,
                owing.logical_agent_id,
                owing.incarnation_id,
                "question",
                "socket-cancel-ask",
            )
            .expect("ask");
        let resolved = store
            .create_ask(
                waiting.logical_agent_id,
                owing.logical_agent_id,
                owing.incarnation_id,
                "resolved question",
                "socket-resolved-ask",
            )
            .expect("resolved ask");
        store
            .begin_attempt(waiting.operation_id, waiting.incarnation_id, "seed-ready")
            .expect("waiting attempt");
        store
            .accept_start_ready(
                waiting.operation_id,
                waiting.incarnation_id,
                &crate::herdr::AgentObservation {
                    terminal_id: "term-a".into(),
                    pane_id: "w1:p1".into(),
                    name: Some("waiting".into()),
                    agent: Some("codex".into()),
                    interactive_ready: true,
                    launch_pending: false,
                    agent_session: None,
                },
                None,
            )
            .expect("ready waiting");
        let created = store
            .create_reply(
                resolved.message_id,
                owing.logical_agent_id,
                "done",
                ReplyDisposition::Final,
                "socket-resolve",
            )
            .expect("create final");
        let operation_id = created.operation_id.expect("pane reply operation");
        let recipient_incarnation = created
            .recipient_incarnation
            .expect("pane reply incarnation");
        store
            .begin_attempt(operation_id, recipient_incarnation, "socket-resolve-req")
            .expect("attempt");
        store
            .mark_submitted(operation_id, 1, "socket-resolve-req")
            .expect("submitted");
        store
            .accept_delivery(operation_id, recipient_incarnation, "w1:p1", "term-a")
            .expect("resolve");
        let kelpie = Kelpie::new(
            store,
            HerdrClient::new(
                directory.path().join("unused-herdr.sock"),
                Duration::from_secs(1),
            ),
        );
        let mut daemon = Daemon::bind(&socket, kelpie).expect("bind daemon");
        let server = thread::spawn(move || {
            for _ in 0..4 {
                daemon.serve_one().expect("serve request");
            }
        });
        let request = |id: &str, requester: LogicalAgentId, reason: &str| {
            serde_json::json!({
                "id": id,
                "method": "cancel",
                "params": {
                    "requester_agent_id": requester,
                    "ask_message_id": ask.message_id,
                    "reason": reason
                }
            })
        };

        let cancelled = send_request(
            &socket,
            &request("third", spoof.logical_agent_id, "no longer needed"),
        );
        assert_eq!(cancelled["result"]["state"], "cancelled");
        assert_eq!(cancelled["result"]["response"], "recorded");
        let absent = send_request(
            &socket,
            &serde_json::json!({
                "id": "absent",
                "method": "cancel",
                "params": {
                    "requester_agent_id": waiting.logical_agent_id,
                    "ask_message_id": MessageId::new(),
                    "reason": "no such ask"
                }
            }),
        );
        assert_eq!(absent["error"]["class"], "conflict");
        let terminal = send_request(
            &socket,
            &request("terminal", waiting.logical_agent_id, "repeat"),
        );
        assert_eq!(terminal["error"]["class"], "conflict");
        let resolved_terminal = send_request(
            &socket,
            &serde_json::json!({
                "id": "resolved",
                "method": "cancel",
                "params": {
                    "requester_agent_id": waiting.logical_agent_id,
                    "ask_message_id": resolved.message_id,
                    "reason": "too late"
                }
            }),
        );
        assert_eq!(resolved_terminal["error"]["class"], "conflict");
        server.join().expect("daemon thread");
    }

    #[test]
    fn local_socket_recovery_persists_lost_binding_and_preserves_obligation() {
        let directory = tempfile::tempdir().expect("tempdir");
        let database = directory.path().join("kelpie.sqlite3");
        let kelpie_socket = directory.path().join("kelpie.sock");
        let herdr_socket = directory.path().join("herdr.sock");
        let herdr_listener = UnixListener::bind(&herdr_socket).expect("bind Herdr fixture");
        let mut store = Store::open(&database).expect("store");
        let declared = store
            .declare_start(&test_intent("worker", "term-1", "recover-start"))
            .expect("intent");
        store
            .begin_attempt(
                declared.operation_id,
                declared.incarnation_id,
                "start-request",
            )
            .expect("attempt");
        store
            .accept_start_ready(
                declared.operation_id,
                declared.incarnation_id,
                &crate::herdr::AgentObservation {
                    terminal_id: "term-1".into(),
                    pane_id: "w1:p1".into(),
                    name: Some("worker".into()),
                    agent: Some("codex".into()),
                    interactive_ready: true,
                    launch_pending: false,
                    agent_session: None,
                },
                None,
            )
            .expect("ready");
        let ask = store
            .create_ask(
                declared.logical_agent_id,
                declared.logical_agent_id,
                declared.incarnation_id,
                "survive runtime loss",
                "recover-ask",
            )
            .expect("ask");
        let herdr_server = thread::spawn(move || {
            let responses = [
                serde_json::json!({"type":"pong","version":"test","protocol":20}),
                serde_json::json!({
                    "type":"session_snapshot",
                    "snapshot":{"protocol":20,"panes":[],"agents":[]}
                }),
            ];
            for result in responses {
                let (mut stream, _) = herdr_listener.accept().expect("accept Herdr request");
                let mut line = String::new();
                BufReader::new(stream.try_clone().expect("clone stream"))
                    .read_line(&mut line)
                    .expect("read Herdr request");
                let request: Value = serde_json::from_str(&line).expect("Herdr request JSON");
                serde_json::to_writer(
                    &mut stream,
                    &serde_json::json!({"id":request["id"],"result":result}),
                )
                .expect("write Herdr response");
                stream.write_all(b"\n").expect("finish Herdr response");
            }
        });
        let kelpie = Kelpie::new(
            store,
            HerdrClient::new(&herdr_socket, Duration::from_secs(1)),
        );
        let mut daemon = Daemon::bind(&kelpie_socket, kelpie).expect("bind daemon");
        let daemon_server = thread::spawn(move || daemon.serve_one().expect("serve recovery"));
        let response = send_request(
            &kelpie_socket,
            &serde_json::json!({"id":"recover-1","method":"recover","params":{}}),
        );
        assert_eq!(response["result"]["incarnations_marked_lost"], 1);
        daemon_server.join().expect("daemon thread");
        herdr_server.join().expect("Herdr thread");

        let reopened = Store::open(&database).expect("reopen");
        assert_eq!(
            reopened
                .incarnation_state(declared.incarnation_id)
                .expect("state"),
            crate::domain::IncarnationState::Lost
        );
        assert_eq!(
            reopened
                .obligation_state(ask.message_id)
                .expect("obligation"),
            crate::domain::ObligationState::Open
        );
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn a_start_awaiting_readiness_does_not_delay_an_unrelated_client() {
        // The defect this proves absent: a start held the accept loop for its
        // whole readiness timeout, so every unrelated tell, reply and ask in the
        // fleet queued behind it. Observed at 120s and 300s in production.
        let directory = tempfile::tempdir().expect("tempdir");
        let kelpie_socket = directory.path().join("kelpie.sock");
        let herdr_socket = directory.path().join("herdr.sock");
        let listener = UnixListener::bind(&herdr_socket).expect("bind fake Herdr");
        let herdr = thread::spawn(move || {
            // ping, snapshot, agent.start, then readiness polls that never
            // resolve: this start is doomed and will burn its whole budget.
            let scripted = [
                serde_json::json!({"type":"pong","version":"test","protocol":20}),
                serde_json::json!({
                    "type":"session_snapshot",
                    "snapshot":{"protocol":20,"panes":[{
                        "pane_id":"w1:p1","terminal_id":"term-a","cwd":"/tmp/work"
                    }],"agents":[]}
                }),
                serde_json::json!({
                    "type":"agent_started",
                    "agent":{"terminal_id":"term-a","pane_id":"w1:p1","name":"slow",
                        "agent":"codex","interactive_ready":false,"launch_pending":true},
                    "argv":["codex"]
                }),
            ];
            let mut index = 0;
            while let Ok((mut stream, _)) = listener.accept() {
                let mut line = String::new();
                if BufReader::new(stream.try_clone().expect("clone"))
                    .read_line(&mut line)
                    .is_err()
                {
                    return;
                }
                let Ok(request) = serde_json::from_str::<Value>(&line) else {
                    return;
                };
                let result = if index < scripted.len() {
                    let result = scripted[index].clone();
                    index += 1;
                    result
                } else {
                    // Still launching, forever.
                    serde_json::json!({"type":"agent_info","agent":{
                        "terminal_id":"term-a","pane_id":"w1:p1","name":"slow",
                        "agent":"codex","interactive_ready":false,"launch_pending":true
                    }})
                };
                let _ = serde_json::to_writer(
                    &mut stream,
                    &serde_json::json!({"id":request["id"],"result":result}),
                );
                let _ = stream.write_all(b"\n");
            }
        });

        let store = Store::in_memory().expect("store");
        let kelpie = Kelpie::new(
            store,
            HerdrClient::new(&herdr_socket, Duration::from_secs(1)),
        );
        let mut daemon = Daemon::bind(&kelpie_socket, kelpie).expect("bind daemon");

        let mut slow = test_intent("slow", "term-a", "slow-start");
        // Long enough that a blocking daemon could not answer anything else.
        slow.readiness_timeout_ms = 30_000;
        let start_socket = kelpie_socket.clone();
        let slow_client = thread::spawn(move || {
            // Teardown closes this connection unanswered, which is the point:
            // the client is still waiting when the assertions run.
            let mut stream = UnixStream::connect(&start_socket).expect("connect");
            let request = serde_json::json!({
                "id":"slow-start","method":"start",
                "params": serde_json::to_value(slow).expect("intent")
            });
            serde_json::to_writer(&mut stream, &request).expect("write");
            stream.write_all(b"\n").expect("finish");
            let mut line = String::new();
            let _ = BufReader::new(stream).read_line(&mut line);
            line
        });

        // Accept the start; it parks instead of blocking.
        let parked_by = Instant::now();
        while daemon.awaiting_starts.is_empty() {
            daemon.poll().expect("accept the start");
            assert!(
                parked_by.elapsed() < Duration::from_secs(10),
                "the start was never accepted"
            );
        }

        let notice_socket = kelpie_socket.clone();
        let unrelated = thread::spawn(move || {
            send_request(
                &notice_socket,
                &serde_json::json!({
                    "id":"unrelated","method":"notice.create",
                    "params":{"body":"served while a start waits"}
                }),
            )
        });

        // The unrelated client is answered while the start is still waiting.
        let asked_at = Instant::now();
        while !unrelated.is_finished() {
            daemon.poll().expect("poll");
            assert!(
                asked_at.elapsed() < Duration::from_secs(10),
                "unrelated request was not served while a start awaited readiness"
            );
        }
        let response = unrelated.join().expect("unrelated client");
        assert!(
            asked_at.elapsed() < Duration::from_secs(5),
            "served in {:?}, which is not concurrent with a 30s start",
            asked_at.elapsed()
        );
        assert!(response["result"]["notice_id"].is_string(), "{response}");
        assert_eq!(
            daemon.awaiting_starts.len(),
            1,
            "the start is still waiting, so it never blocked the loop"
        );
        assert!(!slow_client.is_finished(), "the slow start has not settled");

        // Teardown only. The fake Herdr accepts forever by design, so it is
        // left detached rather than joined.
        drop(daemon);
        let _ = slow_client.join();
        drop(herdr);
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn disconnected_start_client_does_not_change_the_durable_outcome() {
        let directory = tempfile::tempdir().expect("tempdir");
        let kelpie_socket = directory.path().join("kelpie.sock");
        let herdr_socket = directory.path().join("herdr.sock");
        let listener = UnixListener::bind(&herdr_socket).expect("bind fake Herdr");
        let herdr = thread::spawn(move || {
            for expected in [
                "ping",
                "session.snapshot",
                "agent.start",
                "agent.get",
                "agent.prompt",
            ] {
                let (mut stream, _) = listener.accept().expect("accept");
                let mut line = String::new();
                BufReader::new(stream.try_clone().expect("clone"))
                    .read_line(&mut line)
                    .expect("read");
                let request: Value = serde_json::from_str(&line).expect("request");
                assert_eq!(request["method"], expected);
                let agent = serde_json::json!({
                    "terminal_id":"term-a","pane_id":"w1:p1","name":"worker",
                    "agent":"codex","interactive_ready":true,"launch_pending":false
                });
                let result = match expected {
                    "ping" => serde_json::json!({"type":"pong","version":"test","protocol":20}),
                    "session.snapshot" => serde_json::json!({
                        "type":"session_snapshot",
                        "snapshot":{"protocol":20,"panes":[{"pane_id":"w1:p1","terminal_id":"term-a","cwd":"/tmp/work"}],"agents":[]}
                    }),
                    "agent.start" => {
                        serde_json::json!({"type":"agent_started","agent":agent,"argv":["codex"]})
                    }
                    "agent.get" => serde_json::json!({"type":"agent_info","agent":agent}),
                    "agent.prompt" => serde_json::json!({"type":"agent_prompted","agent":agent}),
                    _ => unreachable!(),
                };
                serde_json::to_writer(
                    &mut stream,
                    &serde_json::json!({"id":request["id"],"result":result}),
                )
                .expect("write");
                stream.write_all(b"\n").expect("finish");
            }
        });
        let kelpie = Kelpie::new(
            Store::in_memory().expect("store"),
            HerdrClient::new(&herdr_socket, Duration::from_secs(2)),
        );
        let mut daemon = Daemon::bind(&kelpie_socket, kelpie).expect("bind daemon");
        let mut client = UnixStream::connect(&kelpie_socket).expect("connect client");
        let start = test_intent("worker", "term-a", "disconnect-start");
        serde_json::to_writer(
            &mut client,
            &serde_json::json!({
                "id":"disconnect-start","method":"start",
                "params":serde_json::to_value(start).expect("intent")
            }),
        )
        .expect("write request");
        client.write_all(b"\n").expect("finish request");
        drop(client);
        let started = Instant::now();
        let mut parked = false;
        loop {
            daemon.poll().expect("poll");
            parked |= !daemon.awaiting_starts.is_empty();
            if parked && daemon.awaiting_starts.is_empty() {
                break;
            }
            assert!(started.elapsed() < Duration::from_secs(5));
        }
        herdr.join().expect("Herdr");
        let report = daemon.kelpie.store().report().expect("report");
        let incarnation = &report
            .agents
            .iter()
            .find(|agent| agent.public_name == "worker")
            .expect("worker")
            .incarnations[0];
        assert_eq!(incarnation.state, crate::domain::IncarnationState::Ready);
        assert_eq!(
            incarnation
                .latest_operation
                .as_ref()
                .map(|(_, _, outcome)| *outcome),
            Some(crate::domain::OperationOutcome::Succeeded)
        );
        assert!(
            daemon
                .kelpie
                .store_mut()
                .operator_notices()
                .expect("notices")
                .is_empty()
        );
    }

    #[test]
    fn concurrent_unknown_pane_who_refresh_calls_share_only_the_queued_snapshot() {
        let directory = tempfile::tempdir().expect("tempdir");
        let kelpie_socket = directory.path().join("kelpie.sock");
        let herdr_socket = directory.path().join("herdr.sock");
        let listener = UnixListener::bind(&herdr_socket).expect("bind fake Herdr");
        let snapshot_count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let observed_count = std::sync::Arc::clone(&snapshot_count);
        let herdr = thread::spawn(move || {
            for cycle in 0..2 {
                for method in ["ping", "session.snapshot"] {
                    let (mut stream, _) = listener.accept().expect("accept");
                    let mut line = String::new();
                    BufReader::new(stream.try_clone().expect("clone"))
                        .read_line(&mut line)
                        .expect("read");
                    let request: Value = serde_json::from_str(&line).expect("request");
                    assert_eq!(request["method"], method);
                    let result = if method == "ping" {
                        serde_json::json!({"type":"pong","version":"test","protocol":20})
                    } else {
                        observed_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        if cycle == 0 {
                            thread::sleep(Duration::from_millis(200));
                        }
                        serde_json::json!({
                            "type":"session_snapshot",
                            "snapshot":{
                                "protocol":20,
                                "panes":[
                                    {"pane_id":"w1:p1","terminal_id":"term-1","cwd":"/tmp/a"},
                                    {"pane_id":"w1:p2","terminal_id":"term-2","cwd":"/tmp/b"},
                                    {"pane_id":"w1:p3","terminal_id":"term-3","cwd":"/tmp/c"}
                                ],
                                "agents":[
                                    {"pane_id":"w1:p1","terminal_id":"term-1","name":"agent-a","agent":"codex","interactive_ready":true,"launch_pending":false},
                                    {"pane_id":"w1:p2","terminal_id":"term-2","name":"agent-b","agent":"codex","interactive_ready":true,"launch_pending":false},
                                    {"pane_id":"w1:p3","terminal_id":"term-3","name":"agent-c","agent":"codex","interactive_ready":true,"launch_pending":false}
                                ]
                            }
                        })
                    };
                    serde_json::to_writer(
                        &mut stream,
                        &serde_json::json!({"id":request["id"],"result":result}),
                    )
                    .expect("write");
                    stream.write_all(b"\n").expect("finish");
                }
            }
        });
        let kelpie = Kelpie::new(
            Store::in_memory().expect("store"),
            HerdrClient::new(&herdr_socket, Duration::from_secs(2)),
        );
        let mut daemon = Daemon::bind(&kelpie_socket, kelpie).expect("bind daemon");
        let clients: Vec<_> = (1..=3)
            .map(|index| {
                let socket = kelpie_socket.clone();
                thread::spawn(move || {
                    send_request(
                        &socket,
                        &serde_json::json!({
                            "id":format!("who-{index}"),"method":"who",
                            "params":{"pane_id":format!("w1:p{index}"),"lazy_adopt_key":format!("adopt-{index}"),"refresh":true}
                        }),
                    )
                })
            })
            .collect();
        let started = Instant::now();
        while clients.iter().any(|client| !client.is_finished()) {
            daemon.poll().expect("poll");
            assert!(started.elapsed() < Duration::from_secs(5));
        }
        for response in clients
            .into_iter()
            .map(|client| client.join().expect("client"))
        {
            assert!(response["error"].is_null(), "{response}");
        }
        herdr.join().expect("Herdr");
        assert_eq!(snapshot_count.load(std::sync::atomic::Ordering::SeqCst), 2);
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn waiter_retire_notice_does_not_delay_an_unrelated_client() {
        let directory = tempfile::tempdir().expect("tempdir");
        let kelpie_socket = directory.path().join("kelpie.sock");
        let herdr_socket = directory.path().join("herdr.sock");
        let listener = UnixListener::bind(&herdr_socket).expect("bind fake Herdr");
        let mut store = Store::in_memory().expect("store");
        let waiter = store
            .register_socket_waiter("inbox", Parent::Parentless, "waiter")
            .expect("waiter");
        let owing = store
            .declare_start(&test_intent("owing", "term-a", "owing"))
            .expect("owing");
        let observed = crate::herdr::AgentObservation {
            terminal_id: "term-a".into(),
            pane_id: "w1:p1".into(),
            name: Some("owing".into()),
            agent: Some("codex".into()),
            interactive_ready: true,
            launch_pending: false,
            agent_session: None,
        };
        store
            .begin_attempt(owing.operation_id, owing.incarnation_id, "seed")
            .expect("attempt");
        store
            .accept_start_ready(owing.operation_id, owing.incarnation_id, &observed, None)
            .expect("ready");
        let ask = store
            .create_ask(
                waiter.logical_agent_id,
                owing.logical_agent_id,
                owing.incarnation_id,
                "question",
                "ask",
            )
            .expect("ask");
        let attempt = store
            .begin_attempt(ask.operation_id, owing.incarnation_id, "ask")
            .expect("ask attempt");
        store
            .mark_submitted(ask.operation_id, attempt, "ask")
            .expect("submitted");
        store
            .accept_delivery(ask.operation_id, owing.incarnation_id, "w1:p1", "term-a")
            .expect("delivered");
        let herdr = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept prompt");
            let mut line = String::new();
            BufReader::new(stream.try_clone().expect("clone"))
                .read_line(&mut line)
                .expect("read");
            let request: Value = serde_json::from_str(&line).expect("request");
            assert_eq!(request["method"], "agent.prompt");
            thread::sleep(Duration::from_secs(2));
            serde_json::to_writer(
                &mut stream,
                &serde_json::json!({
                    "id":request["id"],
                    "result":{"type":"agent_prompted","agent":observed}
                }),
            )
            .expect("write");
            stream.write_all(b"\n").expect("finish");
        });
        let kelpie = Kelpie::new(
            store,
            HerdrClient::new(&herdr_socket, Duration::from_secs(3)),
        );
        let mut daemon = Daemon::bind(&kelpie_socket, kelpie).expect("bind daemon");
        let retire_socket = kelpie_socket.clone();
        let retire = thread::spawn(move || {
            send_request(
                &retire_socket,
                &serde_json::json!({
                    "id":"retire","method":"waiter.retire",
                    "params":{"logical_agent_id":waiter.logical_agent_id}
                }),
            )
        });
        while daemon.awaiting_waiter_retires.is_empty() {
            daemon.poll().expect("accept retire");
        }
        let notice_socket = kelpie_socket.clone();
        let unrelated = thread::spawn(move || {
            send_request(
                &notice_socket,
                &serde_json::json!({
                    "id":"unrelated","method":"notice.create","params":{"body":"ready"}
                }),
            )
        });
        let started = Instant::now();
        while !unrelated.is_finished() {
            daemon.poll().expect("poll");
            assert!(started.elapsed() < Duration::from_secs(1));
        }
        assert!(unrelated.join().expect("unrelated")["result"]["notice_id"].is_string());
        while !retire.is_finished() {
            daemon.poll().expect("finish retire");
        }
        let response = retire.join().expect("retire");
        assert_eq!(
            response["result"]["owing_notices"][0]["owing_response"],
            "delivered"
        );
        herdr.join().expect("Herdr");
    }

    #[test]
    fn who_refresh_snapshot_does_not_delay_an_unrelated_client() {
        let directory = tempfile::tempdir().expect("tempdir");
        let kelpie_socket = directory.path().join("kelpie.sock");
        let herdr_socket = directory.path().join("herdr.sock");
        let listener = UnixListener::bind(&herdr_socket).expect("bind fake Herdr");
        let mut store = Store::in_memory().expect("store");
        let declared = store
            .declare_start(&test_intent("worker", "term-a", "attr-refresh"))
            .expect("declare");
        let observed = crate::herdr::AgentObservation {
            terminal_id: "term-a".into(),
            pane_id: "w1:p1".into(),
            name: Some("worker".into()),
            agent: Some("codex".into()),
            interactive_ready: true,
            launch_pending: false,
            agent_session: None,
        };
        store
            .begin_attempt(declared.operation_id, declared.incarnation_id, "seed")
            .expect("attempt");
        store
            .accept_start_ready(
                declared.operation_id,
                declared.incarnation_id,
                &observed,
                None,
            )
            .expect("ready");
        let herdr = thread::spawn(move || {
            for (method, result, delay) in [
                (
                    "ping",
                    serde_json::json!({"type":"pong","version":"test","protocol":20}),
                    Duration::ZERO,
                ),
                (
                    "session.snapshot",
                    serde_json::json!({
                        "type":"session_snapshot",
                        "snapshot":{"protocol":20,"panes":[],"agents":[]}
                    }),
                    Duration::from_secs(2),
                ),
            ] {
                let (mut stream, _) = listener.accept().expect("accept");
                let mut line = String::new();
                BufReader::new(stream.try_clone().expect("clone"))
                    .read_line(&mut line)
                    .expect("read");
                let request: Value = serde_json::from_str(&line).expect("request");
                assert_eq!(request["method"], method);
                thread::sleep(delay);
                serde_json::to_writer(
                    &mut stream,
                    &serde_json::json!({"id":request["id"],"result":result}),
                )
                .expect("write");
                stream.write_all(b"\n").expect("finish");
            }
        });
        let kelpie = Kelpie::new(
            store,
            HerdrClient::new(&herdr_socket, Duration::from_secs(3)),
        );
        let mut daemon = Daemon::bind(&kelpie_socket, kelpie).expect("bind daemon");
        let refresh_socket = kelpie_socket.clone();
        let refresh = thread::spawn(move || {
            send_request(
                &refresh_socket,
                &serde_json::json!({
                    "id":"refresh","method":"who",
                    "params":{"incarnation_id":declared.incarnation_id,"refresh":true}
                }),
            )
        });
        while daemon.awaiting_reads.is_empty() {
            daemon.poll().expect("accept refresh");
        }
        let notice_socket = kelpie_socket.clone();
        let unrelated = thread::spawn(move || {
            send_request(
                &notice_socket,
                &serde_json::json!({
                    "id":"unrelated","method":"notice.create","params":{"body":"ready"}
                }),
            )
        });
        let started = Instant::now();
        while !unrelated.is_finished() {
            daemon.poll().expect("poll");
            assert!(started.elapsed() < Duration::from_secs(1));
        }
        assert!(unrelated.join().expect("unrelated")["result"]["notice_id"].is_string());
        while !refresh.is_finished() {
            daemon.poll().expect("finish refresh");
        }
        assert!(refresh.join().expect("refresh")["error"].is_null());
        herdr.join().expect("Herdr");
    }

    #[test]
    fn a_stalled_client_does_not_delay_an_unrelated_request() {
        // A peer that connects and never sends a line used to block
        // `serve_stream` on `read_line` with no timeout, freezing the fleet.
        let directory = tempfile::tempdir().expect("tempdir");
        let kelpie_socket = directory.path().join("kelpie.sock");
        let herdr_socket = directory.path().join("herdr.sock");
        let kelpie = Kelpie::new(
            Store::in_memory().expect("store"),
            HerdrClient::new(&herdr_socket, Duration::from_secs(1)),
        );
        let mut daemon = Daemon::bind(&kelpie_socket, kelpie).expect("bind daemon");

        let stall_socket = kelpie_socket.clone();
        let stalled = thread::spawn(move || {
            let _stream = UnixStream::connect(&stall_socket).expect("connect stall");
            thread::sleep(Duration::from_secs(3));
        });

        let parked_by = Instant::now();
        while daemon.reading.is_empty() {
            daemon.poll().expect("accept stall");
            assert!(
                parked_by.elapsed() < Duration::from_secs(2),
                "stalled client was never accepted"
            );
        }

        let notice_socket = kelpie_socket.clone();
        let unrelated = thread::spawn(move || {
            send_request(
                &notice_socket,
                &serde_json::json!({
                    "id":"unrelated","method":"notice.create",
                    "params":{"body":"served while a peer sent nothing"}
                }),
            )
        });

        let asked_at = Instant::now();
        while !unrelated.is_finished() {
            daemon.poll().expect("poll");
            assert!(
                asked_at.elapsed() < Duration::from_secs(2),
                "unrelated request waited on a stalled client"
            );
        }
        let response = unrelated.join().expect("unrelated");
        assert!(
            asked_at.elapsed() < Duration::from_millis(500),
            "served in {:?}",
            asked_at.elapsed()
        );
        assert!(response["result"]["notice_id"].is_string(), "{response}");
        assert_eq!(daemon.reading.len(), 1, "stalled client is still parked");
        drop(daemon);
        let _ = stalled.join();
    }

    #[test]
    fn poll_fires_due_tell_with_no_client() {
        let directory = tempfile::tempdir().expect("tempdir");
        let kelpie_socket = directory.path().join("kelpie.sock");
        let herdr_socket = directory.path().join("herdr.sock");
        let listener = UnixListener::bind(&herdr_socket).expect("bind fake Herdr");
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept due prompt");
            let mut line = String::new();
            BufReader::new(stream.try_clone().expect("clone"))
                .read_line(&mut line)
                .expect("read");
            let request: Value = serde_json::from_str(&line).expect("json");
            assert_eq!(request["method"], "agent.prompt");
            let result = serde_json::json!({
                "type":"agent_prompted",
                "agent":{
                    "terminal_id":"term-a","pane_id":"w1:p1","name":"waiting",
                    "agent":"codex","interactive_ready":true,"launch_pending":false
                }
            });
            serde_json::to_writer(
                &mut stream,
                &serde_json::json!({"id":request["id"],"result":result}),
            )
            .expect("write");
            stream.write_all(b"\n").expect("finish");
        });

        let mut store = Store::in_memory().expect("store");
        let declared = store
            .declare_start(&test_intent("waiting", "term-a", "due-poll-start"))
            .expect("declare");
        store
            .begin_attempt(declared.operation_id, declared.incarnation_id, "seed")
            .expect("attempt");
        store
            .accept_start_ready(
                declared.operation_id,
                declared.incarnation_id,
                &crate::herdr::AgentObservation {
                    terminal_id: "term-a".into(),
                    pane_id: "w1:p1".into(),
                    name: Some("waiting".into()),
                    agent: Some("codex".into()),
                    interactive_ready: true,
                    launch_pending: false,
                    agent_session: None,
                },
                None,
            )
            .expect("ready");
        let due_at = crate::store::store_clock_ms().expect("clock") - 1;
        let tell = store
            .create_tell_with_due(
                declared.logical_agent_id,
                declared.logical_agent_id,
                declared.incarnation_id,
                "wake",
                "due-poll-tell",
                Some(due_at),
            )
            .expect("queued");
        let mut daemon = Daemon::bind(
            &kelpie_socket,
            Kelpie::new(
                store,
                HerdrClient::new(&herdr_socket, Duration::from_secs(1)),
            ),
        )
        .expect("bind");
        let started = Instant::now();
        loop {
            let _ = daemon.poll().expect("poll without client");
            if daemon
                .kelpie
                .store()
                .delivery_outcome(tell.operation_id)
                .expect("delivery")
                == crate::domain::DeliveryOutcome::Accepted
            {
                break;
            }
            assert!(
                started.elapsed() < Duration::from_secs(2),
                "due tell did not accept"
            );
            thread::sleep(Duration::from_millis(10));
        }
        server.join().expect("herdr");
    }

    fn seed_ready(
        store: &mut Store,
        name: &str,
        pane: &str,
        terminal: &str,
        key: &str,
    ) -> crate::store::DeclaredStart {
        let mut intent = test_intent(name, terminal, key);
        intent.pane_id = pane.into();
        let declared = store.declare_start(&intent).expect("declare");
        store
            .begin_attempt(declared.operation_id, declared.incarnation_id, key)
            .expect("attempt");
        store
            .accept_start_ready(
                declared.operation_id,
                declared.incarnation_id,
                &crate::herdr::AgentObservation {
                    terminal_id: terminal.into(),
                    pane_id: pane.into(),
                    name: Some(name.into()),
                    agent: Some("codex".into()),
                    interactive_ready: true,
                    launch_pending: false,
                    agent_session: None,
                },
                None,
            )
            .expect("ready");
        declared
    }

    fn tell_request(id: &str, agent: &crate::store::DeclaredStart, body: &str, key: &str) -> Value {
        serde_json::json!({
            "id": id,
            "method": "tell",
            "params": {
                "sender": agent.logical_agent_id,
                "recipient": agent.logical_agent_id,
                "recipient_incarnation": agent.incarnation_id,
                "body": body,
                "idempotency_key": key
            }
        })
    }

    fn spawn_prompt_herdr(
        socket: &Path,
        slow_pane: Option<&'static str>,
        delay: Duration,
        seen: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
    ) -> thread::JoinHandle<()> {
        let listener = UnixListener::bind(socket).expect("bind fake Herdr");
        listener.set_nonblocking(true).expect("nonblocking");
        thread::spawn(move || {
            let started = Instant::now();
            while started.elapsed() < Duration::from_secs(8) {
                match listener.accept() {
                    Ok((stream, _)) => {
                        let seen = std::sync::Arc::clone(&seen);
                        thread::spawn(move || {
                            let mut stream = stream;
                            stream.set_nonblocking(false).expect("blocking");
                            let mut line = String::new();
                            if BufReader::new(stream.try_clone().expect("clone"))
                                .read_line(&mut line)
                                .is_err()
                                || line.trim().is_empty()
                            {
                                return;
                            }
                            let Ok(request) = serde_json::from_str::<Value>(&line) else {
                                return;
                            };
                            if request["method"] == "agent.prompt" {
                                let target = request["params"]["target"]
                                    .as_str()
                                    .unwrap_or_default()
                                    .to_string();
                                let text = request["params"]["text"]
                                    .as_str()
                                    .unwrap_or_default()
                                    .to_string();
                                seen.lock().expect("seen").push(text);
                                if slow_pane == Some(target.as_str()) {
                                    thread::sleep(delay);
                                }
                            }
                            let target = request["params"]["target"].as_str().unwrap_or_default();
                            let (terminal_id, name) = match target {
                                "w-slow:p0" => ("term-slow", "slow"),
                                "w-fast:p0" => ("term-fast", "fast"),
                                "w1:p1" => ("term-a", "worker"),
                                other => (other, "agent"),
                            };
                            let result = if request["method"] == "ping" {
                                serde_json::json!({"type":"pong","version":"test","protocol":20})
                            } else {
                                serde_json::json!({
                                    "type":"agent_prompted",
                                    "agent":{
                                        "terminal_id": terminal_id,
                                        "pane_id": target,
                                        "name": name,
                                        "agent":"codex",
                                        "interactive_ready":true,
                                        "launch_pending":false
                                    }
                                })
                            };
                            let _ = serde_json::to_writer(
                                &mut stream,
                                &serde_json::json!({"id":request["id"],"result":result}),
                            );
                            let _ = stream.write_all(b"\n");
                        });
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(5));
                    }
                    Err(_) => break,
                }
            }
        })
    }

    #[test]
    fn two_tells_to_different_panes_run_concurrently() {
        let directory = tempfile::tempdir().expect("tempdir");
        let kelpie_socket = directory.path().join("kelpie.sock");
        let herdr_socket = directory.path().join("herdr.sock");
        let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let _herdr = spawn_prompt_herdr(
            &herdr_socket,
            Some("w-slow:p0"),
            Duration::from_secs(2),
            std::sync::Arc::clone(&seen),
        );

        let mut store = Store::in_memory().expect("store");
        let slow = seed_ready(&mut store, "slow", "w-slow:p0", "term-slow", "slow-start");
        let fast = seed_ready(&mut store, "fast", "w-fast:p0", "term-fast", "fast-start");
        let mut daemon = Daemon::bind(
            &kelpie_socket,
            Kelpie::new(
                store,
                HerdrClient::new(&herdr_socket, Duration::from_secs(5)),
            ),
        )
        .expect("bind");

        let slow_socket = kelpie_socket.clone();
        let slow_req = tell_request("slow", &slow, "slow-body", "slow-tell");
        let slow_client = thread::spawn(move || send_request(&slow_socket, &slow_req));
        let fast_socket = kelpie_socket.clone();
        let fast_req = tell_request("fast", &fast, "fast-body", "fast-tell");
        let fast_client = thread::spawn(move || send_request(&fast_socket, &fast_req));

        let started = Instant::now();
        while !fast_client.is_finished() {
            daemon.poll().expect("poll");
            assert!(
                started.elapsed() < Duration::from_secs(2),
                "fast pane waited on the slow pane"
            );
        }
        let fast_response = fast_client.join().expect("fast client");
        assert!(
            started.elapsed() < Duration::from_millis(500),
            "fast pane took {:?}",
            started.elapsed()
        );
        assert_eq!(
            fast_response["result"]["delivery_outcome"], "accepted",
            "{fast_response}"
        );
        while !slow_client.is_finished() {
            daemon.poll().expect("drain slow");
        }
        slow_client.join().expect("slow client");
    }

    #[test]
    fn two_tells_to_the_same_pane_preserve_submission_order() {
        let directory = tempfile::tempdir().expect("tempdir");
        let kelpie_socket = directory.path().join("kelpie.sock");
        let herdr_socket = directory.path().join("herdr.sock");
        let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let _herdr = spawn_prompt_herdr(
            &herdr_socket,
            None,
            Duration::from_millis(50),
            std::sync::Arc::clone(&seen),
        );

        let mut store = Store::in_memory().expect("store");
        let worker = seed_ready(&mut store, "worker", "w1:p1", "term-a", "fifo-start");
        let mut daemon = Daemon::bind(
            &kelpie_socket,
            Kelpie::new(
                store,
                HerdrClient::new(&herdr_socket, Duration::from_secs(5)),
            ),
        )
        .expect("bind");

        let first_socket = kelpie_socket.clone();
        let first_req = tell_request("t1", &worker, "first", "fifo-1");
        let first = thread::spawn(move || send_request(&first_socket, &first_req));
        let parked = Instant::now();
        while daemon.awaiting_prompts.is_empty() {
            daemon.poll().expect("park first");
            assert!(
                parked.elapsed() < Duration::from_secs(2),
                "first tell never parked"
            );
        }
        let second_socket = kelpie_socket.clone();
        let second_req = tell_request("t2", &worker, "second", "fifo-2");
        let second = thread::spawn(move || send_request(&second_socket, &second_req));

        let started = Instant::now();
        while !first.is_finished() || !second.is_finished() {
            daemon.poll().expect("poll");
            assert!(
                started.elapsed() < Duration::from_secs(3),
                "same-pane tells did not finish"
            );
        }
        first.join().expect("first");
        second.join().expect("second");
        let texts: Vec<_> = seen
            .lock()
            .expect("seen")
            .iter()
            .filter(|text| text.contains("first") || text.contains("second"))
            .cloned()
            .collect();
        assert!(
            texts.iter().position(|text| text.contains("first"))
                < texts.iter().position(|text| text.contains("second")),
            "pane-lane FIFO violated: {texts:?}"
        );
    }

    fn queue_reply_for_waiter(store: &mut Store) -> (LogicalAgentId, MessageId, MessageId) {
        use crate::domain::{DeliveryOutcome, MessageKind, ReplyDisposition};
        let waiter = store
            .register_socket_waiter("inbox", Parent::Parentless, "inbox-waiter")
            .expect("register");
        let ask = store
            .insert_inbox_message(
                waiter.logical_agent_id,
                MessageKind::Ask,
                "question",
                None,
                None,
            )
            .expect("ask");
        let reply = store
            .insert_inbox_message(
                waiter.logical_agent_id,
                MessageKind::Reply,
                "later reply body",
                Some(ask),
                Some(ReplyDisposition::Final),
            )
            .expect("reply");
        store
            .record_socket_inbox_delivery(reply, waiter.logical_agent_id, DeliveryOutcome::Queued)
            .expect("queue");
        (waiter.logical_agent_id, ask, reply)
    }

    fn rpc(daemon: &mut Daemon, socket: &Path, request: Value) -> Value {
        let socket = socket.to_path_buf();
        let client = thread::spawn(move || send_request(&socket, &request));
        while !client.is_finished() {
            daemon.poll().expect("rpc poll");
        }
        client.join().expect("rpc client")
    }

    fn claim_waiter(socket: &Path, waiter: LogicalAgentId, id: &str) -> UnixStream {
        let mut stream = UnixStream::connect(socket).expect("connect inbox");
        serde_json::to_writer(
            &mut stream,
            &serde_json::json!({
                "id": id,
                "method": "inbox.claim",
                "params": {"logical_agent_id": waiter},
            }),
        )
        .expect("write claim");
        stream.write_all(b"\n").expect("nl");
        stream
    }

    fn read_json(reader: &mut BufReader<UnixStream>) -> Value {
        let mut line = String::new();
        reader.read_line(&mut line).expect("read");
        serde_json::from_str(&line).expect("json")
    }

    fn bind_inbox_daemon(directory: &Path, store: Store) -> (Daemon, PathBuf) {
        let socket = directory.join("kelpie.sock");
        let daemon = Daemon::bind(
            &socket,
            Kelpie::new(
                store,
                HerdrClient::new(directory.join("unused-herdr.sock"), Duration::from_secs(1)),
            ),
        )
        .expect("bind");
        (daemon, socket)
    }

    fn drain_reply(daemon: &mut Daemon, socket: &Path, waiter: LogicalAgentId, id: &str) -> Value {
        let stream = claim_waiter(socket, waiter, id);
        while daemon.inboxes.is_empty() {
            daemon.poll().expect("claim");
        }
        let mut reader = BufReader::new(stream);
        let claim = read_json(&mut reader);
        assert_eq!(claim["result"]["claimed"], true);
        read_json(&mut reader)
    }

    #[test]
    fn pending_and_ask_info_are_not_the_socket_inbox() {
        let directory = tempfile::tempdir().expect("tempdir");
        let mut store = Store::in_memory().expect("store");
        let (waiter, ask, _) = queue_reply_for_waiter(&mut store);
        let (mut daemon, socket) = bind_inbox_daemon(directory.path(), store);
        let pending = rpc(
            &mut daemon,
            &socket,
            serde_json::json!({
                "id": "pending-1",
                "method": "pending",
                "params": {"agent_id": waiter},
            }),
        );
        assert_eq!(pending["result"], serde_json::json!([]));
        assert!(
            !pending.to_string().contains("later reply body"),
            "{pending}"
        );
        let ask_info = rpc(
            &mut daemon,
            &socket,
            serde_json::json!({
                "id": "ask-info-1",
                "method": "ask.info",
                "params": {"ask_message_id": ask},
            }),
        );
        assert!(
            !ask_info.to_string().contains("later reply body"),
            "{ask_info}"
        );
    }

    #[test]
    fn inbox_claim_refuses_a_foreign_id() {
        let directory = tempfile::tempdir().expect("tempdir");
        let mut store = Store::in_memory().expect("store");
        let _ = queue_reply_for_waiter(&mut store);
        let (mut daemon, socket) = bind_inbox_daemon(directory.path(), store);
        let refused = rpc(
            &mut daemon,
            &socket,
            serde_json::json!({
                "id": "claim-foreign",
                "method": "inbox.claim",
                "params": {"logical_agent_id": LogicalAgentId::new()},
            }),
        );
        assert_eq!(refused["error"]["class"], "conflict");
        let message = refused["error"]["message"].as_str().expect("msg");
        assert!(
            message.contains("absent") || message.contains("not an active socket waiter"),
            "{refused}"
        );
    }

    #[test]
    fn alias_tell_to_socket_waiter_queues_without_a_herdr_snapshot() {
        let directory = tempfile::tempdir().expect("tempdir");
        let mut store = Store::in_memory().expect("store");
        let sender = seed_ready(
            &mut store,
            "sender",
            "w1:p-sender",
            "term-sender",
            "sender-start",
        );
        let waiter = store
            .register_socket_waiter("botserver", Parent::Parentless, "waiter-register")
            .expect("waiter");
        let (mut daemon, socket) = bind_inbox_daemon(directory.path(), store);
        let stream = claim_waiter(&socket, waiter.logical_agent_id, "claim-tell");
        while daemon.inboxes.is_empty() {
            daemon.poll().expect("claim");
        }
        let mut reader = BufReader::new(stream);
        assert_eq!(read_json(&mut reader)["result"]["claimed"], true);

        let response = rpc(
            &mut daemon,
            &socket,
            serde_json::json!({
                "id": "tell-waiter",
                "method": "tell",
                "params": {
                    "sender": sender.logical_agent_id,
                    "recipient_alias": "botserver",
                    "body": "unsolicited progress",
                    "idempotency_key": "tell-waiter"
                }
            }),
        );
        assert_eq!(
            response["result"]["recipient"],
            waiter.logical_agent_id.to_string()
        );
        assert_eq!(response["result"]["recipient_incarnation"], Value::Null);
        assert_eq!(response["result"]["operation_id"], Value::Null);
        assert_eq!(response["result"]["delivery_transport"], "socket_inbox");
        assert_eq!(response["result"]["delivery_outcome"], "queued");

        for _ in 0..5 {
            daemon.poll().expect("offer tell");
        }
        let delivery = read_json(&mut reader);
        assert_eq!(delivery["method"], "inbox.delivery");
        assert_eq!(delivery["params"]["kind"], "tell");
        assert_eq!(delivery["params"]["body"], "unsolicited progress");
        assert_eq!(
            delivery["params"]["sender_agent_id"],
            sender.logical_agent_id.to_string()
        );
        assert_eq!(delivery["params"]["sender_public_name"], "sender");
    }

    #[test]
    fn inbox_reconnect_drains_the_same_waiter_until_ack() {
        let directory = tempfile::tempdir().expect("tempdir");
        let mut store = Store::in_memory().expect("store");
        let (waiter, _, reply) = queue_reply_for_waiter(&mut store);
        let (mut daemon, socket) = bind_inbox_daemon(directory.path(), store);

        let delivery = drain_reply(&mut daemon, &socket, waiter, "claim-1");
        assert_eq!(delivery["method"], "inbox.delivery");
        assert_eq!(delivery["params"]["body"], "later reply body");
        assert_eq!(delivery["params"]["message_id"], reply.to_string());
        assert_eq!(delivery["params"]["kind"], "reply");
        while !daemon.inboxes.is_empty() {
            let _ = daemon.poll().expect("drop");
        }

        let mut again = claim_waiter(&socket, waiter, "claim-2");
        while daemon.inboxes.is_empty() {
            daemon.poll().expect("reclaim");
        }
        let mut reader = BufReader::new(again.try_clone().expect("clone"));
        let _claim = read_json(&mut reader);
        let delivery = read_json(&mut reader);
        assert_eq!(delivery["params"]["body"], "later reply body");
        serde_json::to_writer(
            &mut again,
            &serde_json::json!({
                "id": "ack-1",
                "method": "inbox.ack",
                "params": {"message_id": reply},
            }),
        )
        .expect("ack");
        again.write_all(b"\n").expect("nl");
        let mut acked = false;
        for _ in 0..20 {
            daemon.poll().expect("ack poll");
            if daemon
                .kelpie
                .store()
                .queued_socket_inbox_deliveries(waiter)
                .expect("queued")
                .is_empty()
            {
                acked = true;
                break;
            }
        }
        assert!(acked, "ack should complete the same queued attempt");
        let ack = read_json(&mut reader);
        assert_eq!(ack["result"]["outcome"], "accepted");
    }

    #[test]
    fn inbox_drains_a_large_body_as_one_json_line() {
        use crate::domain::{DeliveryOutcome, MessageKind, ReplyDisposition};
        let directory = tempfile::tempdir().expect("tempdir");
        let mut store = Store::in_memory().expect("store");
        let waiter = store
            .register_socket_waiter("inbox", Parent::Parentless, "large-waiter")
            .expect("register");
        let ask = store
            .insert_inbox_message(waiter.logical_agent_id, MessageKind::Ask, "q", None, None)
            .expect("ask");
        let body = "x".repeat(2_000_000);
        let reply = store
            .insert_inbox_message(
                waiter.logical_agent_id,
                MessageKind::Reply,
                &body,
                Some(ask),
                Some(ReplyDisposition::Final),
            )
            .expect("reply");
        store
            .record_socket_inbox_delivery(reply, waiter.logical_agent_id, DeliveryOutcome::Queued)
            .expect("queue");
        let (mut daemon, socket) = bind_inbox_daemon(directory.path(), store);
        let stream = claim_waiter(&socket, waiter.logical_agent_id, "claim-large");
        let client = thread::spawn(move || {
            let mut reader = BufReader::new(stream);
            let claim = read_json(&mut reader);
            assert_eq!(claim["result"]["claimed"], true);
            read_json(&mut reader)
        });
        while !client.is_finished() {
            daemon.poll().expect("poll large");
        }
        let delivery = client.join().expect("client");
        assert_eq!(delivery["method"], "inbox.delivery");
        assert_eq!(
            delivery["params"]["body"].as_str().expect("body").len(),
            2_000_000
        );
    }

    #[test]
    fn inbox_claim_keeps_a_pipelined_ack() {
        let directory = tempfile::tempdir().expect("tempdir");
        let mut store = Store::in_memory().expect("store");
        let (waiter, _, reply) = queue_reply_for_waiter(&mut store);
        let (mut daemon, socket) = bind_inbox_daemon(directory.path(), store);
        let mut stream = UnixStream::connect(&socket).expect("connect");
        let claim = serde_json::json!({
            "id": "claim-pipe",
            "method": "inbox.claim",
            "params": {"logical_agent_id": waiter},
        });
        let ack = serde_json::json!({
            "id": "ack-pipe",
            "method": "inbox.ack",
            "params": {"message_id": reply},
        });
        serde_json::to_writer(&mut stream, &claim).expect("claim");
        stream.write_all(b"\n").expect("nl");
        serde_json::to_writer(&mut stream, &ack).expect("ack");
        stream.write_all(b"\n").expect("nl");
        while daemon.inboxes.is_empty() {
            daemon.poll().expect("claim");
        }
        for _ in 0..20 {
            daemon.poll().expect("pipeline");
            if daemon
                .kelpie
                .store()
                .queued_socket_inbox_deliveries(waiter)
                .expect("queued")
                .is_empty()
            {
                break;
            }
        }
        assert!(
            daemon
                .kelpie
                .store()
                .queued_socket_inbox_deliveries(waiter)
                .expect("queued")
                .is_empty()
        );
    }

    #[test]
    fn inbox_half_close_still_receives_the_ack() {
        let directory = tempfile::tempdir().expect("tempdir");
        let mut store = Store::in_memory().expect("store");
        let (waiter, _, reply) = queue_reply_for_waiter(&mut store);
        let (mut daemon, socket) = bind_inbox_daemon(directory.path(), store);
        let mut stream = UnixStream::connect(&socket).expect("connect");
        serde_json::to_writer(
            &mut stream,
            &serde_json::json!({
                "id": "claim-half",
                "method": "inbox.claim",
                "params": {"logical_agent_id": waiter},
            }),
        )
        .expect("claim");
        stream.write_all(b"\n").expect("nl");
        serde_json::to_writer(
            &mut stream,
            &serde_json::json!({
                "id": "ack-half",
                "method": "inbox.ack",
                "params": {"message_id": reply},
            }),
        )
        .expect("ack");
        stream.write_all(b"\n").expect("nl");
        stream.shutdown(Shutdown::Write).expect("half-close");
        for _ in 0..20 {
            let _ = daemon.poll().expect("half-close poll");
        }
        let mut reader = BufReader::new(stream);
        let claim = read_json(&mut reader);
        assert_eq!(claim["result"]["claimed"], true);
        let delivery = read_json(&mut reader);
        assert_eq!(delivery["method"], "inbox.delivery");
        let ack = read_json(&mut reader);
        assert_eq!(ack["id"], "ack-half");
        assert_eq!(ack["result"]["outcome"], "accepted");
    }

    fn seed_socket_ask(store: &mut Store) -> (LogicalAgentId, LogicalAgentId, MessageId) {
        let waiter = store
            .register_socket_waiter("inbox", Parent::Parentless, "reply-waiter")
            .expect("register");
        let owing = store
            .declare_start(&test_intent("owing", "term-b", "reply-owing"))
            .expect("owing");
        let ask = store
            .create_ask(
                waiter.logical_agent_id,
                owing.logical_agent_id,
                owing.incarnation_id,
                "question",
                "socket-reply-ask",
            )
            .expect("ask");
        (
            waiter.logical_agent_id,
            owing.logical_agent_id,
            ask.message_id,
        )
    }

    #[test]
    fn socket_reply_final_resolves_only_after_inbox_ack() {
        let directory = tempfile::tempdir().expect("tempdir");
        let mut store = Store::in_memory().expect("store");
        let (waiter, owing, ask) = seed_socket_ask(&mut store);
        let (mut daemon, socket) = bind_inbox_daemon(directory.path(), store);
        let replied = rpc(
            &mut daemon,
            &socket,
            serde_json::json!({
                "id": "reply-1",
                "method": "reply",
                "params": {
                    "reply_to": ask,
                    "requester_agent_id": owing,
                    "body": "done",
                    "disposition": "final",
                    "idempotency_key": "socket-final",
                }
            }),
        );
        assert!(replied["error"].is_null(), "{replied}");
        assert_eq!(replied["result"]["delivery_outcome"], "queued");
        assert_eq!(replied["result"]["obligation_state"], "open");
        assert!(replied["result"]["recipient_incarnation"].is_null());
        assert!(replied["result"]["operation_id"].is_null());
        assert_eq!(
            daemon
                .kelpie
                .store()
                .obligation_state(ask)
                .expect("persist"),
            crate::domain::ObligationState::Open
        );

        let mut stream = claim_waiter(&socket, waiter, "claim-final");
        while daemon.inboxes.is_empty() {
            daemon.poll().expect("claim");
        }
        let mut reader = BufReader::new(stream.try_clone().expect("clone"));
        let _claim = read_json(&mut reader);
        let delivery = read_json(&mut reader);
        assert_eq!(delivery["method"], "inbox.delivery");
        assert_eq!(delivery["params"]["kind"], "reply");
        assert_eq!(delivery["params"]["disposition"], "final");
        assert_eq!(delivery["params"]["body"], "done");
        let reply_id = delivery["params"]["message_id"].clone();
        serde_json::to_writer(
            &mut stream,
            &serde_json::json!({
                "id": "ack-final",
                "method": "inbox.ack",
                "params": {"message_id": reply_id},
            }),
        )
        .expect("ack");
        stream.write_all(b"\n").expect("nl");
        for _ in 0..20 {
            daemon.poll().expect("ack poll");
            if daemon.kelpie.store().obligation_state(ask).expect("state")
                == crate::domain::ObligationState::Resolved
            {
                break;
            }
        }
        assert_eq!(
            daemon
                .kelpie
                .store()
                .obligation_state(ask)
                .expect("resolved"),
            crate::domain::ObligationState::Resolved
        );
        let second = rpc(
            &mut daemon,
            &socket,
            serde_json::json!({
                "id": "reply-2",
                "method": "reply",
                "params": {
                    "reply_to": ask,
                    "requester_agent_id": owing,
                    "body": "again",
                    "disposition": "final",
                    "idempotency_key": "socket-final-2",
                }
            }),
        );
        assert_eq!(second["error"]["class"], "conflict");
    }

    #[test]
    fn socket_cancel_reaches_the_waiter_inbox() {
        let directory = tempfile::tempdir().expect("tempdir");
        let mut store = Store::in_memory().expect("store");
        let (waiter, _, ask) = seed_socket_ask(&mut store);
        let (mut daemon, socket) = bind_inbox_daemon(directory.path(), store);
        let cancelled = rpc(
            &mut daemon,
            &socket,
            serde_json::json!({
                "id": "cancel-1",
                "method": "cancel",
                "params": {
                    "requester_agent_id": waiter,
                    "ask_message_id": ask,
                    "reason": "obsolete",
                }
            }),
        );
        assert!(cancelled["error"].is_null(), "{cancelled}");
        assert_eq!(cancelled["result"]["state"], "cancelled");
        assert_eq!(
            daemon.kelpie.store().obligation_state(ask).expect("state"),
            crate::domain::ObligationState::Cancelled
        );
        let delivery = drain_reply(&mut daemon, &socket, waiter, "claim-cancel");
        assert_eq!(delivery["method"], "inbox.delivery");
        assert_eq!(delivery["params"]["kind"], "cancellation");
        assert!(
            delivery["params"]["body"]
                .as_str()
                .expect("body")
                .contains("obsolete")
        );
    }
}
