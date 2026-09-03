//! Foreground local daemon for Kelpie's newline-delimited JSON protocol.

use std::collections::HashSet;
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
    AdoptIntent, IncarnationId, LogicalAgentId, MessageId, Parent, RenewId, RenewTimeout,
    ReplyDisposition, StartIntent,
};
use crate::herdr::HerdrError;
use crate::slice::{AwaitingClear, ClearResult, ClearSubmission, Kelpie, SliceError};
use crate::store::StoreError;

const DEFAULT_REMINDER_INTERVAL_MS: i64 = 300_000;

/// One local client request. Sender fields are same-user attribution, not authentication.
#[derive(Debug, Deserialize)]
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
struct AwaitingStart {
    request_id: String,
    intent: StartIntent,
    declared: crate::store::DeclaredStart,
    deadline: Instant,
    stream: UnixStream,
}

#[derive(Debug)]
struct ResolvedClear {
    recipient: LogicalAgentId,
    recipient_incarnation: IncarnationId,
    idempotency_key: String,
}

#[derive(Debug)]
enum AwaitingClearState {
    Settling {
        clear: ResolvedClear,
        not_before_ms: i64,
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

/// A bound foreground daemon. Dropping it removes only the socket it created.
#[derive(Debug)]
pub struct Daemon {
    listener: UnixListener,
    socket_path: PathBuf,
    kelpie: Kelpie,
    awaiting_starts: Vec<AwaitingStart>,
    awaiting_clears: Vec<AwaitingClearRequest>,
    inboxes: Vec<InboxSession>,
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
        Ok(Self {
            listener,
            socket_path,
            kelpie,
            awaiting_starts: Vec::new(),
            awaiting_clears: Vec::new(),
            inboxes: Vec::new(),
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

    /// Fire due deliveries, then accept one client if one is waiting.
    ///
    /// # Errors
    ///
    /// Returns for socket, request decoding, or response encoding failures.
    pub fn poll(&mut self) -> Result<bool, DaemonError> {
        if let Err(error) = self.kelpie.fire_due_deliveries() {
            let _ = self
                .kelpie
                .store_mut()
                .create_operator_notice(&format!("due fire failed: {error}"));
        }
        if let Err(error) = self.kelpie.fire_due_reminders() {
            let _ = self
                .kelpie
                .store_mut()
                .create_operator_notice(&format!("reminder fire failed: {error}"));
        }
        let renewed = match self.kelpie.drive_renews() {
            Ok(count) => count > 0,
            Err(error) => {
                let _ = self
                    .kelpie
                    .store_mut()
                    .create_operator_notice(&format!("renew drive failed: {error}"));
                false
            }
        };
        let start_advanced = self.advance_awaiting_starts();
        let clear_advanced = self.advance_awaiting_clears();
        let inbox_advanced = self.advance_inboxes();
        let advanced = start_advanced || clear_advanced || inbox_advanced || renewed;
        self.listener.set_nonblocking(true)?;
        match self.listener.accept() {
            Ok((stream, _)) => {
                stream.set_nonblocking(false)?;
                // Serving one client is best effort. A caller that times out or
                // has its pipe closed hangs up mid-exchange, and the write then
                // fails with a broken pipe — routine client behavior. This
                // daemon coordinates a whole fleet, so it must not exit because
                // one caller went away; the failure is logged and the loop
                // continues. Durable state is unaffected either way, since the
                // response is written after the operation is already committed.
                match serve_stream(stream, &mut self.kelpie) {
                    Ok(Served::Answered) => {}
                    Ok(Served::AwaitingStart(awaiting)) => self.awaiting_starts.push(*awaiting),
                    Ok(Served::AwaitingClear(awaiting)) => self.awaiting_clears.push(*awaiting),
                    Ok(Served::Inbox(session)) => self.park_inbox(*session),
                    Err(error) => {
                        eprintln!("kelpied: client connection failed: {error}");
                    }
                }
                Ok(true)
            }
            Err(error)
                if error.kind() == std::io::ErrorKind::WouldBlock
                    || error.kind() == std::io::ErrorKind::Interrupted =>
            {
                // A parked start or clear is work in progress, so report it as
                // activity: idling until the next due time would leave its
                // runtime transition unobserved for that whole sleep.
                Ok(
                    advanced
                        || !self.awaiting_starts.is_empty()
                        || !self.awaiting_clears.is_empty(),
                )
            }
            Err(error) => Err(error.into()),
        }
    }

    /// Observe every awaiting start once, answering the ones that settle.
    ///
    /// Returns whether any start settled, which the caller uses to keep the loop
    /// hot rather than sleeping through a readiness transition.
    fn advance_awaiting_starts(&mut self) -> bool {
        let mut settled = false;
        let mut still_waiting = Vec::with_capacity(self.awaiting_starts.len());
        for mut awaiting in std::mem::take(&mut self.awaiting_starts) {
            let progress = self.kelpie.advance_start_ready(
                &awaiting.intent,
                &awaiting.declared,
                awaiting.deadline,
            );
            let result = match progress {
                Ok(None) => {
                    still_waiting.push(awaiting);
                    continue;
                }
                Ok(Some(started)) => self.kelpie.finish_launch(&awaiting.intent, started),
                Err(error) => {
                    self.kelpie.note_undelivered_brief(&awaiting.intent, &error);
                    Err(error)
                }
            };
            settled = true;
            let response = respond(&awaiting.request_id, result.map(launch_result));
            if let Err(error) = write_response(&mut awaiting.stream, &response) {
                // The caller stopped waiting. Its outcome is already durable, so
                // only the receipt is lost.
                eprintln!("kelpied: parked start response failed: {error}");
            }
        }
        self.awaiting_starts = still_waiting;
        settled
    }

    fn advance_awaiting_clears(&mut self) -> bool {
        let mut settled = false;
        let mut still_waiting = Vec::with_capacity(self.awaiting_clears.len());
        for awaiting in std::mem::take(&mut self.awaiting_clears) {
            let AwaitingClearRequest {
                request_id,
                state,
                mut stream,
            } = awaiting;
            match advance_clear_state(state, &mut self.kelpie) {
                Ok(ClearDispatch::Awaiting(state)) => still_waiting.push(AwaitingClearRequest {
                    request_id,
                    state,
                    stream,
                }),
                result => {
                    settled = true;
                    let result = result.and_then(|progress| match progress {
                        ClearDispatch::Complete(cleared) => Ok(clear_result(cleared)),
                        ClearDispatch::Awaiting(_) => unreachable!(),
                    });
                    let response = respond(&request_id, result);
                    if let Err(error) = write_response(&mut stream, &response) {
                        eprintln!("kelpied: parked clear response failed: {error}");
                    }
                }
            }
        }
        self.awaiting_clears = still_waiting;
        settled
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
                let settled = self
                    .kelpie
                    .wait_for_start_ready(&awaiting.intent, &awaiting.declared, awaiting.deadline)
                    .and_then(|started| self.kelpie.finish_launch(&awaiting.intent, started));
                let response = respond(&awaiting.request_id, settled.map(launch_result));
                write_response(&mut awaiting.stream, &response)
            }
            Served::Inbox(session) => {
                self.park_inbox(*session);
                Ok(())
            }
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
}

fn serve_stream(stream: UnixStream, kelpie: &mut Kelpie) -> Result<Served, DaemonError> {
    // Read the request through a BufReader, then reclaim the underlying stream
    // for the response. Avoid try_clone so no extra FD can keep the peer from
    // observing close/half-close after a long composed start.
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    reader.read_line(&mut line)?;
    let response = match serde_json::from_str::<ClientRequest>(&line) {
        Ok(request) if request.method == "start" || request.method == "handoff" => {
            let mut stream = reader.into_inner();
            match submit_start(request.params, kelpie) {
                Ok((intent, declared, deadline)) => {
                    return Ok(Served::AwaitingStart(Box::new(AwaitingStart {
                        request_id: request.id,
                        intent,
                        declared,
                        deadline,
                        stream,
                    })));
                }
                Err(error) => {
                    let response = respond(&request.id, Err(error));
                    write_response(&mut stream, &response)?;
                    return Ok(Served::Answered);
                }
            }
        }
        Ok(request) if request.method == "inbox.claim" => {
            let leftover = reader.buffer().to_vec();
            let mut stream = reader.into_inner();
            match claim_inbox(&request, kelpie) {
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
            }
        }
        Ok(request) if request.method == "clear" => {
            let mut stream = reader.into_inner();
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
                    })));
                }
                Err(error) => {
                    let response = respond(&request.id, Err(error));
                    write_response(&mut stream, &response)?;
                    return Ok(Served::Answered);
                }
            }
        }
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
    let mut stream = reader.into_inner();
    write_response(&mut stream, &response)?;
    Ok(Served::Answered)
}

fn write_response(stream: &mut UnixStream, response: &ClientResponse) -> Result<(), DaemonError> {
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

/// Declare and submit a start, leaving its readiness to the poll loop.
fn submit_start(
    params: Value,
    kelpie: &mut Kelpie,
) -> Result<(StartIntent, crate::store::DeclaredStart, Instant), SliceError> {
    let intent = serde_json::from_value::<StartIntent>(params)
        .map_err(|error| SliceError::Store(StoreError::InvalidRecord(error.to_string())))?;
    kelpie
        .validate_launch(&intent)
        .and_then(|()| kelpie.validate_handoff(&intent))
        .and_then(|()| kelpie.submit_start(&intent))
        .map(|(declared, deadline)| (intent, declared, deadline))
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
                "live": claimant.has_ready_incarnation,
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
    let params = serde_json::from_value::<AttributionParams>(params)
        .map_err(|error| SliceError::Store(StoreError::InvalidRecord(error.to_string())))?;
    let selectors = usize::from(params.incarnation_id.is_some())
        + usize::from(params.agent_id.is_some())
        + usize::from(params.alias.is_some())
        + usize::from(params.pane_id.is_some());
    if selectors != 1 {
        return Err(SliceError::Store(StoreError::InvalidRecord(
            "provide exactly one of incarnation_id, agent_id, alias, or pane_id".into(),
        )));
    }

    let incarnation_id = if let Some(incarnation_id) = params.incarnation_id {
        incarnation_id
    } else if let Some(agent_id) = params.agent_id {
        kelpie
            .store()
            .newest_incarnation_for_agent(agent_id)
            .map_err(SliceError::Store)?
    } else if let Some(alias) = params.alias {
        kelpie.resolve_ready_alias(&alias)?.1
    } else {
        // Read-only lookup: never adopts, unlike the whoami pane form.
        let pane_id = params.pane_id.unwrap_or_default();
        kelpie
            .store()
            .ready_identity_for_pane(&pane_id)
            .map_err(SliceError::Store)?
            .incarnation_id
    };

    let reason = if params.refresh {
        kelpie.refresh_attribution(incarnation_id)?
    } else {
        None
    };
    let evidence = kelpie
        .store()
        .attribution_evidence(incarnation_id)
        .map_err(SliceError::Store)?;
    let mut result = attribution_result(&evidence);
    // Diagnostic, not evidence: why this refresh could not determine anything.
    if let Some(reason) = reason {
        result["undetermined_because"] = Value::String(reason);
    }
    Ok(result)
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
    let report = kelpie.store().report().map_err(SliceError::Store)?;

    // Live status is Herdr's fact, not Kelpie's, so it is opt-in and carries the
    // moment it was taken rather than being presented as durable state.
    let live = if params.live {
        Some(kelpie.live_agent_status()?)
    } else {
        None
    };

    let keep = if params.active {
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
                .map(|incarnation| report_incarnation(incarnation, live.as_ref()))
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
            !params.active
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

fn dispatch_recover(kelpie: &mut Kelpie) -> Result<Value, SliceError> {
    kelpie.recover().map(|report| {
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
    })
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
    let ended = kelpie.retire_waiter(params.logical_agent_id)?;
    Ok(serde_json::json!({
        "logical_agent_id": params.logical_agent_id,
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
    }))
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
    let (recipient, recipient_incarnation) = resolve_recipient(
        kelpie,
        params.recipient,
        params.recipient_incarnation,
        params.recipient_alias.as_deref(),
        &format!("{}:lazy-adopt:recipient", params.idempotency_key),
    )?;
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
    let mut result = serde_json::json!({
        "message_id": created.message_id,
        "operation_id": created.operation_id,
        "recipient": recipient,
        "recipient_incarnation": recipient_incarnation,
        "delivery_outcome": delivery_outcome
    });
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
    advance_clear_state(
        AwaitingClearState::Settling {
            clear: ResolvedClear {
                recipient,
                recipient_incarnation,
                idempotency_key: params.idempotency_key,
            },
            not_before_ms,
        },
        kelpie,
    )
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
    logical_agent_id: LogicalAgentId,
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

/// Exactly one selector. More than one is ambiguous and fails closed.
#[derive(Debug, Deserialize)]
struct AttributionParams {
    #[serde(default)]
    incarnation_id: Option<IncarnationId>,
    #[serde(default)]
    agent_id: Option<LogicalAgentId>,
    #[serde(default)]
    alias: Option<String>,
    #[serde(default)]
    pane_id: Option<String>,
    /// Observe again before reporting, appending a new observation.
    #[serde(default)]
    refresh: bool,
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
    use std::time::Duration;

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
        while !daemon.poll().expect("poll survives a hangup") {}

        // The next client is served normally, proving the loop is still alive.
        let next = socket.clone();
        let client = thread::spawn(move || {
            send_request(
                &next,
                &serde_json::json!({"id": "after", "method": "not-a-method", "params": {}}),
            )
        });
        while !daemon.poll().expect("poll serves the next client") {}
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
        assert!(!daemon.poll().expect("poll without client"));
        assert_eq!(
            daemon
                .kelpie
                .store()
                .delivery_outcome(tell.operation_id)
                .expect("delivery"),
            crate::domain::DeliveryOutcome::Accepted
        );
        server.join().expect("herdr");
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
        while !daemon.poll().expect("rpc poll") {}
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
