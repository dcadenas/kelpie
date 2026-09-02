//! Typed access to Herdr's documented newline-delimited JSON socket protocol.

use std::io::{BufRead, BufReader, Read, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use thiserror::Error;

/// Exact Herdr protocol Kelpie supports.
pub const SUPPORTED_PROTOCOL: u32 = 20;

/// A minimal observed agent identity from Herdr.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct AgentObservation {
    pub terminal_id: String,
    pub pane_id: String,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub agent: Option<String>,
    #[serde(default)]
    pub interactive_ready: bool,
    #[serde(default)]
    pub launch_pending: bool,
    /// Backend-native conversation reference when Herdr provides it.
    #[serde(default)]
    pub agent_session: Option<Value>,
}

/// Herdr's current observed lifecycle status for an agent runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum AgentStatus {
    Idle,
    Working,
    Blocked,
    Done,
    #[default]
    Unknown,
}

/// Exact live identity plus Herdr's current lifecycle observation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LifecycleObservation {
    #[serde(flatten)]
    pub agent: AgentObservation,
    pub agent_status: AgentStatus,
}

/// A minimal live pane identity from Herdr's authoritative snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PaneObservation {
    pub pane_id: String,
    pub terminal_id: String,
    #[serde(default)]
    pub cwd: Option<String>,
}

/// Authoritative present-state baseline returned by Herdr.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snapshot {
    pub protocol: u32,
    pub panes: Vec<PaneObservation>,
    pub agents: Vec<AgentObservation>,
}

#[derive(Deserialize)]
struct WireSnapshot {
    protocol: u32,
    panes: Vec<PaneObservation>,
    agents: Vec<AgentObservation>,
}

#[derive(Deserialize)]
struct WireLifecycleSnapshot {
    protocol: u32,
    agents: Vec<LifecycleObservation>,
}

/// Errors retain transport and Herdr rejection distinctions.
#[derive(Debug, Error)]
pub enum HerdrError {
    #[error("Herdr is unavailable: {0}")]
    Unavailable(#[source] std::io::Error),
    #[error("Herdr response was malformed: {0}")]
    Malformed(#[source] serde_json::Error),
    #[error("Herdr rejected the request with {code}: {message}")]
    Rejected { code: String, message: String },
    #[error("Herdr protocol {actual} is incompatible; supported protocol is {supported}")]
    Incompatible { actual: u32, supported: u32 },
    #[error("Herdr returned unexpected result type {0}")]
    Unexpected(String),
    #[error("Herdr did not prove agent readiness before the {0:?} deadline")]
    ReadinessTimeout(Duration),
}

/// Direct client for a single Herdr Unix socket.
#[derive(Debug, Clone)]
pub struct HerdrClient {
    socket_path: PathBuf,
    timeout: Duration,
}

/// One established Herdr socket connection awaiting a single request.
#[derive(Debug)]
pub struct HerdrConnection {
    stream: UnixStream,
}

impl HerdrClient {
    /// Create a client for an explicit documented Herdr socket path.
    #[must_use]
    pub fn new(socket_path: impl AsRef<Path>, timeout: Duration) -> Self {
        Self {
            socket_path: socket_path.as_ref().to_path_buf(),
            timeout,
        }
    }

    /// Establish a socket before the caller commits its write-boundary marker.
    ///
    /// # Errors
    ///
    /// Returns `Unavailable` only before any request bytes can be written.
    pub fn connect(&self) -> Result<HerdrConnection, HerdrError> {
        let stream = UnixStream::connect(&self.socket_path).map_err(HerdrError::Unavailable)?;
        stream
            .set_read_timeout(Some(self.timeout))
            .map_err(HerdrError::Unavailable)?;
        stream
            .set_write_timeout(Some(self.timeout))
            .map_err(HerdrError::Unavailable)?;
        Ok(HerdrConnection { stream })
    }

    /// Wait until the Herdr socket exists, without connecting to it.
    ///
    /// Herdr's socket appears when Herdr starts, which can be later than a
    /// supervised Kelpie service starts. This polls the path only: probing by
    /// connecting would consume an `accept` on the peer, which is a real
    /// observable effect and is not safe to repeat.
    ///
    /// Socket presence is not readiness. The caller still performs the single
    /// connect that negotiates the protocol, so an unreachable or incompatible
    /// Herdr fails through the normal classified path.
    ///
    /// A zero `limit` makes exactly one check.
    ///
    /// # Errors
    ///
    /// Returns `Unavailable` when `limit` elapses with no socket at the path.
    pub fn wait_until_present(&self, limit: Duration, poll: Duration) -> Result<(), HerdrError> {
        let deadline = Instant::now() + limit;
        loop {
            if self.socket_path.exists() {
                return Ok(());
            }
            let now = Instant::now();
            if now >= deadline {
                return Err(HerdrError::Unavailable(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!(
                        "no Herdr socket at {} after {limit:?}",
                        self.socket_path.display()
                    ),
                )));
            }
            thread::sleep(poll.min(deadline - now));
        }
    }

    /// Negotiate exact protocol compatibility through `ping`.
    ///
    /// # Errors
    ///
    /// Returns a classified transport, protocol, or Herdr error.
    pub fn negotiate(&self) -> Result<(), HerdrError> {
        let value = self.request("kelpie:ping", "ping", &Value::Object(Map::default()))?;
        let protocol = value
            .get("protocol")
            .and_then(Value::as_u64)
            .ok_or_else(|| HerdrError::Unexpected("pong without protocol".into()))?;
        let protocol = u32::try_from(protocol)
            .map_err(|_| HerdrError::Unexpected("protocol exceeds u32".into()))?;
        validate_protocol(protocol)
    }

    /// Obtain a fresh authoritative `session.snapshot` baseline.
    ///
    /// # Errors
    ///
    /// Returns a classified transport, protocol, or Herdr error.
    pub fn snapshot(&self) -> Result<Snapshot, HerdrError> {
        let result = self.request(
            "kelpie:snapshot",
            "session.snapshot",
            &Value::Object(Map::default()),
        )?;
        let snapshot = result
            .get("snapshot")
            .ok_or_else(|| HerdrError::Unexpected("snapshot result without snapshot".into()))?;
        let snapshot: WireSnapshot =
            serde_json::from_value(snapshot.clone()).map_err(HerdrError::Malformed)?;
        if snapshot.protocol != SUPPORTED_PROTOCOL {
            return Err(HerdrError::Incompatible {
                actual: snapshot.protocol,
                supported: SUPPORTED_PROTOCOL,
            });
        }
        Ok(Snapshot {
            protocol: snapshot.protocol,
            panes: snapshot.panes,
            agents: snapshot.agents,
        })
    }

    /// Obtain fresh exact agent identities with their current lifecycle status.
    ///
    /// # Errors
    ///
    /// Returns a classified transport, protocol, or malformed-state error.
    pub fn lifecycle_snapshot(&self) -> Result<Vec<LifecycleObservation>, HerdrError> {
        let result = self.request(
            "kelpie:lifecycle-snapshot",
            "session.snapshot",
            &Value::Object(Map::default()),
        )?;
        let snapshot = result
            .get("snapshot")
            .ok_or_else(|| HerdrError::Unexpected("snapshot result without snapshot".into()))?;
        let snapshot: WireLifecycleSnapshot =
            serde_json::from_value(snapshot.clone()).map_err(HerdrError::Malformed)?;
        validate_protocol(snapshot.protocol)?;
        Ok(snapshot.agents)
    }

    /// Submit one typed `agent.start` request.
    ///
    /// # Errors
    ///
    /// Returns a classified transport or Herdr error. A transport error after
    /// request submission must be treated by the caller as an unknown outcome.
    pub fn start_agent(
        &self,
        request_id: &str,
        params: &Value,
    ) -> Result<AgentObservation, HerdrError> {
        let result = self.request(request_id, "agent.start", params)?;
        parse_agent_result(&result)
    }

    /// Submit one typed `agent.rename` request.
    ///
    /// # Errors
    ///
    /// Returns a classified transport or Herdr error. A transport error after
    /// request submission must be treated by the caller as an unknown outcome.
    pub fn rename_agent(
        &self,
        request_id: &str,
        target: &str,
        name: &str,
    ) -> Result<AgentObservation, HerdrError> {
        self.request(
            request_id,
            "agent.rename",
            &serde_json::json!({"target": target, "name": name}),
        )
        .and_then(|result| parse_agent_result(&result))
    }

    /// Submit one typed `pane.close` request.
    ///
    /// This ends the pane's process. Callers must prove the exact live binding
    /// first, because a pane reused by another agent closes just as readily.
    ///
    /// # Errors
    ///
    /// Returns a classified transport or Herdr error. A transport error after
    /// request submission must be treated by the caller as an unknown outcome.
    pub fn close_pane(&self, request_id: &str, pane_id: &str) -> Result<(), HerdrError> {
        // Herdr keys `pane.*` methods on `pane_id` and `agent.*` methods on
        // `target`. This is the only `pane.*` request Kelpie sends, so the
        // surrounding `target` convention does not apply and must not be copied
        // here; sending it is rejected as a missing `pane_id` field.
        self.request(
            request_id,
            "pane.close",
            &serde_json::json!({"pane_id": pane_id}),
        )
        .map(|_| ())
    }

    /// Read one agent by exact target, reconciling its managed start state.
    ///
    /// This is not interchangeable with reading the same agent out of a
    /// `session.snapshot`. `agent.get` reconciles the pane's managed-agent phase
    /// before answering, and a launch is promoted to interactive only when
    /// something reconciles it; `session.snapshot` is a pure read and promotes
    /// nothing. Polling a start for readiness therefore MUST use this method.
    ///
    /// # Errors
    ///
    /// Returns a classified transport or Herdr error. A target Herdr cannot
    /// resolve is a rejection, not an empty success.
    pub fn agent(&self, request_id: &str, target: &str) -> Result<AgentObservation, HerdrError> {
        self.request(
            request_id,
            "agent.get",
            &serde_json::json!({"target": target}),
        )
        .and_then(|result| parse_agent_result(&result))
    }

    /// Submit one typed `agent.prompt` request.
    ///
    /// # Errors
    ///
    /// Returns a classified transport or Herdr error. A transport error after
    /// request submission must be treated by the caller as an unknown outcome.
    pub fn prompt_agent(
        &self,
        request_id: &str,
        target: &str,
        text: &str,
    ) -> Result<AgentObservation, HerdrError> {
        self.request(
            request_id,
            "agent.prompt",
            &serde_json::json!({"target": target, "text": text}),
        )
        .and_then(|result| parse_agent_result(&result))
    }

    fn request(&self, id: &str, method: &str, params: &Value) -> Result<Value, HerdrError> {
        self.connect()?.request(id, method, params)
    }
}

impl HerdrConnection {
    /// Write one `agent.start` request on this established connection.
    ///
    /// # Errors
    ///
    /// Any returned transport or response error occurs at or after the external
    /// write boundary and therefore has an unknown operation outcome.
    pub fn start_agent(
        self,
        request_id: &str,
        params: &Value,
    ) -> Result<AgentObservation, HerdrError> {
        self.request(request_id, "agent.start", params)
            .and_then(|result| parse_agent_result(&result))
    }

    /// Write one `agent.rename` request on this established connection.
    ///
    /// # Errors
    ///
    /// Any returned transport or response error occurs at or after the external
    /// write boundary and therefore has an unknown operation outcome.
    pub fn rename_agent(
        self,
        request_id: &str,
        target: &str,
        name: &str,
    ) -> Result<AgentObservation, HerdrError> {
        self.request(
            request_id,
            "agent.rename",
            &serde_json::json!({"target": target, "name": name}),
        )
        .and_then(|result| parse_agent_result(&result))
    }

    /// Write one `agent.prompt` request on this established connection.
    ///
    /// # Errors
    ///
    /// Any returned transport or response error occurs at or after the external
    /// write boundary and therefore has an unknown operation outcome.
    pub fn prompt_agent(
        self,
        request_id: &str,
        target: &str,
        text: &str,
    ) -> Result<AgentObservation, HerdrError> {
        self.request(
            request_id,
            "agent.prompt",
            &serde_json::json!({"target": target, "text": text}),
        )
        .and_then(|result| parse_agent_result(&result))
    }

    fn request(self, id: &str, method: &str, params: &Value) -> Result<Value, HerdrError> {
        request_over_stream(self.stream, id, method, params)
    }
}

fn parse_agent_result(result: &Value) -> Result<AgentObservation, HerdrError> {
    let agent = result
        .get("agent")
        .ok_or_else(|| HerdrError::Unexpected("agent result without agent".into()))?;
    serde_json::from_value(agent.clone()).map_err(HerdrError::Malformed)
}

fn validate_protocol(protocol: u32) -> Result<(), HerdrError> {
    if protocol == SUPPORTED_PROTOCOL {
        Ok(())
    } else {
        Err(HerdrError::Incompatible {
            actual: protocol,
            supported: SUPPORTED_PROTOCOL,
        })
    }
}

fn request_over_stream(
    mut stream: impl Read + Write,
    id: &str,
    method: &str,
    params: &Value,
) -> Result<Value, HerdrError> {
    serde_json::to_writer(
        &mut stream,
        &serde_json::json!({"id": id, "method": method, "params": params}),
    )
    .map_err(HerdrError::Malformed)?;
    stream.write_all(b"\n").map_err(HerdrError::Unavailable)?;
    stream.flush().map_err(HerdrError::Unavailable)?;
    if let Some(point) = after_write_fault_point(id, method) {
        crate::test_fault::pause(point);
    }

    let mut line = String::new();
    BufReader::new(stream)
        .read_line(&mut line)
        .map_err(HerdrError::Unavailable)?;
    let response: Value = serde_json::from_str(&line).map_err(HerdrError::Malformed)?;
    if let Some(error) = response.get("error") {
        return Err(HerdrError::Rejected {
            code: error
                .get("code")
                .and_then(Value::as_str)
                .unwrap_or("unknown")
                .into(),
            message: error
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("Herdr rejected request")
                .into(),
        });
    }
    response
        .get("result")
        .cloned()
        .ok_or_else(|| HerdrError::Unexpected("response without result".into()))
}

fn after_write_fault_point(id: &str, method: &str) -> Option<&'static str> {
    match (method, id) {
        ("agent.start", id) if id.starts_with("kelpie:start:") => {
            Some("start_after_write_before_response")
        }
        ("agent.prompt", id) if id.starts_with("kelpie:ask:") => {
            Some("ask_after_write_before_response")
        }
        ("agent.prompt", id) if id.starts_with("kelpie:tell:") => {
            Some("tell_after_write_before_response")
        }
        ("agent.prompt", id) if id.starts_with("kelpie:initial:to:") => {
            Some("initial_message_after_write_before_response")
        }
        ("agent.prompt", id) if id.starts_with("kelpie:reply:") => {
            Some("reply_after_write_before_response")
        }
        ("agent.prompt", id) if id.starts_with("kelpie:owing-cancellation:") => {
            Some("owing_cancellation_after_write_before_response")
        }
        ("agent.prompt", id) if id.starts_with("kelpie:cancellation:") => {
            Some("cancellation_after_write_before_response")
        }
        ("agent.prompt", id) if id.starts_with("kelpie:clear:") => {
            Some("clear_after_write_before_response")
        }
        ("agent.rename", id) if id.starts_with("kelpie:adopt-rename:") => {
            Some("adopt_rename_after_write_before_response")
        }
        ("pane.close", id) if id.starts_with("kelpie:retire-close:") => {
            Some("retire_after_write_before_response")
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::os::unix::net::UnixListener;
    use std::thread;

    use super::*;

    #[test]
    fn waits_for_a_herdr_socket_that_appears_late() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("herdr.sock");
        let bind_path = path.clone();
        let server = thread::spawn(move || {
            thread::sleep(Duration::from_millis(150));
            UnixListener::bind(&bind_path).expect("bind fixture socket")
        });

        let client = HerdrClient::new(&path, Duration::from_secs(1));
        client
            .wait_until_present(Duration::from_secs(5), Duration::from_millis(10))
            .expect("socket appears");
        drop(server.join().expect("fixture thread"));
    }

    /// Waiting must not consume the peer's single `accept`; the caller's own
    /// request has to be the first connection the fixture ever sees.
    #[test]
    fn waiting_does_not_connect_to_the_socket() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("herdr.sock");
        let listener = UnixListener::bind(&path).expect("bind fixture socket");
        let server = thread::spawn(move || {
            let (stream, _) = listener.accept().expect("accept fixture client");
            let mut reader = BufReader::new(stream.try_clone().expect("clone stream"));
            let mut request = String::new();
            reader.read_line(&mut request).expect("read request");
            let request: Value = serde_json::from_str(&request).expect("valid request");
            assert_eq!(request["method"], "ping");
            let mut stream = stream;
            stream.write_all(b"{\"id\":\"kelpie:ping\",\"result\":{\"type\":\"pong\",\"version\":\"test\",\"protocol\":20}}\n").expect("write response");
        });

        let client = HerdrClient::new(&path, Duration::from_secs(5));
        client
            .wait_until_present(Duration::from_secs(5), Duration::from_millis(10))
            .expect("socket is present");
        client.negotiate().expect("first accept is the ping");
        server.join().expect("fixture thread");
    }

    #[test]
    fn waiting_reports_unavailable_when_the_limit_elapses() {
        let directory = tempfile::tempdir().expect("tempdir");
        let client = HerdrClient::new(directory.path().join("absent.sock"), Duration::from_secs(1));
        let error = client
            .wait_until_present(Duration::from_millis(50), Duration::from_millis(10))
            .expect_err("no socket ever appears");
        assert!(matches!(error, HerdrError::Unavailable(_)));
    }

    #[test]
    fn a_zero_limit_makes_exactly_one_check() {
        let directory = tempfile::tempdir().expect("tempdir");
        let client = HerdrClient::new(directory.path().join("absent.sock"), Duration::from_secs(1));
        let started = Instant::now();
        let error = client
            .wait_until_present(Duration::ZERO, Duration::from_secs(30))
            .expect_err("no socket exists");
        assert!(matches!(error, HerdrError::Unavailable(_)));
        assert!(started.elapsed() < Duration::from_secs(1));
    }

    #[test]
    fn negotiates_protocol_over_ndjson_socket() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("herdr.sock");
        let listener = UnixListener::bind(&path).expect("bind fixture socket");
        let server = thread::spawn(move || {
            let (stream, _) = listener.accept().expect("accept fixture client");
            let mut reader = BufReader::new(stream.try_clone().expect("clone stream"));
            let mut request = String::new();
            reader.read_line(&mut request).expect("read request");
            let request: Value = serde_json::from_str(&request).expect("valid request");
            assert_eq!(request["method"], "ping");
            let mut stream = stream;
            stream.write_all(b"{\"id\":\"kelpie:ping\",\"result\":{\"type\":\"pong\",\"version\":\"test\",\"protocol\":20,\"additive\":true}}\n").expect("write response");
        });
        HerdrClient::new(&path, Duration::from_secs(1))
            .negotiate()
            .expect("compatible fixture");
        server.join().expect("fixture server");
    }

    #[test]
    fn closes_a_pane_by_the_pane_id_field_herdr_requires() {
        // Herdr's schema keys every `pane.*` method on `pane_id` and every
        // `agent.*` method on `target`. Sending `target` here is rejected as a
        // missing `pane_id`, so the exact field name is the contract and not an
        // internal detail: assert the bytes, not just that a request was sent.
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("herdr.sock");
        let listener = UnixListener::bind(&path).expect("bind fixture socket");
        let server = thread::spawn(move || {
            let (stream, _) = listener.accept().expect("accept fixture client");
            let mut reader = BufReader::new(stream.try_clone().expect("clone stream"));
            let mut request = String::new();
            reader.read_line(&mut request).expect("read request");
            let request: Value = serde_json::from_str(&request).expect("valid request");
            assert_eq!(request["method"], "pane.close");
            assert_eq!(request["params"]["pane_id"], "w7:p2");
            assert!(
                request["params"].get("target").is_none(),
                "pane.close must not carry the agent-target field"
            );
            let mut stream = stream;
            stream
                .write_all(b"{\"id\":\"kelpie:retire-close:x\",\"result\":{\"type\":\"ok\"}}\n")
                .expect("write response");
        });
        HerdrClient::new(&path, Duration::from_secs(1))
            .close_pane("kelpie:retire-close:x", "w7:p2")
            .expect("fixture accepts the close");
        server.join().expect("fixture server");
    }

    #[test]
    fn rejects_protocol_mismatch_explicitly() {
        for actual in [19_u32, 21] {
            assert!(
                matches!(
                    validate_protocol(actual),
                    Err(HerdrError::Incompatible {
                        actual: got,
                        supported: 20
                    }) if got == actual
                ),
                "expected incompatible for protocol {actual}"
            );
        }
    }

    #[test]
    fn negotiate_refuses_protocol_19() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("herdr.sock");
        let listener = UnixListener::bind(&path).expect("bind fixture socket");
        let server = thread::spawn(move || {
            let (stream, _) = listener.accept().expect("accept fixture client");
            let mut reader = BufReader::new(stream.try_clone().expect("clone stream"));
            let mut request = String::new();
            reader.read_line(&mut request).expect("read request");
            let mut stream = stream;
            stream
                .write_all(b"{\"id\":\"kelpie:ping\",\"result\":{\"type\":\"pong\",\"version\":\"0.8.0\",\"protocol\":19}}\n")
                .expect("write response");
        });
        let error = HerdrClient::new(&path, Duration::from_secs(1))
            .negotiate()
            .expect_err("protocol 19");
        assert!(
            matches!(
                error,
                HerdrError::Incompatible {
                    actual: 19,
                    supported: 20
                }
            ),
            "{error:?}"
        );
        server.join().expect("fixture server");
    }
}
