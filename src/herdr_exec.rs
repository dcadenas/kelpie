//! Off-thread Herdr I/O so the daemon loop never waits on a socket.

use std::cell::Cell;
use std::collections::{HashMap, VecDeque};
use std::sync::mpsc::{self, Receiver, Sender, TryRecvError};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use serde_json::Value;

use crate::herdr::{
    AgentObservation, HerdrClient, HerdrError, LifecycleObservation, Snapshot, parse_agent_result,
    request_over_stream,
};

/// Parallel Herdr workers. Snapshots share one lane; each pane has its own.
const DEFAULT_WORKERS: usize = 4;
/// Guard only. The daemon is single-threaded, so a lease this old is a bug.
const LEASE_TTL: Duration = Duration::from_secs(30);

/// One unit of Herdr work. FIFO within a [`Lane`].
#[derive(Debug)]
pub enum HerdrJob {
    /// `session.snapshot` on the snapshot lane.
    Snapshot { job_id: u64, negotiate: bool },
    /// `session.snapshot` with lifecycle status, snapshot lane.
    LifecycleSnapshot { job_id: u64 },
    /// Connect (and optionally negotiate on a second socket) on a pane lane.
    Open {
        job_id: u64,
        pane_id: String,
        negotiate: bool,
    },
    /// `agent.get` on a pane lane. Connects per call; does not hold a lease.
    AgentGet {
        job_id: u64,
        request_id: String,
        target: String,
    },
}

/// Follow-up on an open lease. Routed to the worker that holds the socket.
#[derive(Debug)]
pub enum LeaseCmd {
    /// One request on the leased connection.
    Send {
        request_id: String,
        method: String,
        params: Value,
        after_write_pause: &'static str,
    },
    /// Close the socket and return the worker to the pool.
    Drop,
}

/// Where a lease or request failed relative to the write boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailPhase {
    Connect,
    Write,
    Read,
    Lease,
}

/// Completion (and optional write-boundary) for one job.
#[derive(Debug)]
pub enum HerdrEvent {
    /// The pane lane holds an open socket. No mutation has been written.
    Opened {
        job_id: u64,
        lease: Sender<LeaseCmd>,
    },
    /// Request bytes were flushed; the response has not been read yet.
    Written {
        job_id: u64,
        after_write_pause: &'static str,
    },
    /// A `Send` finished.
    Done {
        job_id: u64,
        result: Result<HerdrJobResult, HerdrError>,
    },
    /// Open, write, read, or lease-timeout failure.
    Failed {
        job_id: u64,
        phase: FailPhase,
        error: HerdrError,
    },
    /// The worker dropped the socket and the lane is free.
    Dropped { job_id: u64 },
}

/// Successful Herdr payload for a finished job.
#[derive(Clone, Debug)]
pub enum HerdrJobResult {
    Snapshot(Snapshot),
    Lifecycle(Vec<LifecycleObservation>),
    Prompt(AgentObservation),
    Agent(AgentObservation),
    Closed,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum Lane {
    Snapshot,
    Pane(String),
}

/// Bounded pool that runs Herdr requests off the daemon thread.
pub struct HerdrExec {
    job_tx: Sender<HerdrJob>,
    event_rx: Receiver<HerdrEvent>,
    queued: HashMap<Lane, VecDeque<HerdrJob>>,
    in_flight: HashMap<Lane, u64>,
    coalesced: HashMap<u64, Vec<u64>>,
    pending_events: VecDeque<HerdrEvent>,
    /// Join handles so threads are not detached without a record.
    _workers: Vec<JoinHandle<()>>,
}

impl std::fmt::Debug for HerdrExec {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("HerdrExec")
            .field("queued_lanes", &self.queued.len())
            .field("in_flight", &self.in_flight.len())
            .finish_non_exhaustive()
    }
}

impl HerdrExec {
    /// Spawn the default worker pool.
    #[must_use]
    pub fn spawn(client: HerdrClient) -> Self {
        Self::spawn_workers(client, DEFAULT_WORKERS)
    }

    /// Spawn `workers` threads (at least one).
    #[must_use]
    #[allow(clippy::needless_pass_by_value)]
    pub fn spawn_workers(client: HerdrClient, workers: usize) -> Self {
        let workers = workers.max(1);
        let (job_tx, job_rx) = mpsc::channel();
        let (event_tx, event_rx) = mpsc::channel();
        let job_rx = Arc::new(Mutex::new(job_rx));
        let mut handles = Vec::with_capacity(workers);
        for _ in 0..workers {
            let client = client.clone();
            let event_tx = event_tx.clone();
            let job_rx = Arc::clone(&job_rx);
            handles.push(thread::spawn(move || {
                worker_loop(&client, &job_rx, &event_tx);
            }));
        }
        Self {
            job_tx,
            event_rx,
            queued: HashMap::new(),
            in_flight: HashMap::new(),
            coalesced: HashMap::new(),
            pending_events: VecDeque::new(),
            _workers: handles,
        }
    }

    /// Queue `job` behind any in-flight work on its lane, or dispatch it now.
    ///
    /// A second snapshot queued behind another not-yet-dispatched snapshot
    /// shares that queued request. Every submitted job id still receives the
    /// terminal event.
    pub fn submit(&mut self, job: HerdrJob) {
        let lane = lane_for(&job);
        if let Some(queue) = self.queued.get_mut(&lane) {
            if let Some(leader) = coalescing_leader(queue, &job) {
                self.coalesced
                    .entry(leader)
                    .or_default()
                    .push(job_id_of(&job));
                return;
            }
            queue.push_back(job);
            return;
        }
        if self.in_flight.contains_key(&lane) {
            // Never join an in-flight snapshot: a later occupancy or confirm
            // check must postdate that request. Coalesce only with another
            // snapshot that is queued and has not been dispatched.
            if let Some(leader) = self
                .queued
                .get(&lane)
                .and_then(|queue| coalescing_leader(queue, &job))
            {
                self.coalesced
                    .entry(leader)
                    .or_default()
                    .push(job_id_of(&job));
                return;
            }
            self.queued.entry(lane).or_default().push_back(job);
            return;
        }
        self.dispatch(lane, job);
    }

    /// Non-blocking event drain helper for one event.
    pub fn try_recv(&mut self) -> Option<HerdrEvent> {
        if let Some(event) = self.pending_events.pop_front() {
            return Some(event);
        }
        match self.event_rx.try_recv() {
            Ok(event) => {
                if let HerdrEvent::Done { job_id, .. } = &event
                    && let Some(followers) = self.coalesced.get(job_id)
                {
                    self.pending_events.extend(
                        followers
                            .iter()
                            .copied()
                            .map(|follower| clone_follower_event(&event, follower)),
                    );
                }
                if let Some(job_id) = terminal_job_id(&event) {
                    if let Some(followers) = self.coalesced.remove(&job_id) {
                        self.pending_events.extend(
                            followers
                                .into_iter()
                                .map(|follower| clone_follower_event(&event, follower)),
                        );
                    }
                    let lane = self
                        .in_flight
                        .iter()
                        .find_map(|(lane, id)| (*id == job_id).then(|| lane.clone()));
                    if let Some(lane) = lane {
                        self.in_flight.remove(&lane);
                        if let Some(queue) = self.queued.get_mut(&lane)
                            && let Some(next) = queue.pop_front()
                        {
                            if queue.is_empty() {
                                self.queued.remove(&lane);
                            }
                            self.dispatch(lane, next);
                        }
                    }
                }
                Some(event)
            }
            Err(TryRecvError::Empty | TryRecvError::Disconnected) => None,
        }
    }

    fn dispatch(&mut self, lane: Lane, job: HerdrJob) {
        let job_id = job_id_of(&job);
        self.in_flight.insert(lane, job_id);
        let _ = self.job_tx.send(job);
    }
}

fn terminal_job_id(event: &HerdrEvent) -> Option<u64> {
    match event {
        HerdrEvent::Dropped { job_id } | HerdrEvent::Failed { job_id, .. } => Some(*job_id),
        HerdrEvent::Opened { .. } | HerdrEvent::Written { .. } | HerdrEvent::Done { .. } => None,
    }
}

fn coalescing_leader(queue: &VecDeque<HerdrJob>, job: &HerdrJob) -> Option<u64> {
    queue.iter().find_map(|queued| match (queued, job) {
        (HerdrJob::Snapshot { job_id, .. }, HerdrJob::Snapshot { .. })
        | (HerdrJob::LifecycleSnapshot { job_id }, HerdrJob::LifecycleSnapshot { .. }) => {
            Some(*job_id)
        }
        _ => None,
    })
}

fn clone_follower_event(event: &HerdrEvent, job_id: u64) -> HerdrEvent {
    match event {
        HerdrEvent::Done { result, .. } => HerdrEvent::Done {
            job_id,
            result: match result {
                Ok(value) => Ok(value.clone()),
                Err(error) => Err(clone_herdr_error(error)),
            },
        },
        HerdrEvent::Failed { phase, error, .. } => HerdrEvent::Failed {
            job_id,
            phase: *phase,
            error: clone_herdr_error(error),
        },
        HerdrEvent::Dropped { .. } => HerdrEvent::Dropped { job_id },
        HerdrEvent::Opened { .. } | HerdrEvent::Written { .. } => {
            unreachable!("only completion events are fanned out")
        }
    }
}

fn clone_herdr_error(error: &HerdrError) -> HerdrError {
    match error {
        HerdrError::Unavailable(source) => {
            HerdrError::Unavailable(std::io::Error::new(source.kind(), source.to_string()))
        }
        HerdrError::Malformed(source) => HerdrError::Malformed(serde_json::Error::io(
            std::io::Error::new(std::io::ErrorKind::InvalidData, source.to_string()),
        )),
        HerdrError::Rejected { code, message } => HerdrError::Rejected {
            code: code.clone(),
            message: message.clone(),
        },
        HerdrError::Incompatible { actual, supported } => HerdrError::Incompatible {
            actual: *actual,
            supported: *supported,
        },
        HerdrError::Unexpected(message) => HerdrError::Unexpected(message.clone()),
        HerdrError::ReadinessTimeout(duration) => HerdrError::ReadinessTimeout(*duration),
    }
}

fn job_id_of(job: &HerdrJob) -> u64 {
    match job {
        HerdrJob::Snapshot { job_id, .. }
        | HerdrJob::LifecycleSnapshot { job_id }
        | HerdrJob::Open { job_id, .. }
        | HerdrJob::AgentGet { job_id, .. } => *job_id,
    }
}

fn lane_for(job: &HerdrJob) -> Lane {
    match job {
        HerdrJob::Snapshot { .. } | HerdrJob::LifecycleSnapshot { .. } => Lane::Snapshot,
        HerdrJob::Open { pane_id, .. }
        | HerdrJob::AgentGet {
            target: pane_id, ..
        } => Lane::Pane(pane_id.clone()),
    }
}

fn worker_loop(
    client: &HerdrClient,
    job_rx: &Mutex<Receiver<HerdrJob>>,
    event_tx: &Sender<HerdrEvent>,
) {
    loop {
        let job = {
            let lock = job_rx
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            match lock.recv() {
                Ok(job) => job,
                Err(_) => return,
            }
        };
        match job {
            HerdrJob::Snapshot { job_id, negotiate } => {
                let result = if negotiate {
                    client.negotiate().and_then(|()| client.snapshot())
                } else {
                    client.snapshot()
                }
                .map(HerdrJobResult::Snapshot);
                let _ = event_tx.send(HerdrEvent::Done { job_id, result });
                let _ = event_tx.send(HerdrEvent::Dropped { job_id });
            }
            HerdrJob::LifecycleSnapshot { job_id } => {
                let result = client.lifecycle_snapshot().map(HerdrJobResult::Lifecycle);
                let _ = event_tx.send(HerdrEvent::Done { job_id, result });
                let _ = event_tx.send(HerdrEvent::Dropped { job_id });
            }
            HerdrJob::AgentGet {
                job_id,
                request_id,
                target,
            } => {
                let result = client
                    .agent(&request_id, &target)
                    .map(HerdrJobResult::Agent);
                let _ = event_tx.send(HerdrEvent::Done { job_id, result });
                let _ = event_tx.send(HerdrEvent::Dropped { job_id });
            }
            HerdrJob::Open {
                job_id,
                negotiate,
                pane_id: _,
            } => {
                if negotiate && let Err(error) = client.negotiate() {
                    let _ = event_tx.send(HerdrEvent::Failed {
                        job_id,
                        phase: FailPhase::Connect,
                        error,
                    });
                    continue;
                }
                match client.connect() {
                    Ok(connection) => {
                        let (lease_tx, lease_rx) = mpsc::channel();
                        let _ = event_tx.send(HerdrEvent::Opened {
                            job_id,
                            lease: lease_tx,
                        });
                        run_lease(job_id, connection.into_stream(), &lease_rx, event_tx);
                    }
                    Err(error) => {
                        let _ = event_tx.send(HerdrEvent::Failed {
                            job_id,
                            phase: FailPhase::Connect,
                            error,
                        });
                    }
                }
            }
        }
    }
}

fn run_lease(
    job_id: u64,
    mut stream: std::os::unix::net::UnixStream,
    lease_rx: &Receiver<LeaseCmd>,
    event_tx: &Sender<HerdrEvent>,
) {
    loop {
        match lease_rx.recv_timeout(LEASE_TTL) {
            Ok(LeaseCmd::Drop) | Err(mpsc::RecvTimeoutError::Disconnected) => {
                let _ = event_tx.send(HerdrEvent::Dropped { job_id });
                return;
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                let _ = event_tx.send(HerdrEvent::Failed {
                    job_id,
                    phase: FailPhase::Lease,
                    error: HerdrError::Unexpected("herdr lease exceeded 30s".into()),
                });
                return;
            }
            Ok(LeaseCmd::Send {
                request_id,
                method,
                params,
                after_write_pause,
            }) => {
                let written_tx = event_tx.clone();
                let wrote = Cell::new(false);
                let raw = request_over_stream(
                    &mut stream,
                    &request_id,
                    &method,
                    &params,
                    false,
                    Some(&|| {
                        wrote.set(true);
                        let _ = written_tx.send(HerdrEvent::Written {
                            job_id,
                            after_write_pause,
                        });
                    }),
                );
                match raw {
                    Ok(value) if method == "agent.prompt" => {
                        let result = parse_agent_result(&value).map(HerdrJobResult::Prompt);
                        let _ = event_tx.send(HerdrEvent::Done { job_id, result });
                    }
                    Ok(value)
                        if method == "agent.start"
                            || method == "agent.get"
                            || method == "agent.rename" =>
                    {
                        let result = parse_agent_result(&value).map(HerdrJobResult::Agent);
                        let _ = event_tx.send(HerdrEvent::Done { job_id, result });
                    }
                    Ok(_) if method == "pane.close" => {
                        let _ = event_tx.send(HerdrEvent::Done {
                            job_id,
                            result: Ok(HerdrJobResult::Closed),
                        });
                    }
                    Ok(_) => {
                        let _ = event_tx.send(HerdrEvent::Done {
                            job_id,
                            result: Err(HerdrError::Unexpected(
                                "lease send returned an unexpected result".into(),
                            )),
                        });
                    }
                    Err(error) => {
                        let phase = if wrote.get() {
                            FailPhase::Read
                        } else {
                            FailPhase::Write
                        };
                        let _ = event_tx.send(HerdrEvent::Failed {
                            job_id,
                            phase,
                            error,
                        });
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::herdr::HerdrClient;
    use serde_json::Value;
    use std::io::{BufRead, BufReader, Write};
    use std::os::unix::net::UnixListener;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    fn drain_until(exec: &mut HerdrExec, want: usize, deadline: Instant) -> usize {
        let mut done = 0;
        while done < want && Instant::now() < deadline {
            match exec.try_recv() {
                Some(HerdrEvent::Done { .. }) => done += 1,
                Some(_) | None => thread::sleep(Duration::from_millis(5)),
            }
        }
        done
    }

    #[test]
    fn a_snapshot_submitted_after_done_does_not_share_the_previous_wire_request() {
        let directory = tempfile::tempdir().expect("tempdir");
        let socket = directory.path().join("herdr.sock");
        let listener = UnixListener::bind(&socket).expect("bind");
        let snapshots = Arc::new(AtomicUsize::new(0));
        let count = Arc::clone(&snapshots);
        thread::spawn(move || {
            for _ in 0..2 {
                let (mut stream, _) = listener.accept().expect("accept");
                let mut line = String::new();
                BufReader::new(stream.try_clone().expect("clone"))
                    .read_line(&mut line)
                    .expect("read");
                let request: Value = serde_json::from_str(&line).expect("json");
                assert_eq!(request["method"], "session.snapshot");
                count.fetch_add(1, Ordering::SeqCst);
                serde_json::to_writer(
                    &mut stream,
                    &serde_json::json!({
                        "id": request["id"],
                        "result": {
                            "type": "session_snapshot",
                            "snapshot": {"protocol": 20, "panes": [], "agents": []}
                        }
                    }),
                )
                .expect("write");
                stream.write_all(b"\n").expect("finish");
            }
        });

        let mut exec =
            HerdrExec::spawn_workers(HerdrClient::new(&socket, Duration::from_secs(2)), 1);
        exec.submit(HerdrJob::Snapshot {
            job_id: 1,
            negotiate: false,
        });
        assert_eq!(
            drain_until(&mut exec, 1, Instant::now() + Duration::from_secs(2)),
            1,
            "first snapshot must complete before the second is submitted"
        );
        exec.submit(HerdrJob::Snapshot {
            job_id: 2,
            negotiate: false,
        });
        assert_eq!(
            drain_until(&mut exec, 1, Instant::now() + Duration::from_secs(2)),
            1
        );
        assert_eq!(
            snapshots.load(Ordering::SeqCst),
            2,
            "a snapshot submitted after Done must be its own wire request"
        );
    }

    #[test]
    fn queued_snapshot_followers_all_receive_the_shared_result() {
        let directory = tempfile::tempdir().expect("tempdir");
        let socket = directory.path().join("herdr.sock");
        let listener = UnixListener::bind(&socket).expect("bind");
        let snapshots = Arc::new(AtomicUsize::new(0));
        let count = Arc::clone(&snapshots);
        thread::spawn(move || {
            for _ in 0..2 {
                let (mut stream, _) = listener.accept().expect("accept");
                let mut line = String::new();
                BufReader::new(stream.try_clone().expect("clone"))
                    .read_line(&mut line)
                    .expect("read");
                let request: Value = serde_json::from_str(&line).expect("json");
                count.fetch_add(1, Ordering::SeqCst);
                thread::sleep(Duration::from_millis(60));
                serde_json::to_writer(
                    &mut stream,
                    &serde_json::json!({
                        "id": request["id"],
                        "result": {
                            "type": "session_snapshot",
                            "snapshot": {"protocol": 20, "panes": [], "agents": []}
                        }
                    }),
                )
                .expect("write");
                stream.write_all(b"\n").expect("finish");
            }
        });
        let mut exec =
            HerdrExec::spawn_workers(HerdrClient::new(&socket, Duration::from_secs(2)), 1);
        exec.submit(HerdrJob::Snapshot {
            job_id: 1,
            negotiate: false,
        });
        thread::sleep(Duration::from_millis(15));
        for job_id in [2, 3] {
            exec.submit(HerdrJob::Snapshot {
                job_id,
                negotiate: false,
            });
        }
        assert_eq!(
            drain_until(&mut exec, 3, Instant::now() + Duration::from_secs(3)),
            3,
            "the queued leader and follower must both receive Done"
        );
        assert_eq!(snapshots.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn an_in_flight_snapshot_is_not_joined() {
        let directory = tempfile::tempdir().expect("tempdir");
        let socket = directory.path().join("herdr.sock");
        let listener = UnixListener::bind(&socket).expect("bind");
        let snapshots = Arc::new(AtomicUsize::new(0));
        let count = Arc::clone(&snapshots);
        thread::spawn(move || {
            for _ in 0..2 {
                let (mut stream, _) = listener.accept().expect("accept");
                let mut line = String::new();
                BufReader::new(stream.try_clone().expect("clone"))
                    .read_line(&mut line)
                    .expect("read");
                let request: Value = serde_json::from_str(&line).expect("json");
                assert_eq!(request["method"], "session.snapshot");
                count.fetch_add(1, Ordering::SeqCst);
                thread::sleep(Duration::from_millis(80));
                serde_json::to_writer(
                    &mut stream,
                    &serde_json::json!({
                        "id": request["id"],
                        "result": {
                            "type": "session_snapshot",
                            "snapshot": {"protocol": 20, "panes": [], "agents": []}
                        }
                    }),
                )
                .expect("write");
                stream.write_all(b"\n").expect("finish");
            }
        });

        let mut exec =
            HerdrExec::spawn_workers(HerdrClient::new(&socket, Duration::from_secs(2)), 1);
        exec.submit(HerdrJob::Snapshot {
            job_id: 1,
            negotiate: false,
        });
        thread::sleep(Duration::from_millis(20));
        exec.submit(HerdrJob::Snapshot {
            job_id: 2,
            negotiate: false,
        });
        assert_eq!(
            drain_until(&mut exec, 2, Instant::now() + Duration::from_secs(3)),
            2
        );
        assert_eq!(
            snapshots.load(Ordering::SeqCst),
            2,
            "the second snapshot must wait for its own wire request"
        );
    }
}
