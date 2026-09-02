//! SQLite-backed durable intent, messaging, and obligation state.

use std::collections::HashMap;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use rusqlite::{Connection, OptionalExtension, Transaction, params};
use thiserror::Error;

use crate::domain::{
    DeliveryOutcome, DeliveryTransport, IncarnationId, IncarnationState, InitialMessageKind,
    LogicalAgentId, MessageId, MessageKind, ObligationState, OperationId, OperationOutcome,
    OperatorNoticeId, Parent, RenewId, RenewIntent, RenewPhase, RenewStep, RenewTimeout,
    ReplyDisposition, StartIntent,
};
use crate::herdr::Snapshot;

const SCHEMA_VERSION: i64 = 22;

/// Host wall clock used for due comparison, in Unix epoch milliseconds.
///
/// A delivery is due when `now_ms >= scheduled_at_ms`. This is the same
/// `SystemTime` source as `created_at_ms` and other durable timestamps.
/// A due time that elapses while kelpied is down is reconciled as `unknown`;
/// restart must not fire that delivery without a new attempt record.
///
/// # Errors
///
/// Returns an error if the system clock precedes the Unix epoch or overflows.
pub fn store_clock_ms() -> Result<i64, StoreError> {
    now_millis()
}

/// Durable-store failures with invariant context.
#[derive(Debug, Error)]
pub enum StoreError {
    #[error("durable store failed: {0}")]
    Sql(#[from] rusqlite::Error),
    #[error("durable record is invalid: {0}")]
    InvalidRecord(String),
    #[error("coordination conflict: {0}")]
    Conflict(String),
    #[error("durable Kelpie state must be outside an operated repository: {0}")]
    UnsafeLocation(PathBuf),
}

/// IDs atomically created for one start intent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeclaredStart {
    pub logical_agent_id: LogicalAgentId,
    pub incarnation_id: IncarnationId,
    pub operation_id: OperationId,
}

/// Snapshot-derived evidence required to adopt a live Herdr agent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdoptEvidence {
    pub pane_id: String,
    pub terminal_id: String,
    pub public_name: String,
    pub backend_kind: String,
    pub working_directory: String,
    pub interactive_ready: bool,
    pub launch_pending: bool,
    pub native_agent_session: Option<serde_json::Value>,
}

/// One logical agent holding a public name.
#[derive(Debug, Clone)]
pub struct NameClaimant {
    pub logical_agent_id: String,
    pub created_at_ms: i64,
    pub has_ready_incarnation: bool,
    pub unresolved_count: i64,
}

/// One unresolved obligation touching a name's claimants, with both parties
/// resolved to names and liveness. The asker is the waiter; the responder is
/// the agent that still owes the final reply.
#[derive(Debug, Clone)]
pub struct NameObligation {
    pub ask_message_id: String,
    pub state: String,
    pub asker_agent_id: String,
    pub asker_name: String,
    pub asker_live: bool,
    pub responder_agent_id: String,
    pub responder_name: String,
    pub responder_live: bool,
    pub created_at_ms: i64,
    pub last_activity_at_ms: i64,
}

/// Everything known about who holds a public name. Read-only: this is the data
/// behind create-new refusals and `name-info`.
#[derive(Debug, Clone)]
pub struct NameInfo {
    pub public_name: String,
    pub claimants: Vec<NameClaimant>,
    pub unresolved: Vec<NameObligation>,
}

/// IDs atomically created for one ask and its obligation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CreatedAsk {
    pub message_id: MessageId,
    pub operation_id: OperationId,
}

/// IDs atomically created for a pane-less socket waiter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CreatedWaiter {
    pub logical_agent_id: LogicalAgentId,
}

/// One owing stop-notice recorded while ending a socket waiter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OwingRetireNotice {
    pub ask_message_id: MessageId,
    pub message_id: MessageId,
    /// Present when the owing agent had a Ready incarnation to deliver to.
    pub delivery: Option<(OperationId, IncarnationId)>,
}

/// Result of ending a socket waiter as a delivery target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EndedWaiter {
    pub cancelled_ask_ids: Vec<MessageId>,
    pub owing_notices: Vec<OwingRetireNotice>,
}

/// One queued socket-inbox delivery named by waiter agent, not incarnation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SocketInboxDelivery {
    pub message_id: MessageId,
    pub kind: MessageKind,
    pub body: String,
    pub reply_to: Option<MessageId>,
    pub disposition: Option<ReplyDisposition>,
    pub attempt_number: i64,
}

/// IDs atomically created for one tell and its delivery operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CreatedTell {
    pub message_id: MessageId,
    pub operation_id: OperationId,
}

/// IDs atomically created for one progress or final reply and its delivery.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CreatedReply {
    pub message_id: MessageId,
    /// Present for `herdr_prompt` waiters. Socket-inbox replies have no operation.
    pub operation_id: Option<OperationId>,
    /// Exact Ready incarnation of a `herdr_prompt` waiter selected at send time.
    pub recipient_incarnation: Option<IncarnationId>,
    pub disposition: ReplyDisposition,
}

/// Receive path bound when recording a reply.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplyReceivePath {
    /// Unique Ready incarnation, then Herdr prompt.
    HerdrPrompt(IncarnationId),
    /// Waiting agent's socket inbox, with no Herdr prompt.
    SocketInbox,
}

/// Which occupant a Kelpie-authored cancellation notice is for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancellationAudience {
    /// The asker, told their wait is over.
    Waiting,
    /// The owing agent, told to stop working on the ask.
    Owing,
}

/// IDs atomically created for a cancellation and its response messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CreatedCancellation {
    pub message_id: MessageId,
    /// Present only when the asker had a Ready incarnation to deliver to.
    pub delivery: Option<(OperationId, IncarnationId)>,
    pub owing_message_id: MessageId,
    /// Present only when the owing agent had a Ready incarnation to deliver to.
    pub owing_delivery: Option<(OperationId, IncarnationId)>,
}

/// IDs atomically created for a launch's initial message and delivery intent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CreatedInitialMessage {
    pub message_id: MessageId,
    pub operation_id: OperationId,
}

/// Exact ready runtime binding stored for an incarnation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadyBinding {
    pub pane_id: String,
    pub terminal_id: String,
}

/// Ready logical identity for CLI caller/recipient resolution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadyIdentity {
    pub logical_agent_id: LogicalAgentId,
    pub incarnation_id: IncarnationId,
    pub public_name: String,
}

/// Summary of one side-effect-free recovery reconciliation pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RecoveryReport {
    pub starts_recovered: usize,
    pub outcomes_marked_unknown: usize,
    pub untouched_pending_intents: usize,
    pub unattempted_clears_failed: usize,
    pub retirements_completed: usize,
    pub retirements_still_live: usize,
    pub incarnations_marked_lost: usize,
    /// Ready bindings whose recorded conversation reference was stale and
    /// replaced with the live one. A rotation, not a loss.
    pub native_sessions_refreshed: usize,
}

#[derive(Debug)]
struct RecoveryCandidate {
    operation_id: OperationId,
    incarnation_id: IncarnationId,
    kind: String,
    pane_id: String,
    terminal_id: String,
    intent_json: String,
    attempted: bool,
}

/// One durable operator-inbox entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperatorNotice {
    pub id: OperatorNoticeId,
    pub body: String,
    pub created_at_ms: i64,
    pub acknowledged: bool,
}

/// One unresolved final-reply obligation owed by a logical agent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PendingObligation {
    pub ask_message_id: MessageId,
    pub waiting_agent_id: LogicalAgentId,
    pub state: ObligationState,
}

/// One ask's durable content and parties — the amnesia-recovery read: a
/// renewed or restarted agent re-reads what it was asked through the id its
/// reminder carries. Read-only.
#[derive(Debug, Clone)]
pub struct AskInfo {
    pub ask_message_id: String,
    pub body: String,
    pub asker_agent_id: String,
    pub asker_name: String,
    pub responder_agent_id: String,
    pub responder_name: String,
    pub state: String,
    pub created_at_ms: i64,
    pub last_activity_at_ms: i64,
    pub cancellation_reason: Option<String>,
}

/// One of the agent's waits that was cancelled while it had no Ready
/// incarnation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CancelledWhileAway {
    pub ask_message_id: String,
    pub reason: String,
    pub cancelled_by: Option<String>,
    pub cancelled_at_ms: i64,
}

/// One queued tell or ask whose due time has been reached.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DueDelivery {
    pub operation_id: OperationId,
    pub message_id: MessageId,
    pub kind: MessageKind,
    pub sender: Option<LogicalAgentId>,
    pub recipient: LogicalAgentId,
    pub recipient_incarnation: IncarnationId,
    pub body: String,
    pub scheduled_at_ms: i64,
}

/// One overdue reminder whose owing agent has one exact Ready incarnation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DueReminder {
    pub ask_message_id: MessageId,
    pub owing_agent_id: LogicalAgentId,
    pub waiting_agent_id: LogicalAgentId,
    pub recipient_incarnation: IncarnationId,
    pub pane_id: String,
    pub terminal_id: String,
    pub interval_ms: i64,
    /// The ask's original question. The reminder is the amnesia protocol: a
    /// renewed or restarted agent may owe an answer it can no longer remember,
    /// so the reminder always carries what it was asked.
    pub body: String,
}

/// An unanswered ask eligible for stopped-boundary observation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundaryReminder {
    pub reminder: DueReminder,
    pub saw_working: bool,
}

/// One renew whose next phase transition is owed by the driver.
///
/// Carries both prompts because every phase after the first may need to be
/// completed from durable state alone after a crash.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DueRenew {
    pub renew_id: RenewId,
    pub logical_agent_id: LogicalAgentId,
    pub incarnation_id: IncarnationId,
    pub requester_agent_id: LogicalAgentId,
    pub prepare_prompt: String,
    pub resume_prompt: String,
    pub pane_id: String,
    pub terminal_id: String,
    pub phase: RenewPhase,
    pub on_timeout: RenewTimeout,
    pub prepare_timeout_ms: i64,
    pub every_ms: Option<i64>,
    pub cycle: i64,
    pub ask_message_id: Option<MessageId>,
    pub pre_clear_session_json: Option<String>,
    pub prepare_deadline_ms: Option<i64>,
    /// When a clear that has not yet been proven by rotation becomes worth
    /// reporting. Passing it never abandons the injection; it only ends the
    /// silence.
    pub clear_deadline_ms: Option<i64>,
    /// Whether the stall on this renew has already been reported, so an
    /// operator gets one notice rather than one per scheduler pass.
    pub clear_stall_notified: bool,
    /// For a backend that only rotates on its next prompt, the earliest time
    /// the resume prompt may be submitted. `None` means the injection waits on
    /// the rotation itself instead.
    pub inject_not_before_ms: Option<i64>,
    /// State of the prepare ask's obligation. `Resolved` is the ready signal;
    /// an agent must end its turn to issue a final reply, so the clear always
    /// acts on a settled incarnation.
    pub prepare_obligation_state: Option<ObligationState>,
    /// When the prepare ask resolved, which is when its final reply was
    /// delivered. An agent that renews itself is its own waiter, so that
    /// delivery went into the pane about to be cleared and the clear must not
    /// follow it back to back.
    pub prepare_settled_at_ms: Option<i64>,
}

/// One append-only observation with the time it was recorded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordedObservation {
    pub recorded_at_ms: i64,
    pub observed: crate::attribution::ObservedAttribution,
}

/// Attribution evidence for one exact incarnation.
///
/// Requested configuration and observed execution metadata are separate fields
/// and are never merged. An empty [`Self::observations`] means no adapter has
/// reported yet, which is distinct from an observation whose fields are
/// `Undetermined`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttributionEvidence {
    pub logical_agent_id: LogicalAgentId,
    pub incarnation_id: IncarnationId,
    pub public_name: String,
    pub backend_kind: String,
    pub incarnation_state: crate::domain::IncarnationState,
    pub requested: crate::attribution::RequestedAttribution,
    /// Backend arguments exactly as the launch requested them.
    ///
    /// Launch intent, never evidence: nothing here proves a backend honored it.
    pub requested_backend_args: Vec<String>,
    /// Append-only observations, oldest first.
    pub observations: Vec<RecordedObservation>,
}

impl AttributionEvidence {
    /// Most recent observation, or `None` when nothing has been observed.
    #[must_use]
    pub fn latest(&self) -> Option<&RecordedObservation> {
        self.observations.last()
    }
}

/// One incarnation as reported, without judgement about what its state means.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReportIncarnation {
    pub id: IncarnationId,
    pub state: crate::domain::IncarnationState,
    pub backend_kind: String,
    pub working_directory: String,
    pub herdr_session: String,
    pub intended_pane_id: String,
    pub expected_terminal_id: String,
    pub observed_pane_id: Option<String>,
    pub observed_terminal_id: Option<String>,
    /// Launch intent. Never evidence of what served a turn.
    pub requested: crate::attribution::RequestedAttribution,
    /// Launch intent. Never evidence that a backend honored it.
    pub requested_backend_args: Vec<String>,
    pub created_at_ms: i64,
    /// When this incarnation's current backend-native conversation was observed
    /// to start, or `None` when Kelpie has never seen it start.
    ///
    /// Distinct from `created_at_ms`, which records when the incarnation was
    /// bound to a runtime. The two agree only until the conversation first
    /// rotates; after a clear, resume, compaction, fork, or renew the
    /// incarnation is unchanged and the conversation is new. `None` is a real
    /// answer and MUST NOT be softened into `created_at_ms`.
    pub native_session_rotated_at_ms: Option<i64>,
    pub terminal_at_ms: Option<i64>,
    pub terminal_reason: Option<String>,
    /// Most recent operation targeting this incarnation, as `(id, kind, outcome)`.
    ///
    /// Reported so a stranded runtime can be joined back to the operation that
    /// produced it. A caller that lost its receipt, or that was restarted since,
    /// otherwise has no way to name the operation it needs to reason about.
    pub latest_operation: Option<(OperationId, String, OperationOutcome)>,
    /// The non-terminal renew bound to this incarnation, when one is armed.
    ///
    /// `None` means no cycle is scheduled or running, which for a root that is
    /// supposed to be supervised is the answer that matters. A policy ends when
    /// its incarnation stops being Ready and adoption does not restore it, so
    /// "armed" is not derivable from anything else the report carries.
    pub renew: Option<ReportRenew>,
}

/// Enough of a renew to name it in a notice after it has ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RenewIdentity {
    pub logical_agent_id: LogicalAgentId,
    pub incarnation_id: IncarnationId,
    pub cycle: i64,
    /// `None` for a one-shot renew; set means a standing policy.
    pub every_ms: Option<i64>,
}

/// The armed renew of one incarnation, as `report` presents it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReportRenew {
    pub id: RenewId,
    pub phase: RenewPhase,
    pub cycle: i64,
    /// `None` for a one-shot renew; set means a standing policy re-arms.
    pub every_ms: Option<i64>,
    /// When *this* cycle was due. Written once at insert and never updated, so
    /// it is the next fire only while the phase is `scheduled`; for a cycle
    /// already in flight it is that cycle's original due time, in the past.
    pub scheduled_at_ms: i64,
}

/// One logical agent with its incarnations, newest first.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReportAgent {
    pub id: LogicalAgentId,
    pub public_name: String,
    pub parent_agent_id: Option<LogicalAgentId>,
    pub explicitly_parentless: bool,
    pub created_at_ms: i64,
    pub incarnations: Vec<ReportIncarnation>,
}

/// One reply obligation as an edge between two agents.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReportObligation {
    pub ask_message_id: MessageId,
    pub owing_agent_id: LogicalAgentId,
    pub waiting_agent_id: LogicalAgentId,
    pub state: ObligationState,
    pub created_at_ms: i64,
    pub last_activity_at_ms: i64,
    pub resolving_message_id: Option<MessageId>,
}

/// Every durable node and edge Kelpie owns, at one moment.
///
/// Facts only. Whether a state is a problem is the caller's policy, so nothing
/// here is labelled healthy, stuck, or missing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FleetReport {
    pub generated_at_ms: i64,
    pub agents: Vec<ReportAgent>,
    pub obligations: Vec<ReportObligation>,
}

/// `SQLite` connection owning Kelpie's durable coordination state.
#[derive(Debug)]
pub struct Store {
    connection: Connection,
}

impl Store {
    /// Open, configure, and migrate a durable store.
    ///
    /// # Errors
    ///
    /// Returns an error when `SQLite` cannot open, configure, or migrate safely.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        let path = path.as_ref();
        ensure_outside_repository(path)?;
        let connection = Connection::open(path)?;
        configure(&connection)?;
        migrate(&connection)?;
        Ok(Self { connection })
    }

    /// Create an isolated in-memory store for deterministic tests.
    ///
    /// # Errors
    ///
    /// Returns an error when `SQLite` setup or migration fails.
    pub fn in_memory() -> Result<Self, StoreError> {
        let connection = Connection::open_in_memory()?;
        configure(&connection)?;
        migrate(&connection)?;
        Ok(Self { connection })
    }

    /// Atomically persist logical identity, incarnation, and start operation.
    ///
    /// Create-new intent (`logical_agent_id` absent) allocates a new logical
    /// agent. Continue intent reuses the exact logical agent, attaches the
    /// current public-name alias, and preserves its obligations and history.
    ///
    /// # Errors
    ///
    /// Returns a conflict for a reused idempotency key, missing continue target,
    /// or invalid parent.
    pub fn declare_start(&mut self, intent: &StartIntent) -> Result<DeclaredStart, StoreError> {
        let incarnation_id = IncarnationId::new();
        let operation_id = OperationId::new();
        let now = now_millis()?;
        let tx = self.connection.transaction()?;
        refuse_name_held_by_socket_waiter(&tx, &intent.public_name)?;
        let logical_agent_id = if let Some(existing) = intent.logical_agent_id {
            let found: Option<String> = tx
                .query_row(
                    "SELECT id FROM logical_agents WHERE id = ?1",
                    [existing.to_string()],
                    |row| row.get(0),
                )
                .optional()?;
            if found.is_none() {
                return Err(StoreError::Conflict(
                    "logical agent to continue does not exist".into(),
                ));
            }
            refuse_pane_bind_of_socket_inbox(&tx, existing)?;
            // Public names are live aliases attached to the current occupant.
            tx.execute(
                "UPDATE logical_agents SET public_name = ?1 WHERE id = ?2",
                params![intent.public_name, existing.to_string()],
            )?;
            existing
        } else {
            let logical_agent_id = LogicalAgentId::new();
            insert_logical_agent(
                &tx,
                logical_agent_id,
                &intent.public_name,
                intent.parent,
                DeliveryTransport::HerdrPrompt,
                now,
            )?;
            logical_agent_id
        };
        tx.execute(
            "INSERT INTO incarnations (
                id, logical_agent_id, herdr_session, intended_pane_id,
                expected_terminal_id, backend_kind, backend_args_json,
                working_directory, created_at_ms, state,
                requested_model, requested_provider, requested_effort
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 'declared', ?10, ?11, ?12)",
            params![
                incarnation_id.to_string(),
                logical_agent_id.to_string(),
                intent.herdr_session,
                intent.pane_id,
                intent.expected_terminal_id,
                intent.backend_kind,
                serde_json::to_string(&intent.backend_args)
                    .map_err(|error| invalid_json(&error))?,
                intent.working_directory,
                now,
                empty_to_none(intent.requested_model.as_deref()),
                empty_to_none(intent.requested_provider.as_deref()),
                empty_to_none(intent.requested_effort.as_deref()),
            ],
        )?;
        tx.execute(
            "INSERT INTO operations (
                id, idempotency_key, kind, target_incarnation_id, intent_json,
                created_at_ms, outcome
             ) VALUES (?1, ?2, 'start', ?3, ?4, ?5, 'pending')",
            params![
                operation_id.to_string(),
                intent.idempotency_key,
                incarnation_id.to_string(),
                serde_json::to_string(intent).map_err(|error| invalid_json(&error))?,
                now,
            ],
        )
        .map_err(map_constraint)?;
        tx.commit()?;
        Ok(DeclaredStart {
            logical_agent_id,
            incarnation_id,
            operation_id,
        })
    }

    /// Atomically adopt a live named Herdr agent without issuing `agent.start`.
    ///
    /// The caller supplies already-verified snapshot evidence including the
    /// live Herdr public name. This records a Ready incarnation bound to that
    /// exact name. Create-new vs continue matches start.
    ///
    /// # Errors
    ///
    /// Returns a conflict for a reused idempotency key, missing continue
    /// target, another Ready incarnation already bound to the same exact live
    /// identity, launch-pending evidence, or a missing live public name.
    pub fn declare_adopt(
        &mut self,
        intent: &crate::domain::AdoptIntent,
        evidence: &AdoptEvidence,
    ) -> Result<DeclaredStart, StoreError> {
        if evidence.public_name.is_empty() {
            return Err(StoreError::Conflict(
                "adopt requires a live Herdr public name".into(),
            ));
        }
        self.insert_adopt_binding(intent, evidence, "ready", "succeeded", true)
    }

    /// Persist adopt intent for an unnamed occupant before `agent.rename`.
    ///
    /// The incarnation stays `declared` until a later snapshot proves the
    /// claimed name is live. This method does not mutate Herdr.
    ///
    /// # Errors
    ///
    /// Returns the same conflicts as [`Self::declare_adopt`], plus missing
    /// intended public name.
    pub fn declare_adopt_pending(
        &mut self,
        intent: &crate::domain::AdoptIntent,
        evidence: &AdoptEvidence,
    ) -> Result<DeclaredStart, StoreError> {
        if evidence.public_name.is_empty() {
            return Err(StoreError::Conflict(
                "pending adopt requires the intended Herdr public name".into(),
            ));
        }
        self.insert_adopt_binding(intent, evidence, "declared", "pending", false)
    }

    #[allow(clippy::too_many_lines)]
    fn insert_adopt_binding(
        &mut self,
        intent: &crate::domain::AdoptIntent,
        evidence: &AdoptEvidence,
        incarnation_state: &str,
        operation_outcome: &str,
        resolved: bool,
    ) -> Result<DeclaredStart, StoreError> {
        if evidence.pane_id != intent.pane_id || evidence.terminal_id != intent.expected_terminal_id
        {
            return Err(StoreError::Conflict(
                "adopt evidence does not match the exact pane and terminal selector".into(),
            ));
        }
        if evidence.launch_pending {
            return Err(StoreError::Conflict(
                "adopt requires a live agent that is not launch-pending".into(),
            ));
        }
        if let Some(expected_name) = intent.public_name.as_deref()
            && evidence.public_name != expected_name
        {
            return Err(StoreError::Conflict(
                "live agent name does not match the requested public name".into(),
            ));
        }
        if let Some(expected_kind) = intent.backend_kind.as_deref()
            && evidence.backend_kind != expected_kind
        {
            return Err(StoreError::Conflict(
                "live backend kind does not match the requested backend kind".into(),
            ));
        }
        if evidence.backend_kind.is_empty() || evidence.public_name.is_empty() {
            return Err(StoreError::Conflict(
                "adopt requires an observed backend kind and public name".into(),
            ));
        }
        let public_name = evidence.public_name.clone();
        let native_session_json = evidence
            .native_agent_session
            .as_ref()
            .map(ToString::to_string);
        let incarnation_id = IncarnationId::new();
        let operation_id = OperationId::new();
        let now = now_millis()?;
        let tx = self.connection.transaction()?;
        let conflict: Option<String> = tx
            .query_row(
                "SELECT id FROM incarnations
                 WHERE state = 'ready'
                   AND observed_pane_id = ?1
                   AND observed_terminal_id = ?2",
                params![evidence.pane_id, evidence.terminal_id],
                |row| row.get(0),
            )
            .optional()?;
        if let Some(existing) = conflict {
            return Err(StoreError::Conflict(format!(
                "exact live binding is already adopted by ready incarnation {existing}"
            )));
        }
        let alias_conflict: Option<String> = tx
            .query_row(
                "SELECT i.id FROM incarnations i
                 JOIN logical_agents l ON l.id = i.logical_agent_id
                 WHERE i.state = 'ready' AND l.public_name = ?1",
                [&public_name],
                |row| row.get(0),
            )
            .optional()?;
        if let Some(existing) = alias_conflict {
            return Err(StoreError::Conflict(format!(
                "ready alias {public_name} is already bound to incarnation {existing}"
            )));
        }
        refuse_name_held_by_socket_waiter(&tx, &public_name)?;
        // A create-new adopt under a name a prior logical agent still holds
        // unresolved obligations under would strand them: the new identity
        // cannot resolve an obligation it does not own, and a reply addressed
        // to the prior agent has no Ready incarnation to reach. Continue is
        // the remedy for that agent's own debts, not a way to take a name
        // somebody else is still waiting on.
        let info = Self::name_info_on(&tx, &public_name)?;
        let continued = intent.logical_agent_id.map(|id| id.to_string());
        let foreign_unresolved = info.unresolved.iter().any(|obligation| {
            continued.as_deref() != Some(obligation.asker_agent_id.as_str())
                && continued.as_deref() != Some(obligation.responder_agent_id.as_str())
        });
        if foreign_unresolved {
            return Err(StoreError::Conflict(Self::name_conflict_message(&info)));
        }
        let logical_agent_id = if let Some(existing) = intent.logical_agent_id {
            let found: Option<String> = tx
                .query_row(
                    "SELECT id FROM logical_agents WHERE id = ?1",
                    [existing.to_string()],
                    |row| row.get(0),
                )
                .optional()?;
            if found.is_none() {
                return Err(StoreError::Conflict(
                    "logical agent to continue does not exist".into(),
                ));
            }
            refuse_pane_bind_of_socket_inbox(&tx, existing)?;
            tx.execute(
                "UPDATE logical_agents SET public_name = ?1 WHERE id = ?2",
                params![public_name, existing.to_string()],
            )?;
            existing
        } else {
            let logical_agent_id = LogicalAgentId::new();
            insert_logical_agent(
                &tx,
                logical_agent_id,
                &public_name,
                intent.parent,
                DeliveryTransport::HerdrPrompt,
                now,
            )?;
            logical_agent_id
        };
        let intent_json = serde_json::json!({
            "adopt": intent,
            "evidence": {
                "pane_id": evidence.pane_id,
                "terminal_id": evidence.terminal_id,
                "public_name": public_name,
                "observed_public_name": evidence.public_name,
                "name_authority": "observed",
                "backend_kind": evidence.backend_kind,
                "working_directory": evidence.working_directory,
                "herdr_session": intent.herdr_session,
                "native_agent_session": evidence.native_agent_session,
            }
        });
        tx.execute(
            "INSERT INTO incarnations (
                id, logical_agent_id, herdr_session, intended_pane_id,
                expected_terminal_id, observed_pane_id, observed_terminal_id,
                backend_kind, backend_args_json, working_directory, created_at_ms, state,
                name_authority, observed_native_session_json,
                requested_model, requested_provider, requested_effort
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17)",
            params![
                incarnation_id.to_string(),
                logical_agent_id.to_string(),
                intent.herdr_session,
                evidence.pane_id,
                evidence.terminal_id,
                evidence.pane_id,
                evidence.terminal_id,
                evidence.backend_kind,
                serde_json::to_string(&intent.backend_args)
                    .map_err(|error| StoreError::InvalidRecord(error.to_string()))?,
                evidence.working_directory,
                now,
                incarnation_state,
                "observed",
                native_session_json,
                empty_to_none(intent.requested_model.as_deref()),
                empty_to_none(intent.requested_provider.as_deref()),
                empty_to_none(intent.requested_effort.as_deref()),
            ],
        )?;
        let resolved_at = resolved.then_some(now);
        tx.execute(
            "INSERT INTO operations (
                id, idempotency_key, kind, target_incarnation_id, intent_json,
                created_at_ms, resolved_at_ms, outcome
             ) VALUES (?1, ?2, 'adopt', ?3, ?4, ?5, ?6, ?7)",
            params![
                operation_id.to_string(),
                intent.idempotency_key,
                incarnation_id.to_string(),
                intent_json.to_string(),
                now,
                resolved_at,
                operation_outcome,
            ],
        )
        .map_err(map_constraint)?;
        tx.commit()?;
        Ok(DeclaredStart {
            logical_agent_id,
            incarnation_id,
            operation_id,
        })
    }

    /// List public aliases currently bound to Ready incarnations.
    ///
    /// # Errors
    ///
    /// Returns an error when the store cannot be read.
    pub fn ready_aliases(&self) -> Result<Vec<String>, StoreError> {
        let mut statement = self.connection.prepare(
            "SELECT l.public_name
             FROM logical_agents l
             JOIN incarnations i ON i.logical_agent_id = l.id
             WHERE i.state = 'ready'",
        )?;
        let rows = statement.query_map([], |row| row.get(0))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(StoreError::from)
    }

    /// Resolve a pending name-claim adopt from an authoritative snapshot.
    ///
    /// Requires exact pane, terminal, public name, and backend. Does not
    /// require managed `interactive_ready`.
    ///
    /// # Errors
    ///
    /// Returns a conflict unless the snapshot proves the intended name is live.
    pub fn accept_adopt_ready(
        &mut self,
        operation_id: OperationId,
        incarnation_id: IncarnationId,
        agent: &crate::herdr::AgentObservation,
    ) -> Result<(), StoreError> {
        let now = now_millis()?;
        let tx = self.connection.transaction()?;
        let expected: Option<(String, String, String, String)> = tx
            .query_row(
                "SELECT i.intended_pane_id, i.expected_terminal_id, l.public_name, i.backend_kind
                 FROM incarnations i JOIN logical_agents l ON l.id = i.logical_agent_id
                 WHERE i.id = ?1 AND i.state IN ('declared', 'starting')",
                [incarnation_id.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .optional()?;
        let exact = expected
            .as_ref()
            .is_some_and(|(pane, terminal, name, kind)| {
                agent.pane_id == *pane
                    && agent.terminal_id == *terminal
                    && agent.name.as_deref() == Some(name.as_str())
                    && agent.agent.as_deref() == Some(kind.as_str())
                    && !agent.launch_pending
            });
        if !exact {
            return Err(StoreError::Conflict(
                "Herdr observation does not prove the claimed adopt name is live".into(),
            ));
        }
        if let Some((_, _, name, _)) = expected.as_ref() {
            refuse_name_held_by_socket_waiter(&tx, name)?;
        }
        let changed = tx.execute(
            "UPDATE operations SET outcome = 'succeeded', resolved_at_ms = ?1
             WHERE id = ?2 AND target_incarnation_id = ?3 AND outcome IN ('pending', 'accepted')",
            params![now, operation_id.to_string(), incarnation_id.to_string()],
        )?;
        if changed != 1 {
            return Err(StoreError::Conflict(
                "stale adopt readiness observation cannot mutate this incarnation".into(),
            ));
        }
        let native_session_json = agent.agent_session.as_ref().map(ToString::to_string);
        tx.execute(
            "UPDATE incarnations SET state = 'ready', observed_pane_id = ?1,
             observed_terminal_id = ?2, name_authority = 'observed',
             observed_native_session_json = COALESCE(?3, observed_native_session_json)
             WHERE id = ?4",
            params![
                agent.pane_id,
                agent.terminal_id,
                native_session_json,
                incarnation_id.to_string()
            ],
        )?;
        tx.execute(
            "UPDATE operation_attempts SET phase = 'response_committed', resolved_at_ms = ?1
             WHERE operation_id = ?2 AND attempt_number = (
                SELECT MAX(attempt_number) FROM operation_attempts WHERE operation_id = ?2
             )",
            params![now, operation_id.to_string()],
        )?;
        tx.commit()?;
        Ok(())
    }

    /// Look up a prior adopt/start by idempotency key when present.
    ///
    /// # Errors
    ///
    /// Returns an error when the stored operation record is malformed.
    pub fn declared_by_idempotency_key(
        &self,
        idempotency_key: &str,
    ) -> Result<Option<DeclaredStart>, StoreError> {
        let row: Option<(String, String, String)> = self
            .connection
            .query_row(
                "SELECT o.id, o.target_incarnation_id, i.logical_agent_id
                 FROM operations o
                 JOIN incarnations i ON i.id = o.target_incarnation_id
                 WHERE o.idempotency_key = ?1",
                [idempotency_key],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()?;
        row.map(|(operation, incarnation, logical)| {
            Ok(DeclaredStart {
                operation_id: OperationId::parse(&operation).ok_or_else(|| {
                    StoreError::InvalidRecord(format!("invalid operation id {operation}"))
                })?,
                incarnation_id: IncarnationId::parse(&incarnation).ok_or_else(|| {
                    StoreError::InvalidRecord(format!("invalid incarnation id {incarnation}"))
                })?,
                logical_agent_id: LogicalAgentId::parse(&logical).ok_or_else(|| {
                    StoreError::InvalidRecord(format!("invalid logical agent id {logical}"))
                })?,
            })
        })
        .transpose()
    }

    /// Journal an external attempt before any Herdr request write.
    ///
    /// # Errors
    ///
    /// Returns a conflict unless the exact operation and incarnation still own the attempt.
    pub fn begin_attempt(
        &mut self,
        operation_id: OperationId,
        incarnation_id: IncarnationId,
        request_id: &str,
    ) -> Result<i64, StoreError> {
        let now = now_millis()?;
        let tx = self.connection.transaction()?;
        let owner: Option<String> = tx
            .query_row(
                "SELECT target_incarnation_id FROM operations WHERE id = ?1 AND outcome = 'pending'",
                [operation_id.to_string()],
                |row| row.get(0),
            )
            .optional()?;
        if owner.as_deref() != Some(&incarnation_id.to_string()) {
            return Err(StoreError::Conflict(
                "operation no longer belongs to the exact pending incarnation".into(),
            ));
        }
        let attempt_number: i64 = tx.query_row(
            "SELECT COALESCE(MAX(attempt_number), 0) + 1 FROM operation_attempts WHERE operation_id = ?1",
            [operation_id.to_string()],
            |row| row.get(0),
        )?;
        tx.execute(
            "INSERT INTO operation_attempts
             (operation_id, attempt_number, request_id, started_at_ms, phase)
             VALUES (?1, ?2, ?3, ?4, 'prepared')",
            params![operation_id.to_string(), attempt_number, request_id, now],
        )?;
        tx.execute(
            "UPDATE incarnations SET state = 'starting' WHERE id = ?1 AND state = 'declared'",
            [incarnation_id.to_string()],
        )?;
        tx.commit()?;
        Ok(attempt_number)
    }

    /// Record Herdr's acceptance of a start without claiming runtime readiness.
    ///
    /// # Errors
    ///
    /// Returns a conflict for stale results or mismatched terminal and pane identities.
    pub fn accept_start_submission(
        &mut self,
        operation_id: OperationId,
        incarnation_id: IncarnationId,
        pane_id: &str,
        terminal_id: &str,
    ) -> Result<(), StoreError> {
        let tx = self.connection.transaction()?;
        let expected: Option<(String, String)> = tx
            .query_row(
                "SELECT intended_pane_id, expected_terminal_id FROM incarnations
                 WHERE id = ?1 AND state = 'starting'",
                [incarnation_id.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        if expected
            .as_ref()
            .map(|(pane, terminal)| (pane.as_str(), terminal.as_str()))
            != Some((pane_id, terminal_id))
        {
            return Err(StoreError::Conflict(
                "Herdr result does not name the exact intended incarnation binding".into(),
            ));
        }
        let changed = tx.execute(
            "UPDATE operations SET outcome = 'accepted'
             WHERE id = ?1 AND target_incarnation_id = ?2 AND outcome = 'pending'",
            params![operation_id.to_string(), incarnation_id.to_string()],
        )?;
        if changed != 1 {
            return Err(StoreError::Conflict(
                "stale start acceptance cannot mutate this incarnation".into(),
            ));
        }
        tx.execute(
            "UPDATE incarnations SET observed_pane_id = ?1,
             observed_terminal_id = ?2 WHERE id = ?3",
            params![pane_id, terminal_id, incarnation_id.to_string()],
        )?;
        tx.execute(
            "UPDATE operation_attempts SET phase = 'accepted'
             WHERE operation_id = ?1 AND attempt_number = (
                SELECT MAX(attempt_number) FROM operation_attempts WHERE operation_id = ?1
             )",
            [operation_id.to_string()],
        )?;
        tx.commit()?;
        Ok(())
    }

    /// Resolve a start only from a later authoritative exact-ready observation.
    ///
    /// # Errors
    ///
    /// Returns a conflict unless pane, terminal, public name, backend kind, and
    /// readiness flags prove the exact intended incarnation is ready.
    pub fn accept_start_ready(
        &mut self,
        operation_id: OperationId,
        incarnation_id: IncarnationId,
        agent: &crate::herdr::AgentObservation,
        supersedes: Option<IncarnationId>,
    ) -> Result<(), StoreError> {
        let now = now_millis()?;
        let tx = self.connection.transaction()?;
        let expected: Option<(String, String, String, String)> = tx
            .query_row(
                "SELECT i.intended_pane_id, i.expected_terminal_id, l.public_name, i.backend_kind
                 FROM incarnations i JOIN logical_agents l ON l.id = i.logical_agent_id
                 WHERE i.id = ?1 AND i.state = 'starting'",
                [incarnation_id.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .optional()?;
        let exact_ready = expected
            .as_ref()
            .is_some_and(|(pane, terminal, name, kind)| {
                agent.pane_id == *pane
                    && agent.terminal_id == *terminal
                    && agent.name.as_deref() == Some(name.as_str())
                    && agent.agent.as_deref() == Some(kind.as_str())
                    && agent.interactive_ready
                    && !agent.launch_pending
            });
        if !exact_ready {
            return Err(StoreError::Conflict(
                "Herdr observation does not prove the exact intended incarnation is ready".into(),
            ));
        }
        if let Some((_, _, name, _)) = expected.as_ref() {
            refuse_name_held_by_socket_waiter(&tx, name)?;
        }
        let changed = tx.execute(
            "UPDATE operations SET outcome = 'succeeded', resolved_at_ms = ?1
             WHERE id = ?2 AND target_incarnation_id = ?3 AND outcome IN ('pending', 'accepted')",
            params![now, operation_id.to_string(), incarnation_id.to_string()],
        )?;
        if changed != 1 {
            return Err(StoreError::Conflict(
                "stale readiness observation cannot mutate this incarnation".into(),
            ));
        }
        let native_session_json = agent.agent_session.as_ref().map(ToString::to_string);
        tx.execute(
            "UPDATE incarnations SET state = 'ready', observed_pane_id = ?1,
             observed_terminal_id = ?2, observed_native_session_json = ?3 WHERE id = ?4",
            params![
                agent.pane_id,
                agent.terminal_id,
                native_session_json,
                incarnation_id.to_string()
            ],
        )?;
        tx.execute(
            "UPDATE operation_attempts SET phase = 'response_committed', resolved_at_ms = ?1
             WHERE operation_id = ?2 AND attempt_number = (
                SELECT MAX(attempt_number) FROM operation_attempts WHERE operation_id = ?2
             )",
            params![now, operation_id.to_string()],
        )?;
        if let Some(predecessor) = supersedes {
            // Same statement, same transaction, same instant: the successor
            // becomes Ready exactly as the predecessor stops being Ready. The
            // predecessor must still be Ready and must belong to the same
            // logical agent, or this is not a handoff and nothing is demoted.
            let demoted = tx.execute(
                "UPDATE incarnations SET state = 'superseded', terminal_at_ms = ?1,
                 terminal_reason = 'superseded by a handoff to a new incarnation'
                 WHERE id = ?2 AND state = 'ready' AND logical_agent_id = (
                    SELECT logical_agent_id FROM incarnations WHERE id = ?3
                 )",
                params![now, predecessor.to_string(), incarnation_id.to_string()],
            )?;
            if demoted != 1 {
                return Err(StoreError::Conflict(
                    "handoff predecessor is not a ready incarnation of the same logical agent"
                        .into(),
                ));
            }
        }
        tx.commit()?;
        Ok(())
    }

    /// Preserve an ambiguous external result without retrying it.
    ///
    /// # Errors
    ///
    /// Returns an error when the exact pending operation cannot be updated.
    pub fn mark_unknown(
        &mut self,
        operation_id: OperationId,
        incarnation_id: IncarnationId,
        evidence: &str,
    ) -> Result<(), StoreError> {
        self.mark_unknown_with_clear_spacing(operation_id, incarnation_id, evidence, None)
    }

    /// Preserve an ambiguous clear and durably postpone queued follow-up prompts.
    ///
    /// # Errors
    ///
    /// Returns an error when the exact pending clear cannot be updated.
    pub fn mark_clear_unknown(
        &mut self,
        operation_id: OperationId,
        incarnation_id: IncarnationId,
        evidence: &str,
        prompt_settle_delay_ms: i64,
    ) -> Result<(), StoreError> {
        let is_clear: bool = self.connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM operations WHERE id = ?1 AND kind = 'clear')",
            [operation_id.to_string()],
            |row| row.get(0),
        )?;
        if !is_clear {
            return Err(StoreError::Conflict(
                "clear unknown result does not name a clear operation".into(),
            ));
        }
        self.mark_unknown_with_clear_spacing(
            operation_id,
            incarnation_id,
            evidence,
            Some(prompt_settle_delay_ms),
        )
    }

    fn mark_unknown_with_clear_spacing(
        &mut self,
        operation_id: OperationId,
        incarnation_id: IncarnationId,
        evidence: &str,
        prompt_settle_delay_ms: Option<i64>,
    ) -> Result<(), StoreError> {
        let now = now_millis()?;
        let tx = self.connection.transaction()?;
        let changed = tx.execute(
            "UPDATE operations SET outcome = 'unknown', resolved_at_ms = ?1
             WHERE id = ?2 AND target_incarnation_id = ?3 AND outcome IN ('pending', 'accepted')",
            params![now, operation_id.to_string(), incarnation_id.to_string()],
        )?;
        if changed != 1 {
            return Err(StoreError::Conflict(
                "unknown result does not own the current operation".into(),
            ));
        }
        tx.execute(
            "UPDATE operation_attempts SET phase = 'unknown', evidence_json = ?1,
             resolved_at_ms = ?2 WHERE operation_id = ?3 AND attempt_number = (
                SELECT MAX(attempt_number) FROM operation_attempts WHERE operation_id = ?3
             )",
            params![
                serde_json::json!({"detail": evidence}).to_string(),
                now,
                operation_id.to_string()
            ],
        )?;
        tx.execute(
            "UPDATE incarnations SET state = 'unknown' WHERE id = ?1 AND state = 'starting'",
            [incarnation_id.to_string()],
        )?;
        tx.execute(
            "UPDATE deliveries SET outcome = 'unknown', resolved_at_ms = ?1
             WHERE operation_id = ?2 AND outcome IN ('pending', 'submitted', 'accepted', 'queued')",
            params![now, operation_id.to_string()],
        )?;
        if let Some(delay_ms) = prompt_settle_delay_ms {
            tx.execute(
                "UPDATE deliveries
                 SET scheduled_at_ms = MAX(scheduled_at_ms, COALESCE((
                     SELECT MAX(a.started_at_ms) + ?1 FROM operation_attempts a
                     WHERE a.operation_id = ?2 AND a.phase != 'prepared'
                 ), scheduled_at_ms))
                 WHERE recipient_incarnation_id = ?3 AND outcome = 'queued'",
                params![
                    delay_ms,
                    operation_id.to_string(),
                    incarnation_id.to_string()
                ],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    /// Journal one retryable rejection without ending the operation.
    ///
    /// The attempt is terminal evidence and keeps its own record; the operation
    /// stays `pending` so an explicitly retryable rejection can be attempted
    /// again inside the caller's budget. A rejection Herdr received and refused
    /// is proven non-delivery, so a later attempt cannot duplicate an effect.
    ///
    /// # Errors
    ///
    /// Returns a conflict unless the exact operation is still pending.
    pub fn reject_attempt(
        &mut self,
        operation_id: OperationId,
        incarnation_id: IncarnationId,
        evidence: &str,
    ) -> Result<(), StoreError> {
        let now = now_millis()?;
        let tx = self.connection.transaction()?;
        let owner: Option<String> = tx
            .query_row(
                "SELECT target_incarnation_id FROM operations WHERE id = ?1 AND outcome = 'pending'",
                [operation_id.to_string()],
                |row| row.get(0),
            )
            .optional()?;
        if owner.as_deref() != Some(&incarnation_id.to_string()) {
            return Err(StoreError::Conflict(
                "retryable rejection does not own the current pending operation".into(),
            ));
        }
        let changed = tx.execute(
            "UPDATE operation_attempts SET phase = 'rejected', evidence_json = ?1,
             resolved_at_ms = ?2 WHERE operation_id = ?3 AND attempt_number = (
                SELECT MAX(attempt_number) FROM operation_attempts WHERE operation_id = ?3
             )",
            params![
                serde_json::json!({"detail": evidence, "retryable": true}).to_string(),
                now,
                operation_id.to_string()
            ],
        )?;
        if changed != 1 {
            return Err(StoreError::Conflict(
                "retryable rejection has no attempt to resolve".into(),
            ));
        }
        tx.commit()?;
        Ok(())
    }

    /// Persist a decisive structured Herdr rejection.
    ///
    /// # Errors
    ///
    /// Returns a conflict unless the exact operation is still non-terminal.
    pub fn mark_rejected(
        &mut self,
        operation_id: OperationId,
        incarnation_id: IncarnationId,
        evidence: &str,
        delivery_outcome: DeliveryOutcome,
    ) -> Result<(), StoreError> {
        let now = now_millis()?;
        let tx = self.connection.transaction()?;
        let changed = tx.execute(
            "UPDATE operations SET outcome = 'failed', resolved_at_ms = ?1
             WHERE id = ?2 AND target_incarnation_id = ?3 AND outcome IN ('pending', 'accepted')",
            params![now, operation_id.to_string(), incarnation_id.to_string()],
        )?;
        if changed != 1 {
            return Err(StoreError::Conflict(
                "rejection does not own the current operation".into(),
            ));
        }
        tx.execute(
            "UPDATE operation_attempts SET phase = 'rejected', evidence_json = ?1,
             resolved_at_ms = ?2 WHERE operation_id = ?3 AND attempt_number = (
                SELECT MAX(attempt_number) FROM operation_attempts WHERE operation_id = ?3
             )",
            params![
                serde_json::json!({"detail": evidence}).to_string(),
                now,
                operation_id.to_string()
            ],
        )?;
        tx.execute(
            "UPDATE incarnations SET state = 'failed' WHERE id = ?1 AND state = 'starting'",
            [incarnation_id.to_string()],
        )?;
        tx.execute(
            "UPDATE deliveries SET outcome = ?1, resolved_at_ms = ?2
             WHERE operation_id = ?3 AND outcome IN ('pending', 'submitted', 'queued')",
            params![
                delivery_outcome_name(delivery_outcome),
                now,
                operation_id.to_string()
            ],
        )?;
        tx.commit()?;
        Ok(())
    }

    /// Persist that a prepared request is entering the external write boundary.
    ///
    /// This transition is committed immediately before writing to Herdr. After
    /// a crash, `submitted` is intentionally ambiguous and is never auto-retried.
    ///
    /// # Errors
    ///
    /// Returns a conflict unless the exact latest attempt is still prepared.
    pub fn mark_submitted(
        &mut self,
        operation_id: OperationId,
        attempt_number: i64,
        request_id: &str,
    ) -> Result<(), StoreError> {
        let now = now_millis()?;
        let tx = self.connection.transaction()?;
        let changed = tx.execute(
            "UPDATE operation_attempts SET phase = 'submitted'
             WHERE operation_id = ?1 AND attempt_number = ?2 AND request_id = ?3 AND phase = 'prepared'",
            params![operation_id.to_string(), attempt_number, request_id],
        )?;
        if changed != 1 {
            return Err(StoreError::Conflict(
                "attempt is not the exact prepared request".into(),
            ));
        }
        tx.execute(
            "UPDATE deliveries SET outcome = 'submitted', attempted_at_ms = ?1,
             herdr_request_id = ?2 WHERE operation_id = ?3 AND outcome = 'pending'",
            params![now, request_id, operation_id.to_string()],
        )?;
        tx.commit()?;
        Ok(())
    }

    /// Move a queued delivery to `submitted` immediately before the Herdr write.
    ///
    /// The row stays `queued` until this commit. Cancel wins if it still sees
    /// `queued` with no submitted attempt. Submit requires `now_ms >= scheduled_at_ms`
    /// so a future due cannot be accepted early.
    ///
    /// # Errors
    ///
    /// Returns a conflict unless the attempt is prepared and the delivery is
    /// still queued and due.
    pub fn submit_queued_delivery(
        &mut self,
        operation_id: OperationId,
        attempt_number: i64,
        request_id: &str,
        now_ms: i64,
    ) -> Result<(), StoreError> {
        let tx = self.connection.transaction()?;
        let changed = tx.execute(
            "UPDATE operation_attempts SET phase = 'submitted'
             WHERE operation_id = ?1 AND attempt_number = ?2 AND request_id = ?3 AND phase = 'prepared'",
            params![operation_id.to_string(), attempt_number, request_id],
        )?;
        if changed != 1 {
            return Err(StoreError::Conflict(
                "attempt is not the exact prepared request".into(),
            ));
        }
        let delivery_changed = tx.execute(
            "UPDATE deliveries SET outcome = 'submitted', attempted_at_ms = ?1,
             herdr_request_id = ?2 WHERE operation_id = ?3 AND outcome = 'queued'
             AND scheduled_at_ms <= ?1",
            params![now_ms, request_id, operation_id.to_string()],
        )?;
        if delivery_changed != 1 {
            return Err(StoreError::Conflict(
                "queued delivery is no longer due or was cancelled before the Herdr write".into(),
            ));
        }
        tx.commit()?;
        Ok(())
    }

    /// Commit Herdr acceptance for one exact message delivery.
    ///
    /// # Errors
    ///
    /// Returns a conflict when the Herdr response identifies a replacement runtime.
    pub fn accept_delivery(
        &mut self,
        operation_id: OperationId,
        incarnation_id: IncarnationId,
        pane_id: &str,
        terminal_id: &str,
    ) -> Result<(), StoreError> {
        let now = now_millis()?;
        let tx = self.connection.transaction()?;
        let binding: Option<(String, String)> = tx
            .query_row(
                "SELECT observed_pane_id, observed_terminal_id FROM incarnations
                 WHERE id = ?1 AND state = 'ready'",
                [incarnation_id.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        if binding
            .as_ref()
            .map(|(pane, terminal)| (pane.as_str(), terminal.as_str()))
            != Some((pane_id, terminal_id))
        {
            return Err(StoreError::Conflict(
                "delivery response belongs to a replacement runtime".into(),
            ));
        }
        let changed = tx.execute(
            "UPDATE operations SET outcome = 'succeeded', resolved_at_ms = ?1
             WHERE id = ?2 AND target_incarnation_id = ?3 AND outcome = 'pending'",
            params![now, operation_id.to_string(), incarnation_id.to_string()],
        )?;
        if changed != 1 {
            return Err(StoreError::Conflict(
                "delivery operation is no longer pending".into(),
            ));
        }
        tx.execute(
            "UPDATE deliveries SET outcome = 'accepted', resolved_at_ms = ?1
             WHERE operation_id = ?2 AND recipient_incarnation_id = ?3 AND outcome = 'submitted'",
            params![now, operation_id.to_string(), incarnation_id.to_string()],
        )?;
        tx.execute(
            "UPDATE operation_attempts SET phase = 'response_committed', resolved_at_ms = ?1
             WHERE operation_id = ?2 AND attempt_number = (
                SELECT MAX(attempt_number) FROM operation_attempts WHERE operation_id = ?2
             )",
            params![now, operation_id.to_string()],
        )?;
        tx.execute(
            "UPDATE obligation_reminders SET next_due_at_ms = ?1 + interval_ms,
             boundary_check_at_ms = ?1
             WHERE ask_message_id = (
                 SELECT m.id FROM messages m JOIN deliveries d ON d.message_id = m.id
                 WHERE d.operation_id = ?2 AND m.kind = 'ask'
             ) AND disabled_at_ms IS NULL AND suspended_at_ms IS NULL",
            params![now, operation_id.to_string()],
        )?;
        // Final replies resolve only after accepted delivery so rejected/unknown
        // prompts never claim the waiter received the answer.
        let final_reply: Option<(String, String)> = tx
            .query_row(
                "SELECT m.reply_to_message_id, m.id
                 FROM messages m
                 JOIN deliveries d ON d.message_id = m.id
                 WHERE d.operation_id = ?1 AND m.kind = 'reply' AND m.disposition = 'final'",
                [operation_id.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        if let Some((ask_message_id, resolving_message_id)) = final_reply {
            let resolved = tx.execute(
                "UPDATE obligations SET state = 'resolved', last_activity_at_ms = ?1,
                 resolving_message_id = ?2
                 WHERE ask_message_id = ?3 AND state IN ('open', 'in_progress')",
                params![now, resolving_message_id, ask_message_id],
            )?;
            if resolved != 1 {
                return Err(StoreError::Conflict(
                    "final delivery acceptance cannot resolve a terminal or absent obligation"
                        .into(),
                ));
            }
        }
        tx.commit()?;
        Ok(())
    }

    /// Persist a launch's structured initial message and delivery operation.
    ///
    /// An operator-attributed ask is rejected because the initial schema cannot
    /// represent an obligation owed back to the operator. Operator tells and
    /// agent-attributed tells or asks are supported.
    ///
    /// # Errors
    ///
    /// Returns a conflict for an invalid sender, recipient, or operator ask.
    pub fn create_initial_message(
        &mut self,
        recipient: LogicalAgentId,
        recipient_incarnation: IncarnationId,
        intent: &crate::domain::InitialMessageIntent,
        idempotency_key: &str,
    ) -> Result<CreatedInitialMessage, StoreError> {
        if intent.kind == InitialMessageKind::Ask && intent.sender.is_none() {
            return Err(StoreError::Conflict(
                "an operator-attributed initial ask needs an agent waiting identity".into(),
            ));
        }
        let message_id = MessageId::new();
        let operation_id = OperationId::new();
        let now = now_millis()?;
        let tx = self.connection.transaction()?;
        let owner: Option<String> = tx
            .query_row(
                "SELECT logical_agent_id FROM incarnations WHERE id = ?1 AND state = 'ready'",
                [recipient_incarnation.to_string()],
                |row| row.get(0),
            )
            .optional()?;
        if owner.as_deref() != Some(&recipient.to_string()) {
            return Err(StoreError::Conflict(
                "initial-message recipient is not the exact ready incarnation".into(),
            ));
        }
        if let Some(sender) = intent.sender {
            let exists: bool = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM logical_agents WHERE id = ?1)",
                [sender.to_string()],
                |row| row.get(0),
            )?;
            if !exists {
                return Err(StoreError::Conflict(
                    "initial-message sender logical agent is absent".into(),
                ));
            }
        }
        let kind = match intent.kind {
            InitialMessageKind::Tell => "tell",
            InitialMessageKind::Ask => "ask",
        };
        let creates_obligation = i64::from(intent.kind == InitialMessageKind::Ask);
        tx.execute(
            "INSERT INTO messages
             (id, sender_agent_id, recipient_agent_id, kind, body, created_at_ms, creates_obligation)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                message_id.to_string(),
                intent.sender.map(|id| id.to_string()),
                recipient.to_string(),
                kind,
                intent.body,
                now,
                creates_obligation
            ],
        )?;
        if let Some(sender) = intent
            .sender
            .filter(|_| intent.kind == InitialMessageKind::Ask)
        {
            insert_obligation(&tx, message_id, recipient, sender, now)?;
        }
        tx.execute(
            "INSERT INTO operations
             (id, idempotency_key, kind, target_incarnation_id, intent_json,
              created_at_ms, outcome)
             VALUES (?1, ?2, 'prompt', ?3, ?4, ?5, 'pending')",
            params![
                operation_id.to_string(),
                idempotency_key,
                recipient_incarnation.to_string(),
                serde_json::json!({"message_id": message_id}).to_string(),
                now
            ],
        )
        .map_err(map_constraint)?;
        tx.execute(
            "INSERT INTO deliveries
             (message_id, recipient_incarnation_id, attempt_number, scheduled_at_ms,
              outcome, operation_id)
             VALUES (?1, ?2, 1, ?3, 'pending', ?4)",
            params![
                message_id.to_string(),
                recipient_incarnation.to_string(),
                now,
                operation_id.to_string()
            ],
        )?;
        tx.commit()?;
        Ok(CreatedInitialMessage {
            message_id,
            operation_id,
        })
    }

    /// Register a pane-less `LogicalAgent` as a socket-inbox waiter.
    ///
    /// Creates no incarnation and no pane occupant. The public name is held until
    /// [`Store::end_socket_waiter`] releases it.
    ///
    /// # Errors
    ///
    /// Returns a conflict for an illegal name, a name already held by a Ready
    /// alias or another socket waiter, a missing parent, or a reused
    /// idempotency key bound to a different waiter.
    pub fn register_socket_waiter(
        &mut self,
        public_name: &str,
        parent: Parent,
        idempotency_key: &str,
    ) -> Result<CreatedWaiter, StoreError> {
        if !crate::name::valid_herdr_name(public_name) {
            return Err(StoreError::InvalidRecord(format!(
                "{public_name} is not a legal Herdr agent name"
            )));
        }
        let now = now_millis()?;
        let tx = self.connection.transaction()?;
        let replay: Option<String> = tx
            .query_row(
                "SELECT logical_agent_id FROM socket_waiter_keys WHERE idempotency_key = ?1",
                [idempotency_key],
                |row| row.get(0),
            )
            .optional()?;
        if let Some(existing) = replay {
            let logical_agent_id = LogicalAgentId::parse(&existing).ok_or_else(|| {
                StoreError::InvalidRecord(format!("invalid waiter id {existing}"))
            })?;
            let (stored_name, stored_parent, stored_parentless, ended): (
                String,
                Option<String>,
                i64,
                Option<i64>,
            ) = tx.query_row(
                "SELECT public_name, parent_agent_id, explicitly_parentless, targeting_ended_at_ms
                 FROM logical_agents WHERE id = ?1",
                [logical_agent_id.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )?;
            let parent_matches = match parent {
                Parent::Parentless => stored_parentless == 1 && stored_parent.is_none(),
                Parent::Agent(id) => stored_parent.as_deref() == Some(&id.to_string()),
            };
            if ended.is_some() {
                return Err(StoreError::Conflict(
                    "waiter.register idempotency key is bound to an ended waiter".into(),
                ));
            }
            if stored_name != public_name || !parent_matches {
                return Err(StoreError::Conflict(
                    "waiter.register idempotency key already bound to a different waiter".into(),
                ));
            }
            tx.commit()?;
            return Ok(CreatedWaiter { logical_agent_id });
        }
        refuse_name_held_by_socket_waiter(&tx, public_name)?;
        refuse_live_or_pending_alias(&tx, public_name)?;
        if let Parent::Agent(parent_id) = parent {
            let exists: bool = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM logical_agents WHERE id = ?1)",
                [parent_id.to_string()],
                |row| row.get(0),
            )?;
            if !exists {
                return Err(StoreError::Conflict(
                    "parent logical agent does not exist".into(),
                ));
            }
        }
        let logical_agent_id = LogicalAgentId::new();
        insert_logical_agent(
            &tx,
            logical_agent_id,
            public_name,
            parent,
            DeliveryTransport::SocketInbox,
            now,
        )?;
        tx.execute(
            "INSERT INTO socket_waiter_keys (idempotency_key, logical_agent_id) VALUES (?1, ?2)",
            params![idempotency_key, logical_agent_id.to_string()],
        )
        .map_err(map_constraint)?;
        tx.commit()?;
        Ok(CreatedWaiter { logical_agent_id })
    }

    /// End a socket waiter as a delivery target and release its public name.
    ///
    /// Open and in-progress asks waiting on this waiter are cancelled in the
    /// same transaction, with reason `waiter retired`. Queued socket-inbox
    /// deliveries for the waiter become `target_unavailable`.
    ///
    /// # Errors
    ///
    /// Returns a conflict when the agent is not an active socket waiter.
    pub fn end_socket_waiter(
        &mut self,
        logical_agent_id: LogicalAgentId,
    ) -> Result<EndedWaiter, StoreError> {
        self.end_socket_waiter_with_owing_due(logical_agent_id, &HashMap::new())
    }

    /// Open and in-progress asks this waiter is waiting on, oldest first.
    ///
    /// # Errors
    ///
    /// Returns a conflict when the logical agent is absent.
    pub fn unresolved_asks_waiting_on(
        &self,
        waiting_agent_id: LogicalAgentId,
    ) -> Result<Vec<MessageId>, StoreError> {
        let exists: bool = self.connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM logical_agents WHERE id = ?1)",
            [waiting_agent_id.to_string()],
            |row| row.get(0),
        )?;
        if !exists {
            return Err(StoreError::Conflict("logical agent is absent".into()));
        }
        let mut statement = self.connection.prepare(
            "SELECT ask_message_id FROM obligations
             WHERE waiting_agent_id = ?1 AND state IN ('open', 'in_progress')
             ORDER BY creation_sequence",
        )?;
        let rows = statement.query_map([waiting_agent_id.to_string()], |row| {
            row.get::<_, String>(0)
        })?;
        let mut asks = Vec::new();
        for row in rows {
            asks.push(parse_message_id(&row?)?);
        }
        Ok(asks)
    }

    /// End a socket waiter, cancelling its waits with optional owing schedules.
    ///
    /// # Errors
    ///
    /// Returns a conflict when the agent is not an active socket waiter.
    #[allow(clippy::too_many_lines)]
    pub fn end_socket_waiter_with_owing_due(
        &mut self,
        logical_agent_id: LogicalAgentId,
        owing_due_at_ms: &HashMap<MessageId, Option<i64>>,
    ) -> Result<EndedWaiter, StoreError> {
        const REASON: &str = "waiter retired";
        let now = now_millis()?;
        let tx = self.connection.transaction()?;
        require_active_socket_waiter(&tx, logical_agent_id)?;
        let public_name: String = tx.query_row(
            "SELECT public_name FROM logical_agents WHERE id = ?1",
            [logical_agent_id.to_string()],
            |row| row.get(0),
        )?;
        let changed = tx.execute(
            "UPDATE logical_agents
             SET targeting_ended_at_ms = ?1
             WHERE id = ?2
               AND delivery_transport = 'socket_inbox'
               AND targeting_ended_at_ms IS NULL",
            params![now, logical_agent_id.to_string()],
        )?;
        if changed != 1 {
            return Err(StoreError::Conflict(format!(
                "logical agent {logical_agent_id} is not an active socket waiter"
            )));
        }
        let asks = {
            let mut statement = tx.prepare(
                "SELECT ask_message_id, owing_agent_id FROM obligations
                 WHERE waiting_agent_id = ?1 AND state IN ('open', 'in_progress')
                 ORDER BY creation_sequence",
            )?;
            let rows = statement.query_map([logical_agent_id.to_string()], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?;
            let mut asks = Vec::new();
            for row in rows {
                asks.push(row?);
            }
            asks
        };
        let mut cancelled_ask_ids = Vec::new();
        let mut owing_notices = Vec::new();
        for (ask, owing) in asks {
            let ask_id = parse_message_id(&ask)?;
            let owing_agent = parse_logical_agent_id(&owing)?;
            let waiting_body = format!(
                "Your ask {ask_id} was cancelled by {public_name}. Reason: {REASON}. \
                 No reply is owed. Re-ask the current holder of the name if the question \
                 still matters."
            );
            let owing_body = format!(
                "Stop working on ask {ask_id}. It was cancelled by {public_name}. \
                 Reason: {REASON}. No reply is owed."
            );
            let due_at_ms = owing_due_at_ms.get(&ask_id).copied().flatten();
            let (waiting_message_id, _) = record_cancellation_side(
                &tx,
                logical_agent_id,
                ask_id,
                REASON,
                &waiting_body,
                CancellationAudience::Waiting,
                None,
                now,
                "ambiguous ready incarnation for waiting agent",
            )?;
            let (owing_message_id, owing_delivery) = record_cancellation_side(
                &tx,
                owing_agent,
                ask_id,
                REASON,
                &owing_body,
                CancellationAudience::Owing,
                due_at_ms,
                now,
                "ambiguous ready incarnation for owing agent",
            )?;
            tx.execute(
                "UPDATE obligations SET cancellation_response_message_id = ?1,
                 cancellation_owing_message_id = ?2
                 WHERE ask_message_id = ?3",
                params![
                    waiting_message_id.to_string(),
                    owing_message_id.to_string(),
                    ask_id.to_string()
                ],
            )?;
            let settled = tx.execute(
                "UPDATE obligations SET state = 'cancelled', last_activity_at_ms = ?1,
                 cancellation_requester_agent_id = ?2, cancellation_reason = ?3
                 WHERE ask_message_id = ?4 AND waiting_agent_id = ?2
                 AND state IN ('open', 'in_progress')",
                params![
                    now,
                    logical_agent_id.to_string(),
                    REASON,
                    ask_id.to_string()
                ],
            )?;
            if settled != 1 {
                return Err(StoreError::Conflict(
                    "obligation changed before waiter retirement committed".into(),
                ));
            }
            cancelled_ask_ids.push(ask_id);
            owing_notices.push(OwingRetireNotice {
                ask_message_id: ask_id,
                message_id: owing_message_id,
                delivery: owing_delivery,
            });
        }
        tx.execute(
            "UPDATE deliveries
                SET outcome = 'target_unavailable', resolved_at_ms = ?1
              WHERE recipient_agent_id = ?2
                AND delivery_transport = 'socket_inbox'
                AND outcome = 'queued'",
            params![now, logical_agent_id.to_string()],
        )?;
        tx.commit()?;
        Ok(EndedWaiter {
            cancelled_ask_ids,
            owing_notices,
        })
    }

    /// Record one socket-inbox delivery named by waiter agent, not incarnation.
    ///
    /// Persist is not acceptance. Callers pass the observed outcome.
    ///
    /// # Errors
    ///
    /// Returns a conflict when the waiter is absent, ended, or not socket-inbox,
    /// or when the message is missing.
    pub fn record_socket_inbox_delivery(
        &mut self,
        message_id: MessageId,
        recipient_agent_id: LogicalAgentId,
        outcome: DeliveryOutcome,
    ) -> Result<(), StoreError> {
        let now = now_millis()?;
        let tx = self.connection.transaction()?;
        let transport: Option<(String, Option<i64>)> = tx
            .query_row(
                "SELECT delivery_transport, targeting_ended_at_ms
                 FROM logical_agents WHERE id = ?1",
                [recipient_agent_id.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        match transport {
            Some((transport, ended)) if transport == "socket_inbox" && ended.is_none() => {}
            Some(_) => {
                return Err(StoreError::Conflict(format!(
                    "logical agent {recipient_agent_id} is not an active socket waiter"
                )));
            }
            None => {
                return Err(StoreError::Conflict(format!(
                    "socket waiter {recipient_agent_id} is absent"
                )));
            }
        }
        let exists: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM messages WHERE id = ?1)",
            [message_id.to_string()],
            |row| row.get(0),
        )?;
        if !exists {
            return Err(StoreError::Conflict("delivery message is absent".into()));
        }
        tx.execute(
            "INSERT INTO deliveries
             (message_id, delivery_transport, recipient_incarnation_id, recipient_agent_id,
              attempt_number, scheduled_at_ms, outcome)
             VALUES (?1, 'socket_inbox', NULL, ?2, 1, ?3, ?4)",
            params![
                message_id.to_string(),
                recipient_agent_id.to_string(),
                now,
                match outcome {
                    DeliveryOutcome::Pending => "pending",
                    DeliveryOutcome::Submitted => "submitted",
                    DeliveryOutcome::Accepted => "accepted",
                    DeliveryOutcome::Queued => "queued",
                    DeliveryOutcome::Unknown => "unknown",
                    DeliveryOutcome::Rejected => "rejected",
                    DeliveryOutcome::TargetUnavailable => "target_unavailable",
                    DeliveryOutcome::Superseded => "superseded",
                }
            ],
        )?;
        tx.commit()?;
        Ok(())
    }

    /// Confirm `logical_agent_id` is an active socket waiter.
    ///
    /// Same-user attribution, not authentication. A pane agent, ended waiter, or
    /// absent id is not owned as an inbox.
    ///
    /// # Errors
    ///
    /// Returns a conflict when the agent is absent, ended, or not socket-inbox.
    pub fn claim_socket_waiter(&self, logical_agent_id: LogicalAgentId) -> Result<(), StoreError> {
        require_active_socket_waiter(&self.connection, logical_agent_id)
    }

    /// Queued socket-inbox deliveries for one waiter, oldest first.
    ///
    /// Draining is the same attempt completing. It is not a resend.
    ///
    /// # Errors
    ///
    /// Returns a conflict when the agent is not an active socket waiter.
    pub fn queued_socket_inbox_deliveries(
        &self,
        recipient_agent_id: LogicalAgentId,
    ) -> Result<Vec<SocketInboxDelivery>, StoreError> {
        require_active_socket_waiter(&self.connection, recipient_agent_id)?;
        let mut statement = self.connection.prepare(
            "SELECT m.id, m.kind, m.body, m.reply_to_message_id, m.disposition, d.attempt_number
               FROM deliveries d
               JOIN messages m ON m.id = d.message_id
              WHERE d.recipient_agent_id = ?1
                AND d.delivery_transport = 'socket_inbox'
                AND d.outcome = 'queued'
              ORDER BY d.scheduled_at_ms, m.created_at_ms",
        )?;
        let rows = statement.query_map([recipient_agent_id.to_string()], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, Option<String>>(4)?,
                row.get::<_, i64>(5)?,
            ))
        })?;
        let mut deliveries = Vec::new();
        for row in rows {
            let (message_id, kind, body, reply_to, disposition, attempt_number) = row?;
            deliveries.push(SocketInboxDelivery {
                message_id: parse_message_id(&message_id)?,
                kind: parse_message_kind(&kind)?,
                body,
                reply_to: reply_to.as_deref().map(parse_message_id).transpose()?,
                disposition: disposition
                    .as_deref()
                    .map(parse_reply_disposition)
                    .transpose()?,
                attempt_number,
            });
        }
        Ok(deliveries)
    }

    /// Acknowledge one queued socket-inbox delivery for this waiter.
    ///
    /// Persist is not acceptance. ACK is. Already-accepted ACK is idempotent.
    /// Dropping the host leaves the delivery queued.
    ///
    /// # Errors
    ///
    /// Returns a conflict when the waiter is not active or the delivery is not a
    /// queued or already-accepted socket-inbox row for that waiter.
    pub fn ack_socket_inbox_delivery(
        &mut self,
        recipient_agent_id: LogicalAgentId,
        message_id: MessageId,
    ) -> Result<DeliveryOutcome, StoreError> {
        let now = now_millis()?;
        let tx = self.connection.transaction()?;
        require_active_socket_waiter(&tx, recipient_agent_id)?;
        let outcome: Option<String> = tx
            .query_row(
                "SELECT outcome FROM deliveries
                  WHERE message_id = ?1
                    AND recipient_agent_id = ?2
                    AND delivery_transport = 'socket_inbox'",
                params![message_id.to_string(), recipient_agent_id.to_string()],
                |row| row.get(0),
            )
            .optional()?;
        match outcome.as_deref() {
            Some("accepted") => {
                tx.commit()?;
                Ok(DeliveryOutcome::Accepted)
            }
            Some("queued") => {
                tx.execute(
                    "UPDATE deliveries
                        SET outcome = 'accepted', resolved_at_ms = ?1
                      WHERE message_id = ?2
                        AND recipient_agent_id = ?3
                        AND delivery_transport = 'socket_inbox'
                        AND outcome = 'queued'",
                    params![now, message_id.to_string(), recipient_agent_id.to_string()],
                )?;
                resolve_socket_inbox_final_reply(&tx, message_id, now)?;
                tx.commit()?;
                Ok(DeliveryOutcome::Accepted)
            }
            Some(other) => Err(StoreError::Conflict(format!(
                "socket-inbox delivery {message_id} for {recipient_agent_id} is {other}"
            ))),
            None => Err(StoreError::Conflict(format!(
                "no socket-inbox delivery {message_id} for waiter {recipient_agent_id}"
            ))),
        }
    }

    /// Insert a message so tests can queue a socket-inbox delivery without reply bind.
    #[cfg(test)]
    pub(crate) fn insert_inbox_message(
        &mut self,
        recipient: LogicalAgentId,
        kind: MessageKind,
        body: &str,
        reply_to: Option<MessageId>,
        disposition: Option<ReplyDisposition>,
    ) -> Result<MessageId, StoreError> {
        let now = now_millis()?;
        let message_id = MessageId::new();
        let kind_name = match kind {
            MessageKind::Tell => "tell",
            MessageKind::Ask => "ask",
            MessageKind::Reply => "reply",
            MessageKind::Cancellation => "cancellation",
        };
        let sender = match kind {
            MessageKind::Cancellation => None,
            _ => Some(recipient.to_string()),
        };
        self.connection.execute(
            "INSERT INTO messages
             (id, sender_agent_id, recipient_agent_id, kind, body, created_at_ms,
              reply_to_message_id, disposition, creates_obligation)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                message_id.to_string(),
                sender,
                recipient.to_string(),
                kind_name,
                body,
                now,
                reply_to.map(|id| id.to_string()),
                disposition.map(disposition_name),
                i64::from(kind == MessageKind::Ask),
            ],
        )?;
        Ok(message_id)
    }

    /// Read the delivery transport recorded at logical-agent creation.
    ///
    /// # Errors
    ///
    /// Returns an error when the agent is missing or the stored value is unknown.
    pub fn delivery_transport(
        &self,
        logical_agent_id: LogicalAgentId,
    ) -> Result<DeliveryTransport, StoreError> {
        let value: String = self.connection.query_row(
            "SELECT delivery_transport FROM logical_agents WHERE id = ?1",
            [logical_agent_id.to_string()],
            |row| row.get(0),
        )?;
        parse_delivery_transport(&value)
    }

    /// Atomically persist an ask, delivery, operation, and reply obligation.
    ///
    /// # Errors
    ///
    /// Returns an error if either logical identity or exact incarnation is absent.
    pub fn create_ask(
        &mut self,
        sender: LogicalAgentId,
        recipient: LogicalAgentId,
        recipient_incarnation: IncarnationId,
        body: &str,
        idempotency_key: &str,
    ) -> Result<CreatedAsk, StoreError> {
        self.create_ask_with_due(
            sender,
            recipient,
            recipient_incarnation,
            body,
            idempotency_key,
            None,
        )
    }

    /// Persist an ask that should fire once at `due_at_ms`.
    ///
    /// # Errors
    ///
    /// Returns an error if identities are absent or `due_at_ms` is negative.
    pub fn create_ask_with_due(
        &mut self,
        sender: LogicalAgentId,
        recipient: LogicalAgentId,
        recipient_incarnation: IncarnationId,
        body: &str,
        idempotency_key: &str,
        due_at_ms: Option<i64>,
    ) -> Result<CreatedAsk, StoreError> {
        self.create_ask_with_schedule(
            sender,
            recipient,
            recipient_incarnation,
            body,
            idempotency_key,
            due_at_ms,
            None,
            false,
        )
    }

    /// Persist an ask with optional delivery and reply-reminder schedules.
    ///
    /// The reminder remains unarmed until the original ask delivery is accepted.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid identities, times, or reminder intervals.
    #[allow(clippy::too_many_arguments)]
    pub fn create_ask_with_schedule(
        &mut self,
        sender: LogicalAgentId,
        recipient: LogicalAgentId,
        recipient_incarnation: IncarnationId,
        body: &str,
        idempotency_key: &str,
        due_at_ms: Option<i64>,
        remind_after_ms: Option<i64>,
        operator_attributed: bool,
    ) -> Result<CreatedAsk, StoreError> {
        if remind_after_ms.is_some_and(|value| value <= 0) {
            return Err(StoreError::Conflict(
                "reminder interval must be greater than zero".into(),
            ));
        }
        let message_id = MessageId::new();
        let operation_id = OperationId::new();
        let now = now_millis()?;
        let schedule = delivery_schedule(now, due_at_ms)?;
        let tx = self.connection.transaction()?;
        let incarnation_owner: Option<String> = tx
            .query_row(
                "SELECT logical_agent_id FROM incarnations WHERE id = ?1",
                [recipient_incarnation.to_string()],
                |row| row.get(0),
            )
            .optional()?;
        if incarnation_owner.as_deref() != Some(&recipient.to_string()) {
            return Err(StoreError::Conflict(
                "recipient incarnation does not belong to the recipient logical agent".into(),
            ));
        }
        let waiting = waiter_identity(&tx, sender)?;
        if operator_attributed && waiting.transport != DeliveryTransport::SocketInbox {
            return Err(StoreError::Conflict(
                "operator attribution does not make operator the waiter".into(),
            ));
        }
        if waiting.ended {
            return Err(StoreError::Conflict(format!(
                "socket waiter {sender} is no longer a delivery target"
            )));
        }
        let message_sender = if operator_attributed {
            None
        } else {
            Some(sender.to_string())
        };
        tx.execute(
            "INSERT INTO messages
             (id, sender_agent_id, recipient_agent_id, kind, body, created_at_ms, creates_obligation)
             VALUES (?1, ?2, ?3, 'ask', ?4, ?5, 1)",
            params![message_id.to_string(), message_sender, recipient.to_string(), body, now],
        )?;
        insert_obligation(&tx, message_id, recipient, sender, now)?;
        if let Some(interval_ms) = remind_after_ms {
            tx.execute(
                "INSERT INTO obligation_reminders (ask_message_id, interval_ms)
                 VALUES (?1, ?2)",
                params![message_id.to_string(), interval_ms],
            )?;
        }
        let mut intent = serde_json::json!({"message_id": message_id, "recipient_incarnation_id": recipient_incarnation});
        if let Some(due_at_ms) = due_at_ms {
            intent["due_at_ms"] = serde_json::json!(due_at_ms);
        }
        tx.execute(
            "INSERT INTO operations
             (id, idempotency_key, kind, target_incarnation_id, intent_json, created_at_ms, outcome)
             VALUES (?1, ?2, 'prompt', ?3, ?4, ?5, 'pending')",
            params![
                operation_id.to_string(),
                idempotency_key,
                recipient_incarnation.to_string(),
                intent.to_string(),
                now
            ],
        )
        .map_err(map_constraint)?;
        tx.execute(
            "INSERT INTO deliveries
             (message_id, recipient_incarnation_id, attempt_number, scheduled_at_ms, outcome, operation_id)
             VALUES (?1, ?2, 1, ?3, ?4, ?5)",
            params![
                message_id.to_string(),
                recipient_incarnation.to_string(),
                schedule.scheduled_at_ms,
                schedule.outcome,
                operation_id.to_string()
            ],
        )?;
        tx.commit()?;
        Ok(CreatedAsk {
            message_id,
            operation_id,
        })
    }

    /// Atomically persist a tell, delivery, and operation without an obligation.
    ///
    /// # Errors
    ///
    /// Returns an error if either logical identity or the exact incarnation is absent.
    pub fn create_tell(
        &mut self,
        sender: LogicalAgentId,
        recipient: LogicalAgentId,
        recipient_incarnation: IncarnationId,
        body: &str,
        idempotency_key: &str,
    ) -> Result<CreatedTell, StoreError> {
        self.create_tell_with_due(
            sender,
            recipient,
            recipient_incarnation,
            body,
            idempotency_key,
            None,
        )
    }

    /// Persist a tell that should fire once at `due_at_ms`.
    ///
    /// # Errors
    ///
    /// Returns an error if the incarnation is not Ready or `due_at_ms` is negative.
    pub fn create_tell_with_due(
        &mut self,
        sender: LogicalAgentId,
        recipient: LogicalAgentId,
        recipient_incarnation: IncarnationId,
        body: &str,
        idempotency_key: &str,
        due_at_ms: Option<i64>,
    ) -> Result<CreatedTell, StoreError> {
        let message_id = MessageId::new();
        let operation_id = OperationId::new();
        let now = now_millis()?;
        let schedule = delivery_schedule(now, due_at_ms)?;
        let tx = self.connection.transaction()?;
        let incarnation_owner: Option<String> = tx
            .query_row(
                "SELECT logical_agent_id FROM incarnations WHERE id = ?1 AND state = 'ready'",
                [recipient_incarnation.to_string()],
                |row| row.get(0),
            )
            .optional()?;
        if incarnation_owner.as_deref() != Some(&recipient.to_string()) {
            return Err(StoreError::Conflict(
                "recipient incarnation is not the recipient's exact ready incarnation".into(),
            ));
        }
        let sender_exists: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM logical_agents WHERE id = ?1)",
            [sender.to_string()],
            |row| row.get(0),
        )?;
        if !sender_exists {
            return Err(StoreError::Conflict(
                "sender logical agent is absent".into(),
            ));
        }
        tx.execute(
            "INSERT INTO messages
             (id, sender_agent_id, recipient_agent_id, kind, body, created_at_ms, creates_obligation)
             VALUES (?1, ?2, ?3, 'tell', ?4, ?5, 0)",
            params![
                message_id.to_string(),
                sender.to_string(),
                recipient.to_string(),
                body,
                now
            ],
        )?;
        let mut intent = serde_json::json!({"message_id": message_id});
        if let Some(due_at_ms) = due_at_ms {
            intent["due_at_ms"] = serde_json::json!(due_at_ms);
        }
        tx.execute(
            "INSERT INTO operations
             (id, idempotency_key, kind, target_incarnation_id, intent_json,
              created_at_ms, outcome)
             VALUES (?1, ?2, 'prompt', ?3, ?4, ?5, 'pending')",
            params![
                operation_id.to_string(),
                idempotency_key,
                recipient_incarnation.to_string(),
                intent.to_string(),
                now
            ],
        )
        .map_err(map_constraint)?;
        tx.execute(
            "INSERT INTO deliveries
             (message_id, recipient_incarnation_id, attempt_number, scheduled_at_ms,
              outcome, operation_id)
             VALUES (?1, ?2, 1, ?3, ?4, ?5)",
            params![
                message_id.to_string(),
                recipient_incarnation.to_string(),
                schedule.scheduled_at_ms,
                schedule.outcome,
                operation_id.to_string()
            ],
        )?;
        tx.commit()?;
        Ok(CreatedTell {
            message_id,
            operation_id,
        })
    }

    /// Persist a correlated progress or final reply and its delivery intent.
    ///
    /// The ask message ID alone resolves the exact owing sender and waiting
    /// recipient from the durable obligation. Send intent binds the waiter's
    /// receive path: a `herdr_prompt` waiter to its unique Ready incarnation,
    /// a `socket_inbox` waiter to that inbox with no Herdr prompt. Progress
    /// sets the obligation to `in_progress` immediately. Final resolution
    /// waits for accepted delivery: Herdr prompt acceptance, or socket ACK.
    /// Persist is not acceptance.
    ///
    /// # Errors
    ///
    /// Returns a conflict for an unknown or terminal `reply_to`, a missing or
    /// ambiguous Ready waiting incarnation, an ended socket waiter, or a
    /// reused idempotency key.
    pub fn create_reply(
        &mut self,
        reply_to: MessageId,
        requester_agent_id: LogicalAgentId,
        body: &str,
        disposition: ReplyDisposition,
        idempotency_key: &str,
    ) -> Result<CreatedReply, StoreError> {
        self.create_reply_with_due(
            reply_to,
            requester_agent_id,
            body,
            disposition,
            idempotency_key,
            None,
        )
    }

    /// Persist a reply for immediate or delayed delivery.
    ///
    /// # Errors
    ///
    /// Returns the same conflicts as [`Self::create_reply`].
    #[allow(clippy::too_many_lines)]
    pub fn create_reply_with_due(
        &mut self,
        reply_to: MessageId,
        requester_agent_id: LogicalAgentId,
        body: &str,
        disposition: ReplyDisposition,
        idempotency_key: &str,
        due_at_ms: Option<i64>,
    ) -> Result<CreatedReply, StoreError> {
        let message_id = MessageId::new();
        let operation_id = OperationId::new();
        let now = now_millis()?;
        let tx = self.connection.transaction()?;
        let owners: Option<(String, String, String)> = tx
            .query_row(
                "SELECT owing_agent_id, waiting_agent_id, state FROM obligations WHERE ask_message_id = ?1",
                [reply_to.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()?;
        let Some((owing, waiting, state)) = owners else {
            return Err(StoreError::Conflict(
                "reply_to does not name an obligation".into(),
            ));
        };
        if matches!(state.as_str(), "resolved" | "cancelled" | "orphaned") {
            return Err(StoreError::Conflict(
                "obligation is already terminal".into(),
            ));
        }
        let waiting_agent = LogicalAgentId::parse(&waiting).ok_or_else(|| {
            StoreError::InvalidRecord(format!("invalid waiting agent id {waiting}"))
        })?;
        if requester_agent_id.to_string() != owing {
            return Err(StoreError::Conflict(
                "requester does not own the obligation".into(),
            ));
        }
        let waiting_identity = waiter_identity(&tx, waiting_agent)?;
        if waiting_identity.ended {
            return Err(StoreError::Conflict(format!(
                "socket waiter {waiting_agent} is no longer a delivery target"
            )));
        }
        tx.execute(
            "INSERT INTO messages
             (id, sender_agent_id, recipient_agent_id, kind, body, created_at_ms,
              reply_to_message_id, disposition, creates_obligation)
             VALUES (?1, ?2, ?3, 'reply', ?4, ?5, ?6, ?7, 0)",
            params![
                message_id.to_string(),
                owing,
                waiting,
                body,
                now,
                reply_to.to_string(),
                disposition_name(disposition),
            ],
        )?;
        match waiting_identity.transport {
            DeliveryTransport::SocketInbox => {
                queue_socket_inbox_delivery(&tx, message_id, waiting_agent, now)?;
                tx.execute(
                    "INSERT INTO socket_inbox_keys (idempotency_key, message_id)
                     VALUES (?1, ?2)",
                    params![idempotency_key, message_id.to_string()],
                )
                .map_err(map_constraint)?;
                apply_reply_obligation_activity(&tx, reply_to, disposition, now)?;
                tx.commit()?;
                return Ok(CreatedReply {
                    message_id,
                    operation_id: None,
                    recipient_incarnation: None,
                    disposition,
                });
            }
            DeliveryTransport::HerdrPrompt => {}
        }
        let recipient_incarnation = ready_incarnation_for_agent(&tx, waiting_agent)?;
        let intent = serde_json::json!({
            "message_id": message_id,
            "reply_to": reply_to,
            "disposition": disposition_name(disposition),
            "recipient_incarnation_id": recipient_incarnation
        });
        tx.execute(
            "INSERT INTO operations
             (id, idempotency_key, kind, target_incarnation_id, intent_json, created_at_ms, outcome)
             VALUES (?1, ?2, 'prompt', ?3, ?4, ?5, 'pending')",
            params![
                operation_id.to_string(),
                idempotency_key,
                recipient_incarnation.to_string(),
                intent.to_string(),
                now
            ],
        )
        .map_err(map_constraint)?;
        let delivery_outcome = if due_at_ms.is_some() {
            "queued"
        } else {
            "pending"
        };
        tx.execute(
            "INSERT INTO deliveries
             (message_id, recipient_incarnation_id, attempt_number, scheduled_at_ms, outcome, operation_id)
             VALUES (?1, ?2, 1, ?3, ?4, ?5)",
            params![
                message_id.to_string(),
                recipient_incarnation.to_string(),
                due_at_ms.unwrap_or(now),
                delivery_outcome,
                operation_id.to_string()
            ],
        )?;
        apply_reply_obligation_activity(&tx, reply_to, disposition, now)?;
        tx.commit()?;
        Ok(CreatedReply {
            message_id,
            operation_id: Some(operation_id),
            recipient_incarnation: Some(recipient_incarnation),
            disposition,
        })
    }

    /// Bind the waiter's receive path for an open reply obligation.
    ///
    /// # Errors
    ///
    /// Returns a conflict unless the obligation is open, the requester owes it,
    /// and the waiter is either uniquely Ready or an active socket waiter.
    pub fn reply_receive_path(
        &self,
        reply_to: MessageId,
        requester_agent_id: LogicalAgentId,
    ) -> Result<ReplyReceivePath, StoreError> {
        let parties: Option<(String, String)> = self
            .connection
            .query_row(
                "SELECT waiting_agent_id, owing_agent_id FROM obligations
                 WHERE ask_message_id = ?1 AND state IN ('open','in_progress')",
                [reply_to.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        let Some((waiting, owing)) = parties else {
            return Err(StoreError::Conflict(
                "reply_to does not name an open obligation".into(),
            ));
        };
        // A reply is the owing agent's verb. The asker replying to its own ask
        // would have its words delivered back to itself attributed to the
        // owing agent — forged provenance — so it is refused outright.
        if requester_agent_id.to_string() != owing {
            let owing_name =
                self.agent_address(LogicalAgentId::parse(&owing).ok_or_else(|| {
                    StoreError::InvalidRecord(format!("invalid owing agent id {owing}"))
                })?)?;
            return Err(StoreError::Conflict(format!(
                "only the owing agent can reply to this ask; it is owed by {owing_name} \
                 (agent {owing}) — to send the asker information, use `kelpie tell`"
            )));
        }
        let waiting = parse_logical_agent_id(&waiting)?;
        let identity = waiter_identity(&self.connection, waiting)?;
        if identity.ended {
            return Err(StoreError::Conflict(format!(
                "socket waiter {waiting} is no longer a delivery target"
            )));
        }
        match identity.transport {
            DeliveryTransport::SocketInbox => Ok(ReplyReceivePath::SocketInbox),
            DeliveryTransport::HerdrPrompt => {
                ready_incarnation_for_agent(&self.connection, waiting)
                    .map(ReplyReceivePath::HerdrPrompt)
            }
        }
    }

    /// Return the correlation and disposition needed to render a queued reply.
    ///
    /// # Errors
    ///
    /// Returns an invalid-record error unless the message is a complete reply.
    pub fn reply_rendering(
        &self,
        message_id: MessageId,
    ) -> Result<(MessageId, ReplyDisposition), StoreError> {
        let values: Option<(String, String)> = self
            .connection
            .query_row(
                "SELECT reply_to_message_id, disposition FROM messages
                 WHERE id = ?1 AND kind = 'reply'",
                [message_id.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        let (reply_to, disposition) = values.ok_or_else(|| {
            StoreError::InvalidRecord(format!("message {message_id} is not a complete reply"))
        })?;
        let reply_to = parse_message_id(&reply_to)?;
        let disposition = match disposition.as_str() {
            "progress" => ReplyDisposition::Progress,
            "final" => ReplyDisposition::Final,
            other => {
                return Err(StoreError::InvalidRecord(format!(
                    "invalid reply disposition {other}"
                )));
            }
        };
        Ok((reply_to, disposition))
    }

    /// Resolve the unique Ready incarnation for one logical agent.
    ///
    /// # Errors
    ///
    /// Returns a conflict when zero or more than one Ready incarnation exists.
    pub fn resolve_ready_incarnation(
        &self,
        logical_agent_id: LogicalAgentId,
    ) -> Result<IncarnationId, StoreError> {
        let mut statement = self.connection.prepare(
            "SELECT id FROM incarnations
             WHERE logical_agent_id = ?1 AND state = 'ready'
             ORDER BY created_at_ms ASC",
        )?;
        let rows = statement.query_map([logical_agent_id.to_string()], |row| {
            row.get::<_, String>(0)
        })?;
        let mut matches = Vec::new();
        for row in rows {
            matches.push(row?);
        }
        match matches.as_slice() {
            [incarnation_id] => IncarnationId::parse(incarnation_id).ok_or_else(|| {
                StoreError::InvalidRecord(format!("invalid incarnation id {incarnation_id}"))
            }),
            [] => Err(StoreError::Conflict(format!(
                "no ready incarnation for waiting agent {logical_agent_id}"
            ))),
            _ => Err(StoreError::Conflict(format!(
                "ambiguous ready incarnation for waiting agent {logical_agent_id}"
            ))),
        }
    }

    /// Collect every logical agent holding a public name and every unresolved
    /// obligation touching them, with both parties resolved to names and
    /// liveness.
    ///
    /// Read-only. This is the data behind create-new refusals and `name-info`.
    ///
    /// # Errors
    ///
    /// Returns store errors from the underlying queries.
    pub fn name_info(&self, public_name: &str) -> Result<NameInfo, StoreError> {
        Self::name_info_on(&self.connection, public_name)
    }

    fn name_info_on(conn: &Connection, public_name: &str) -> Result<NameInfo, StoreError> {
        let mut claimants = Vec::new();
        let mut statement = conn.prepare(
            "SELECT l.id, l.created_at_ms,
                    EXISTS (SELECT 1 FROM incarnations i
                            WHERE i.logical_agent_id = l.id AND i.state = 'ready'),
                    (SELECT COUNT(*) FROM obligations o
                     WHERE o.state IN ('open', 'in_progress')
                       AND (o.owing_agent_id = l.id OR o.waiting_agent_id = l.id))
             FROM logical_agents l
             WHERE l.public_name = ?1
             ORDER BY l.created_at_ms ASC, l.id ASC",
        )?;
        let rows = statement.query_map([public_name], |row| {
            Ok(NameClaimant {
                logical_agent_id: row.get(0)?,
                created_at_ms: row.get(1)?,
                has_ready_incarnation: row.get::<_, i64>(2)? != 0,
                unresolved_count: row.get(3)?,
            })
        })?;
        for row in rows {
            claimants.push(row?);
        }

        let mut unresolved = Vec::new();
        let mut statement = conn.prepare(
            "SELECT o.ask_message_id, o.state, o.created_at_ms, o.last_activity_at_ms,
                    o.waiting_agent_id, wa.public_name,
                    EXISTS (SELECT 1 FROM incarnations i
                            WHERE i.logical_agent_id = o.waiting_agent_id
                              AND i.state = 'ready'),
                    o.owing_agent_id, ra.public_name,
                    EXISTS (SELECT 1 FROM incarnations i
                            WHERE i.logical_agent_id = o.owing_agent_id
                              AND i.state = 'ready')
             FROM obligations o
             JOIN logical_agents wa ON wa.id = o.waiting_agent_id
             JOIN logical_agents ra ON ra.id = o.owing_agent_id
             WHERE o.state IN ('open', 'in_progress')
               AND (o.waiting_agent_id IN (SELECT id FROM logical_agents
                                           WHERE public_name = ?1)
                    OR o.owing_agent_id IN (SELECT id FROM logical_agents
                                            WHERE public_name = ?1))
             ORDER BY o.created_at_ms ASC, o.ask_message_id ASC",
        )?;
        let rows = statement.query_map([public_name], |row| {
            Ok(NameObligation {
                ask_message_id: row.get(0)?,
                state: row.get(1)?,
                created_at_ms: row.get(2)?,
                last_activity_at_ms: row.get(3)?,
                asker_agent_id: row.get(4)?,
                asker_name: row.get(5)?,
                asker_live: row.get::<_, i64>(6)? != 0,
                responder_agent_id: row.get(7)?,
                responder_name: row.get(8)?,
                responder_live: row.get::<_, i64>(9)? != 0,
            })
        })?;
        for row in rows {
            unresolved.push(row?);
        }

        Ok(NameInfo {
            public_name: public_name.to_string(),
            claimants,
            unresolved,
        })
    }

    /// Compose the create-new refusal for a claimed public name: every
    /// unresolved ask with both parties named and marked live or not, then the
    /// three honest exits — continue the claimant, cancel the asks, or take a
    /// different name. The refusal names the prior agent id, the unresolved
    /// count, and both remedies; the asks themselves are here so the operator
    /// does not re-derive the diagnosis by hand.
    fn name_conflict_message(info: &NameInfo) -> String {
        let prior = info
            .claimants
            .iter()
            .max_by_key(|claimant| {
                (
                    claimant.unresolved_count,
                    std::cmp::Reverse(claimant.logical_agent_id.clone()),
                )
            })
            .cloned()
            .expect("a refusal is only composed when an obligation has a claimant");
        let mut text = format!(
            "public name {} belongs to {} logical agent(s); create-new under it \
             would strand {} unresolved obligation(s)",
            info.public_name,
            info.claimants.len(),
            info.unresolved.len(),
        );
        for obligation in info.unresolved.iter().take(4) {
            let _ = write!(
                text,
                "\n  ask {} ({}): asker {} ({}, {}), responder {} ({}, {})",
                obligation.ask_message_id,
                obligation.state,
                obligation.asker_name,
                obligation.asker_agent_id,
                if obligation.asker_live {
                    "live"
                } else {
                    "not live"
                },
                obligation.responder_name,
                obligation.responder_agent_id,
                if obligation.responder_live {
                    "live"
                } else {
                    "not live"
                },
            );
        }
        if info.unresolved.len() > 4 {
            let _ = write!(
                text,
                "\n  …and {} more obligation(s)",
                info.unresolved.len() - 4
            );
        }
        let example = info
            .unresolved
            .first()
            .expect("a refusal is only composed when an obligation exists");
        let _ = write!(
            text,
            "\nremedies:\n  - continue that agent: kelpie adopt --pane <pane> \
             --terminal <terminal> --logical-id {}\n  - cancel each ask, e.g.: \
             kelpie cancel {} --reason \"<why>\" --sender-id {}\n  - or take a \
             different name: rename this agent in herdr, then run kelpie adopt\
             \nkelpie name-info {} shows the full picture any time",
            prior.logical_agent_id,
            example.ask_message_id,
            example.asker_agent_id,
            info.public_name,
        );
        text
    }

    /// Resolve a public-name alias to the exact ready logical agent and incarnation.
    ///
    /// Resolution happens at send intent time. The returned IDs are the durable
    /// targets; later alias reuse must not retarget stored records that already
    /// captured these IDs.
    ///
    /// # Errors
    ///
    /// Returns a conflict when the alias matches zero or more than one Ready
    /// incarnation.
    pub fn resolve_ready_alias(
        &self,
        public_name: &str,
    ) -> Result<(LogicalAgentId, IncarnationId), StoreError> {
        self.find_ready_alias(public_name)?.ok_or_else(|| {
            // Absence of a binding is not absence of a runtime: a live Herdr
            // agent can hold this name without Kelpie having adopted it. But
            // when prior logical agents hold the name, that is the likelier
            // truth, so the error says so and points at the full picture
            // instead of leaving the caller to re-derive it.
            StoreError::Conflict(self.alias_unready_message(public_name))
        })
    }

    /// Compose the no-Ready-alias conflict: when the name has claimants, name
    /// them, count what they are owed, and point at `name-info`; when it has
    /// none, keep the live-but-unadopted hint.
    fn alias_unready_message(&self, public_name: &str) -> String {
        let Ok(info) = Self::name_info_on(&self.connection, public_name) else {
            return format!(
                "no ready agent for alias {public_name}; a live Herdr agent may hold that name unadopted"
            );
        };
        if info.claimants.is_empty() {
            return format!(
                "no ready agent for alias {public_name}; a live Herdr agent may hold that name unadopted"
            );
        }
        let owed: i64 = info.claimants.iter().map(|c| c.unresolved_count).sum();
        format!(
            "no ready agent for alias {public_name}; {} logical agent(s) already hold \
             this name, none of them live, {} unresolved obligation(s) — \
             kelpie name-info {public_name} shows who, and kelpie adopt \
             --logical-id <id> or kelpie cancel settles it",
            info.claimants.len(),
            owed,
        )
    }

    /// Find a unique Ready alias without treating absence as an error.
    ///
    /// # Errors
    ///
    /// Returns a conflict when more than one Ready incarnation has the alias.
    pub fn find_ready_alias(
        &self,
        public_name: &str,
    ) -> Result<Option<(LogicalAgentId, IncarnationId)>, StoreError> {
        let mut statement = self.connection.prepare(
            "SELECT l.id, i.id
             FROM logical_agents l
             JOIN incarnations i ON i.logical_agent_id = l.id
             WHERE l.public_name = ?1 AND i.state = 'ready'
             ORDER BY i.created_at_ms ASC",
        )?;
        let rows = statement.query_map([public_name], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;
        let mut matches = Vec::new();
        for row in rows {
            matches.push(row?);
        }
        match matches.as_slice() {
            [(agent_id, incarnation_id)] => {
                let agent = LogicalAgentId::parse(agent_id).ok_or_else(|| {
                    StoreError::InvalidRecord(format!("invalid logical agent id {agent_id}"))
                })?;
                let incarnation = IncarnationId::parse(incarnation_id).ok_or_else(|| {
                    StoreError::InvalidRecord(format!("invalid incarnation id {incarnation_id}"))
                })?;
                Ok(Some((agent, incarnation)))
            }
            [] => Ok(None),
            _ => Err(StoreError::Conflict(format!(
                "alias {public_name} is ambiguous among ready agents"
            ))),
        }
    }

    /// Read the exact recipient incarnation recorded for a message delivery.
    ///
    /// # Errors
    ///
    /// Returns an error when the delivery is missing or malformed.
    pub fn delivery_recipient_incarnation(
        &self,
        message_id: MessageId,
    ) -> Result<IncarnationId, StoreError> {
        let value: String = self.connection.query_row(
            "SELECT recipient_incarnation_id FROM deliveries
             WHERE message_id = ?1
             ORDER BY attempt_number ASC
             LIMIT 1",
            [message_id.to_string()],
            |row| row.get(0),
        )?;
        IncarnationId::parse(&value).ok_or_else(|| {
            StoreError::InvalidRecord(format!("invalid delivery incarnation {value}"))
        })
    }

    /// Read the exact message parties stored for a durable message.
    ///
    /// # Errors
    ///
    /// Returns an error when the message is missing or malformed.
    pub fn message_parties(
        &self,
        message_id: MessageId,
    ) -> Result<(LogicalAgentId, LogicalAgentId), StoreError> {
        let (sender, recipient): (String, String) = self.connection.query_row(
            "SELECT sender_agent_id, recipient_agent_id FROM messages WHERE id = ?1",
            [message_id.to_string()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        let sender = LogicalAgentId::parse(&sender).ok_or_else(|| {
            StoreError::InvalidRecord(format!("invalid sender agent id {sender}"))
        })?;
        let recipient = LogicalAgentId::parse(&recipient).ok_or_else(|| {
            StoreError::InvalidRecord(format!("invalid recipient agent id {recipient}"))
        })?;
        Ok((sender, recipient))
    }

    /// Read the current durable operation outcome.
    ///
    /// # Errors
    ///
    /// Returns an error if the operation is missing or contains invalid state.
    pub fn operation_outcome(&self, id: OperationId) -> Result<OperationOutcome, StoreError> {
        let value: String = self.connection.query_row(
            "SELECT outcome FROM operations WHERE id = ?1",
            [id.to_string()],
            |row| row.get(0),
        )?;
        parse_operation_outcome(&value)
    }

    /// Persist one standalone clear before its Herdr write.
    ///
    /// # Errors
    ///
    /// Returns a conflict unless the IDs name the recipient's exact Ready
    /// incarnation or the idempotency key is unused.
    pub fn create_clear(
        &mut self,
        recipient: LogicalAgentId,
        recipient_incarnation: IncarnationId,
        command: &str,
        pre_clear_session: &serde_json::Value,
        prompt_settle_delay_ms: i64,
        idempotency_key: &str,
    ) -> Result<OperationId, StoreError> {
        let operation_id = OperationId::new();
        let now = now_millis()?;
        let tx = self.connection.transaction()?;
        let owner: Option<String> = tx
            .query_row(
                "SELECT logical_agent_id FROM incarnations WHERE id = ?1 AND state = 'ready'",
                [recipient_incarnation.to_string()],
                |row| row.get(0),
            )
            .optional()?;
        if owner.as_deref() != Some(&recipient.to_string()) {
            return Err(StoreError::Conflict(
                "recipient incarnation is not the recipient's exact ready incarnation".into(),
            ));
        }
        let active_renew: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM renews WHERE incarnation_id = ?1
             AND phase IN ('preparing','ready','clearing','injected','timed_out'))",
            [recipient_incarnation.to_string()],
            |row| row.get(0),
        )?;
        if active_renew {
            return Err(StoreError::Conflict(
                "recipient incarnation has an active renew".into(),
            ));
        }
        let active_clear: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM operations WHERE kind = 'clear'
             AND target_incarnation_id = ?1 AND outcome IN ('pending','accepted'))",
            [recipient_incarnation.to_string()],
            |row| row.get(0),
        )?;
        if active_clear {
            return Err(StoreError::Conflict(
                "recipient incarnation already has an active clear".into(),
            ));
        }
        tx.execute(
            "INSERT INTO operations
             (id, idempotency_key, kind, target_incarnation_id, intent_json,
              created_at_ms, outcome)
             VALUES (?1, ?2, 'clear', ?3, ?4, ?5, 'pending')",
            params![
                operation_id.to_string(),
                idempotency_key,
                recipient_incarnation.to_string(),
                serde_json::json!({
                    "recipient": recipient,
                    "command": command,
                    "pre_clear_session": pre_clear_session,
                    "prompt_settle_delay_ms": prompt_settle_delay_ms,
                })
                .to_string(),
                now,
            ],
        )
        .map_err(map_constraint)?;
        tx.commit()?;
        Ok(operation_id)
    }

    /// Validate that one exact Ready incarnation is available for clear.
    ///
    /// # Errors
    ///
    /// Returns a conflict for an ownership mismatch or active renew.
    pub fn validate_clear_target(
        &self,
        recipient: LogicalAgentId,
        recipient_incarnation: IncarnationId,
    ) -> Result<(), StoreError> {
        let available: bool = self.connection.query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM incarnations i
                 WHERE i.id = ?1 AND i.logical_agent_id = ?2 AND i.state = 'ready'
                   AND NOT EXISTS(
                       SELECT 1 FROM renews r WHERE r.incarnation_id = i.id
                         AND r.phase IN ('preparing','ready','clearing','injected','timed_out')
                   )
                   AND NOT EXISTS(
                       SELECT 1 FROM operations o WHERE o.kind = 'clear'
                         AND o.target_incarnation_id = i.id
                         AND o.outcome IN ('pending','accepted')
                   )
             )",
            params![recipient_incarnation.to_string(), recipient.to_string()],
            |row| row.get(0),
        )?;
        if !available {
            return Err(StoreError::Conflict(
                "recipient is not an exact Ready incarnation available for clear".into(),
            ));
        }
        Ok(())
    }

    /// Return the latest prompt write attempt into one exact incarnation.
    ///
    /// # Errors
    ///
    /// Returns an error if the operation history cannot be read.
    pub fn last_prompt_attempt_at_ms(
        &self,
        incarnation_id: IncarnationId,
    ) -> Result<Option<i64>, StoreError> {
        self.connection
            .query_row(
                "SELECT MAX(started_at_ms) FROM (
                     SELECT a.started_at_ms
                     FROM operation_attempts a
                     JOIN operations o ON o.id = a.operation_id
                     WHERE o.kind = 'prompt' AND o.target_incarnation_id = ?1
                       AND a.phase != 'prepared'
                     UNION ALL
                     SELECT r.started_at_ms
                     FROM reminder_attempts r
                     WHERE r.recipient_incarnation_id = ?1 AND r.phase != 'prepared'
                     UNION ALL
                     SELECT o.resolved_at_ms
                     FROM operations o
                     WHERE o.kind = 'clear' AND o.target_incarnation_id = ?1
                       AND o.outcome = 'succeeded'
                     UNION ALL
                     SELECT a.started_at_ms
                     FROM operation_attempts a
                     JOIN operations o ON o.id = a.operation_id
                     WHERE o.kind = 'clear' AND o.target_incarnation_id = ?1
                       AND o.outcome = 'unknown' AND a.phase != 'prepared'
                 )",
                [incarnation_id.to_string()],
                |row| row.get(0),
            )
            .map_err(StoreError::Sql)
    }

    /// Whether a standalone clear is between durable intent and terminal outcome.
    ///
    /// # Errors
    ///
    /// Returns an error if operation state cannot be read.
    pub fn clear_in_flight(&self, incarnation_id: IncarnationId) -> Result<bool, StoreError> {
        self.connection
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM operations WHERE kind = 'clear'
                 AND target_incarnation_id = ?1 AND outcome IN ('pending','accepted'))",
                [incarnation_id.to_string()],
                |row| row.get(0),
            )
            .map_err(StoreError::Sql)
    }

    /// Return the latest standalone-clear event that requires prompt spacing.
    ///
    /// # Errors
    ///
    /// Returns an error if operation history cannot be read.
    pub fn last_clear_spacing_at_ms(
        &self,
        incarnation_id: IncarnationId,
    ) -> Result<Option<i64>, StoreError> {
        self.connection
            .query_row(
                "SELECT MAX(spacing_at_ms) FROM (
                     SELECT o.resolved_at_ms AS spacing_at_ms
                     FROM operations o
                     WHERE o.kind = 'clear' AND o.target_incarnation_id = ?1
                       AND o.outcome = 'succeeded'
                     UNION ALL
                     SELECT a.started_at_ms AS spacing_at_ms
                     FROM operation_attempts a
                     JOIN operations o ON o.id = a.operation_id
                     WHERE o.kind = 'clear' AND o.target_incarnation_id = ?1
                       AND o.outcome = 'unknown' AND a.phase != 'prepared'
                 )",
                [incarnation_id.to_string()],
                |row| row.get(0),
            )
            .map_err(StoreError::Sql)
    }

    /// Return the clear that must be resolved before another may be submitted.
    ///
    /// A clear whose rotation was never observed is `unknown`, and `unknown`
    /// means the command may well have landed. Kelpie never blindly resends an
    /// unknown external effect, and a clear is the least forgivable one to
    /// resend: each retry destroys a real context to re-ask a question the
    /// observation channel is not answering.
    ///
    /// The block lifts by itself the moment evidence arrives. Reconciliation
    /// refreshes the recorded backend-native session reference, so a rotation
    /// observed after that clear resolves the ambiguity the clear left behind
    /// and a further clear is allowed again. Nothing here is time-based.
    ///
    /// # Errors
    ///
    /// Returns an error if operation history cannot be read.
    pub fn unproven_clear(
        &self,
        incarnation_id: IncarnationId,
    ) -> Result<Option<OperationId>, StoreError> {
        let blocking: Option<String> = self
            .connection
            .query_row(
                "SELECT o.id FROM operations o
                 JOIN incarnations i ON i.id = o.target_incarnation_id
                 WHERE o.kind = 'clear' AND o.target_incarnation_id = ?1
                   AND o.outcome = 'unknown'
                   AND COALESCE(i.native_session_rotated_at_ms, 0)
                       <= COALESCE(o.resolved_at_ms, o.created_at_ms)
                 ORDER BY COALESCE(o.resolved_at_ms, o.created_at_ms) DESC
                 LIMIT 1",
                [incarnation_id.to_string()],
                |row| row.get(0),
            )
            .optional()?;
        blocking
            .map(|id| {
                OperationId::parse(&id)
                    .ok_or_else(|| StoreError::InvalidRecord(format!("invalid operation id {id}")))
            })
            .transpose()
    }

    /// Complete a standalone clear after its required proof.
    ///
    /// `new_session` is absent only for a backend whose next prompt allocates
    /// the replacement conversation.
    ///
    /// # Errors
    ///
    /// Returns a conflict unless the exact clear operation remains pending.
    pub fn complete_clear(
        &mut self,
        operation_id: OperationId,
        incarnation_id: IncarnationId,
        new_session: Option<&serde_json::Value>,
        prompt_settle_delay_ms: i64,
    ) -> Result<(), StoreError> {
        let now = now_millis()?;
        let tx = self.connection.transaction()?;
        let changed = tx.execute(
            "UPDATE operations SET outcome = 'succeeded', resolved_at_ms = ?1
             WHERE id = ?2 AND kind = 'clear' AND target_incarnation_id = ?3
               AND outcome = 'pending'",
            params![now, operation_id.to_string(), incarnation_id.to_string()],
        )?;
        if changed != 1 {
            return Err(StoreError::Conflict(
                "clear completion does not own the pending operation".into(),
            ));
        }
        if let Some(session) = new_session {
            let updated = tx.execute(
                "UPDATE incarnations SET observed_native_session_json = ?1,
                 native_session_rotated_at_ms = ?2 WHERE id = ?3 AND state = 'ready'",
                params![session.to_string(), now, incarnation_id.to_string()],
            )?;
            if updated != 1 {
                return Err(StoreError::Conflict(
                    "clear completion belongs to a replacement runtime".into(),
                ));
            }
        }
        tx.execute(
            "UPDATE operation_attempts SET phase = 'response_committed'
             WHERE operation_id = ?1 AND attempt_number = (
                 SELECT MAX(attempt_number) FROM operation_attempts WHERE operation_id = ?1
             )",
            [operation_id.to_string()],
        )?;
        tx.execute(
            "UPDATE deliveries SET scheduled_at_ms = MAX(scheduled_at_ms, ?1)
             WHERE recipient_incarnation_id = ?2 AND outcome = 'queued'",
            params![
                now.saturating_add(prompt_settle_delay_ms),
                incarnation_id.to_string()
            ],
        )?;
        tx.commit()?;
        Ok(())
    }

    /// Read the durable delivery outcome associated with an operation.
    ///
    /// # Errors
    ///
    /// Returns an error if the delivery is absent or malformed.
    pub fn delivery_outcome(&self, id: OperationId) -> Result<DeliveryOutcome, StoreError> {
        let value: String = self.connection.query_row(
            "SELECT outcome FROM deliveries WHERE operation_id = ?1",
            [id.to_string()],
            |row| row.get(0),
        )?;
        parse_delivery_outcome(&value)
    }

    /// Read the durable delivery outcome for one message.
    ///
    /// # Errors
    ///
    /// Returns an error if the delivery is absent or malformed.
    pub fn delivery_outcome_for_message(
        &self,
        message_id: MessageId,
    ) -> Result<DeliveryOutcome, StoreError> {
        let value: String = self.connection.query_row(
            "SELECT outcome FROM deliveries WHERE message_id = ?1",
            [message_id.to_string()],
            |row| row.get(0),
        )?;
        parse_delivery_outcome(&value)
    }

    /// Persist an operator notice before any best-effort display attempt.
    ///
    /// # Errors
    ///
    /// Returns an error if the durable inbox write fails.
    pub fn create_operator_notice(&mut self, body: &str) -> Result<OperatorNoticeId, StoreError> {
        let id = OperatorNoticeId::new();
        self.connection.execute(
            "INSERT INTO operator_notices (id, body, created_at_ms) VALUES (?1, ?2, ?3)",
            params![id.to_string(), body, now_millis()?],
        )?;
        Ok(id)
    }

    /// Record durable retirement intent without causing a runtime or filesystem effect.
    ///
    /// # Errors
    ///
    /// Returns a conflict unless the exact incarnation is currently ready.
    pub fn request_retirement(
        &mut self,
        incarnation_id: IncarnationId,
        idempotency_key: &str,
    ) -> Result<OperationId, StoreError> {
        let operation_id = OperationId::new();
        let now = now_millis()?;
        let tx = self.connection.transaction()?;
        let changed = tx.execute(
            "UPDATE incarnations SET state = 'retiring' WHERE id = ?1 AND state = 'ready'",
            [incarnation_id.to_string()],
        )?;
        if changed != 1 {
            return Err(StoreError::Conflict(
                "only an exact ready incarnation can enter retirement".into(),
            ));
        }
        tx.execute(
            "INSERT INTO operations
             (id, idempotency_key, kind, target_incarnation_id, intent_json,
              created_at_ms, outcome)
             VALUES (?1, ?2, 'retire', ?3, ?4, ?5, 'accepted')",
            params![
                operation_id.to_string(),
                idempotency_key,
                incarnation_id.to_string(),
                serde_json::json!({"incarnation_id": incarnation_id}).to_string(),
                now
            ],
        )
        .map_err(map_constraint)?;
        tx.commit()?;
        Ok(operation_id)
    }

    /// Return all durable operator notices in creation order.
    ///
    /// # Errors
    ///
    /// Returns an error if an inbox record is malformed or unreadable.
    pub fn operator_notices(&self) -> Result<Vec<OperatorNotice>, StoreError> {
        let mut statement = self.connection.prepare(
            "SELECT id, body, created_at_ms, acknowledged_at_ms IS NOT NULL
             FROM operator_notices ORDER BY created_at_ms, id",
        )?;
        let rows = statement.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
            ))
        })?;
        let mut notices = Vec::new();
        for row in rows {
            let (id, body, created_at_ms, acknowledged) = row?;
            notices.push(OperatorNotice {
                id: OperatorNoticeId::parse(&id).ok_or_else(|| {
                    StoreError::InvalidRecord(format!("invalid operator notice id {id}"))
                })?,
                body,
                created_at_ms,
                acknowledged,
            });
        }
        Ok(notices)
    }

    /// Read the exact observed binding for a ready incarnation.
    ///
    /// # Errors
    ///
    /// Returns a conflict unless the incarnation is ready and fully bound.
    pub fn ready_binding(&self, id: IncarnationId) -> Result<ReadyBinding, StoreError> {
        let binding: Option<(String, String)> = self
            .connection
            .query_row(
                "SELECT observed_pane_id, observed_terminal_id FROM incarnations
                 WHERE id = ?1 AND state = 'ready'",
                [id.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        binding
            .map(|(pane_id, terminal_id)| ReadyBinding {
                pane_id,
                terminal_id,
            })
            .ok_or_else(|| StoreError::Conflict("incarnation has no exact ready binding".into()))
    }

    /// The binding of an incarnation that may still be retired, with its state.
    ///
    /// Retirement writes durable intent before it touches Herdr, so a close that
    /// Herdr rejected leaves a `retiring` incarnation whose runtime is still the
    /// one the caller meant. Resolving `retiring` as well as `ready` is what
    /// lets that retirement be finished instead of stranded; the caller decides
    /// what each state permits.
    ///
    /// # Errors
    ///
    /// Returns a conflict unless the incarnation is ready or already retiring.
    pub fn retirable_binding(
        &self,
        id: IncarnationId,
    ) -> Result<(ReadyBinding, IncarnationState), StoreError> {
        let binding: Option<(String, String, String)> = self
            .connection
            .query_row(
                "SELECT observed_pane_id, observed_terminal_id, state FROM incarnations
                 WHERE id = ?1 AND state IN ('ready', 'retiring')",
                [id.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()?;
        let (pane_id, terminal_id, state) = binding
            .ok_or_else(|| StoreError::Conflict("incarnation has no retirable binding".into()))?;
        Ok((
            ReadyBinding {
                pane_id,
                terminal_id,
            },
            parse_incarnation_state(&state)?,
        ))
    }

    /// Any other incarnation that is Ready on one exact pane and terminal.
    ///
    /// A pane, a terminal, a backend kind, and a public name are all reusable,
    /// so a snapshot proving "something matching is live here" cannot prove it
    /// is the *same* incarnation. A retirement recorded against an older one
    /// must never close the runtime a newer one now owns, and this is what
    /// distinguishes them.
    ///
    /// # Errors
    ///
    /// Returns an error if the durable record cannot be read or is malformed.
    pub fn ready_incarnation_other_than(
        &self,
        id: IncarnationId,
        pane_id: &str,
        terminal_id: &str,
    ) -> Result<Option<IncarnationId>, StoreError> {
        let holder: Option<String> = self
            .connection
            .query_row(
                "SELECT id FROM incarnations
                 WHERE observed_pane_id = ?1 AND observed_terminal_id = ?2
                   AND state = 'ready' AND id != ?3
                 ORDER BY created_at_ms DESC LIMIT 1",
                params![pane_id, terminal_id, id.to_string()],
                |row| row.get(0),
            )
            .optional()?;
        holder
            .map(|holder| {
                IncarnationId::parse(&holder).ok_or_else(|| {
                    StoreError::InvalidRecord(format!("invalid incarnation id {holder}"))
                })
            })
            .transpose()
    }

    /// The accepted retire operation already recorded for one incarnation.
    ///
    /// # Errors
    ///
    /// Returns a conflict when no unresolved retirement is recorded for it.
    pub fn open_retirement(&self, id: IncarnationId) -> Result<OperationId, StoreError> {
        let recorded: Option<String> = self
            .connection
            .query_row(
                "SELECT id FROM operations
                 WHERE target_incarnation_id = ?1 AND kind = 'retire' AND outcome = 'accepted'
                 ORDER BY created_at_ms DESC LIMIT 1",
                [id.to_string()],
                |row| row.get(0),
            )
            .optional()?;
        let recorded = recorded.ok_or_else(|| {
            StoreError::Conflict(format!("incarnation {id} has no unresolved retirement"))
        })?;
        OperationId::parse(&recorded)
            .ok_or_else(|| StoreError::InvalidRecord(format!("invalid operation id {recorded}")))
    }

    /// The logical agent one incarnation belongs to.
    ///
    /// # Errors
    ///
    /// Returns an error if the incarnation is absent or its owner is malformed.
    pub fn logical_agent_of(&self, id: IncarnationId) -> Result<LogicalAgentId, StoreError> {
        let owner: Option<String> = self
            .connection
            .query_row(
                "SELECT logical_agent_id FROM incarnations WHERE id = ?1",
                [id.to_string()],
                |row| row.get(0),
            )
            .optional()?;
        let owner =
            owner.ok_or_else(|| StoreError::Conflict(format!("incarnation {id} is absent")))?;
        LogicalAgentId::parse(&owner)
            .ok_or_else(|| StoreError::InvalidRecord(format!("invalid logical agent id {owner}")))
    }

    /// Read one exact incarnation's durable lifecycle state.
    ///
    /// # Errors
    ///
    /// Returns an error if the incarnation is absent or its state is malformed.
    pub fn incarnation_state(
        &self,
        id: IncarnationId,
    ) -> Result<crate::domain::IncarnationState, StoreError> {
        let value: String = self.connection.query_row(
            "SELECT state FROM incarnations WHERE id = ?1",
            [id.to_string()],
            |row| row.get(0),
        )?;
        parse_incarnation_state(&value)
    }

    /// Resolve the unique Ready incarnation bound to a live pane.
    ///
    /// # Errors
    ///
    /// Returns a conflict when the pane has zero or more than one Ready binding.
    pub fn ready_identity_for_pane(&self, pane_id: &str) -> Result<ReadyIdentity, StoreError> {
        self.find_ready_identity_for_pane(pane_id)?
            .ok_or_else(|| StoreError::Conflict(format!("no ready agent for pane {pane_id}")))
    }

    /// Find a unique Ready pane binding without treating absence as an error.
    ///
    /// # Errors
    ///
    /// Returns a conflict when more than one Ready incarnation uses the pane.
    pub fn find_ready_identity_for_pane(
        &self,
        pane_id: &str,
    ) -> Result<Option<ReadyIdentity>, StoreError> {
        find_ready_identity_query(
            self,
            "SELECT l.id, i.id, l.public_name
             FROM logical_agents l
             JOIN incarnations i ON i.logical_agent_id = l.id
             WHERE i.state = 'ready' AND i.observed_pane_id = ?1
             ORDER BY i.created_at_ms ASC",
            pane_id,
            "pane",
        )
    }

    /// Find the unique continuable logical agent for one live binding.
    ///
    /// `ready` is already bound. `retiring`, `retired`, and `superseded` left
    /// the runtime on purpose. `lost` and `unknown` on this exact pane,
    /// terminal, and backend are the prior occupant that lazy create-new would
    /// fork. `declared` or `failed` of that same backend is retried so a
    /// rejected name claim does not wedge the pane. `starting` on the same pane
    /// and terminal fails closed.
    ///
    /// # Errors
    ///
    /// Returns a conflict when more than one logical agent still occupies the
    /// pane and terminal, or when the unique occupant is not a lost, unknown,
    /// declared, or failed incarnation of the live backend.
    pub fn continuable_logical_agent_for_binding(
        &self,
        pane_id: &str,
        terminal_id: &str,
        backend_kind: &str,
    ) -> Result<Option<LogicalAgentId>, StoreError> {
        let mut statement = self.connection.prepare(
            "SELECT logical_agent_id, state, backend_kind
             FROM incarnations
             WHERE observed_pane_id = ?1
               AND observed_terminal_id = ?2
               AND state NOT IN ('ready', 'retiring', 'retired', 'superseded')
             ORDER BY logical_agent_id ASC",
        )?;
        let rows = statement.query_map(params![pane_id, terminal_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?;
        let mut occupants = Vec::new();
        for row in rows {
            occupants.push(row?);
        }
        if occupants.is_empty() {
            return Ok(None);
        }
        let mut ids = Vec::new();
        for (id, _, _) in &occupants {
            if !ids.iter().any(|existing| existing == id) {
                ids.push(id.clone());
            }
        }
        if ids.len() != 1 {
            return Err(StoreError::Conflict(format!(
                "pane {pane_id} terminal {terminal_id} has {} continuable logical agents; \
                 adopt --logical-id to continue one of {}",
                ids.len(),
                ids.join(", ")
            )));
        }
        let id = LogicalAgentId::parse(&ids[0]).ok_or_else(|| {
            StoreError::InvalidRecord(format!("invalid logical agent id {}", ids[0]))
        })?;
        let matches_live = occupants.iter().any(|(_, state, backend)| {
            matches!(state.as_str(), "lost" | "unknown" | "declared" | "failed")
                && backend == backend_kind
        });
        if matches_live {
            Ok(Some(id))
        } else {
            Err(StoreError::Conflict(format!(
                "pane {pane_id} terminal {terminal_id} has agent {id} but not a lost, unknown, \
                 declared, or failed {backend_kind} incarnation; adopt --logical-id to continue \
                 it, or adopt the live occupant as a new agent"
            )))
        }
    }

    /// Read a logical agent's current human-readable address.
    ///
    /// # Errors
    ///
    /// Returns an error when the logical agent does not exist.
    pub fn agent_address(&self, id: LogicalAgentId) -> Result<String, StoreError> {
        self.connection
            .query_row(
                "SELECT public_name FROM logical_agents WHERE id = ?1",
                [id.to_string()],
                |row| row.get(0),
            )
            .map_err(StoreError::Sql)
    }

    /// Read the current durable obligation state.
    ///
    /// # Errors
    ///
    /// Returns an error if the obligation is missing or contains invalid state.
    pub fn obligation_state(&self, ask: MessageId) -> Result<ObligationState, StoreError> {
        let value: String = self.connection.query_row(
            "SELECT state FROM obligations WHERE ask_message_id = ?1",
            [ask.to_string()],
            |row| row.get(0),
        )?;
        parse_obligation_state(&value)
    }

    /// Return unresolved reply obligations owed by one logical agent.
    ///
    /// This durable query is independent of current Herdr liveness.
    ///
    /// # Errors
    ///
    /// Returns an error when the logical agent is absent or a record is malformed.
    pub fn pending_obligations(
        &self,
        owing_agent_id: LogicalAgentId,
    ) -> Result<Vec<PendingObligation>, StoreError> {
        let exists: bool = self.connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM logical_agents WHERE id = ?1)",
            [owing_agent_id.to_string()],
            |row| row.get(0),
        )?;
        if !exists {
            return Err(StoreError::Conflict("logical agent is absent".into()));
        }
        let mut statement = self.connection.prepare(
            "SELECT ask_message_id, waiting_agent_id, state FROM obligations
             WHERE owing_agent_id = ?1 AND state IN ('open', 'in_progress')
             ORDER BY creation_sequence",
        )?;
        let rows = statement.query_map([owing_agent_id.to_string()], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?;
        let mut pending = Vec::new();
        for row in rows {
            let (ask, waiting, state) = row?;
            pending.push(PendingObligation {
                ask_message_id: MessageId::parse(&ask).ok_or_else(|| {
                    StoreError::InvalidRecord(format!("invalid ask message id {ask}"))
                })?,
                waiting_agent_id: LogicalAgentId::parse(&waiting).ok_or_else(|| {
                    StoreError::InvalidRecord(format!("invalid waiting agent id {waiting}"))
                })?,
                state: parse_obligation_state(&state)?,
            });
        }
        Ok(pending)
    }

    /// Cancel one exact unresolved obligation under a same-user requester claim.
    ///
    /// The requester is attribution, not authentication. Authenticated or
    /// capability-bearing transports must validate it before invoking Kelpie.
    ///
    /// # Errors
    ///
    /// Returns a conflict without mutation for an empty reason, absent
    /// obligation, ownership mismatch, or terminal obligation state.
    pub fn cancel_obligation(
        &mut self,
        requester_agent_id: LogicalAgentId,
        ask_message_id: MessageId,
        reason: &str,
    ) -> Result<(), StoreError> {
        if reason.trim().is_empty() {
            return Err(StoreError::Conflict(
                "cancellation reason must not be empty".into(),
            ));
        }
        let now = now_millis()?;
        let tx = self.connection.transaction()?;
        let obligation: Option<(String, String)> = tx
            .query_row(
                "SELECT waiting_agent_id, state FROM obligations WHERE ask_message_id = ?1",
                [ask_message_id.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        let Some((waiting_agent_id, state)) = obligation else {
            return Err(StoreError::Conflict("obligation is absent".into()));
        };
        if waiting_agent_id != requester_agent_id.to_string() {
            return Err(StoreError::Conflict(
                "requester does not own the obligation".into(),
            ));
        }
        if !matches!(state.as_str(), "open" | "in_progress") {
            return Err(StoreError::Conflict(format!(
                "obligation in {state} state is not cancellable"
            )));
        }
        let changed = tx.execute(
            "UPDATE obligations SET state = 'cancelled', last_activity_at_ms = ?1,
             cancellation_requester_agent_id = ?2, cancellation_reason = ?3
             WHERE ask_message_id = ?4 AND waiting_agent_id = ?2
             AND state IN ('open', 'in_progress')",
            params![
                now,
                requester_agent_id.to_string(),
                reason,
                ask_message_id.to_string()
            ],
        )?;
        if changed != 1 {
            return Err(StoreError::Conflict(
                "obligation changed before cancellation committed".into(),
            ));
        }
        tx.commit()?;
        Ok(())
    }

    /// The asker's Ready incarnation for a cancellation response, if any.
    ///
    /// Resolved before durable intent so the delivery can honour the same
    /// clear/settle scheduling as every other prompt.
    ///
    /// # Errors
    ///
    /// Returns a conflict when the obligation is absent or the Ready match is
    /// ambiguous.
    pub fn cancel_recipient_incarnation(
        &self,
        ask_message_id: MessageId,
    ) -> Result<Option<IncarnationId>, StoreError> {
        let waiting: Option<String> = self
            .connection
            .query_row(
                "SELECT waiting_agent_id FROM obligations WHERE ask_message_id = ?1",
                [ask_message_id.to_string()],
                |row| row.get(0),
            )
            .optional()?;
        let Some(waiting) = waiting else {
            return Err(StoreError::Conflict("obligation is absent".into()));
        };
        let waiting_agent = LogicalAgentId::parse(&waiting).ok_or_else(|| {
            StoreError::InvalidRecord(format!("invalid waiting agent id {waiting}"))
        })?;
        let mut statement = self.connection.prepare(
            "SELECT id FROM incarnations
             WHERE logical_agent_id = ?1 AND state = 'ready'
             ORDER BY created_at_ms ASC",
        )?;
        let rows =
            statement.query_map([waiting_agent.to_string()], |row| row.get::<_, String>(0))?;
        let mut ready = Vec::new();
        for row in rows {
            ready.push(row?);
        }
        match ready.as_slice() {
            [incarnation] => IncarnationId::parse(incarnation)
                .ok_or_else(|| {
                    StoreError::InvalidRecord(format!("invalid incarnation id {incarnation}"))
                })
                .map(Some),
            [] => Ok(None),
            [_, _, ..] => Err(StoreError::Conflict(
                "ambiguous ready incarnation for waiting agent".into(),
            )),
        }
    }

    /// The owing agent's Ready incarnation for a cancellation stop-notice, if any.
    ///
    /// # Errors
    ///
    /// Returns a conflict when the obligation is absent or the Ready match is
    /// ambiguous.
    pub fn cancel_owing_incarnation(
        &self,
        ask_message_id: MessageId,
    ) -> Result<Option<IncarnationId>, StoreError> {
        let owing: Option<String> = self
            .connection
            .query_row(
                "SELECT owing_agent_id FROM obligations WHERE ask_message_id = ?1",
                [ask_message_id.to_string()],
                |row| row.get(0),
            )
            .optional()?;
        let Some(owing) = owing else {
            return Err(StoreError::Conflict("obligation is absent".into()));
        };
        let owing_agent = LogicalAgentId::parse(&owing)
            .ok_or_else(|| StoreError::InvalidRecord(format!("invalid owing agent id {owing}")))?;
        let mut statement = self.connection.prepare(
            "SELECT id FROM incarnations
             WHERE logical_agent_id = ?1 AND state = 'ready'
             ORDER BY created_at_ms ASC",
        )?;
        let rows = statement.query_map([owing_agent.to_string()], |row| row.get::<_, String>(0))?;
        let mut ready = Vec::new();
        for row in rows {
            ready.push(row?);
        }
        match ready.as_slice() {
            [incarnation] => IncarnationId::parse(incarnation)
                .ok_or_else(|| {
                    StoreError::InvalidRecord(format!("invalid incarnation id {incarnation}"))
                })
                .map(Some),
            [] => Ok(None),
            [_, _, ..] => Err(StoreError::Conflict(
                "ambiguous ready incarnation for owing agent".into(),
            )),
        }
    }

    /// Cancel one obligation owned by the requester and compose Kelpie's
    /// cancellation notices.
    ///
    /// The obligation settles `cancelled` in the same transaction that records
    /// the durable delivery intent, before any Herdr write. A `herdr_prompt`
    /// asker with a Ready incarnation is prepared for prompt delivery; a
    /// `socket_inbox` asker is queued on that inbox. The owing agent gets the
    /// same treatment for its stop-notice. Without a live receive path a notice
    /// stays recorded on its message row for revival surfacing. Notices are
    /// authored by Kelpie — `cancellation` messages with no sender — and are
    /// never attributed to the asker or the responder.
    ///
    /// # Errors
    ///
    /// Returns a conflict without mutation for an empty reason, absent
    /// obligation, ownership mismatch, or terminal obligation state.
    #[allow(clippy::too_many_lines, clippy::too_many_arguments)]
    pub fn cancel_with_response(
        &mut self,
        requester_agent_id: LogicalAgentId,
        ask_message_id: MessageId,
        reason: &str,
        body: &str,
        owing_body: &str,
        due_at_ms: Option<i64>,
        owing_due_at_ms: Option<i64>,
    ) -> Result<CreatedCancellation, StoreError> {
        if reason.trim().is_empty() {
            return Err(StoreError::Conflict(
                "cancellation reason must not be empty".into(),
            ));
        }
        let now = now_millis()?;
        let tx = self.connection.transaction()?;
        let obligation: Option<(String, String, String)> = tx
            .query_row(
                "SELECT waiting_agent_id, owing_agent_id, state
                 FROM obligations WHERE ask_message_id = ?1",
                [ask_message_id.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()?;
        let Some((waiting, owing, state)) = obligation else {
            return Err(StoreError::Conflict("obligation is absent".into()));
        };
        if waiting != requester_agent_id.to_string() {
            return Err(StoreError::Conflict(
                "requester does not own the obligation".into(),
            ));
        }
        if !matches!(state.as_str(), "open" | "in_progress") {
            return Err(StoreError::Conflict(format!(
                "obligation in {state} state is not cancellable"
            )));
        }
        let waiting_agent = parse_logical_agent_id(&waiting)?;
        let owing_agent = parse_logical_agent_id(&owing)?;
        let (message_id, delivery) = record_cancellation_side(
            &tx,
            waiting_agent,
            ask_message_id,
            reason,
            body,
            CancellationAudience::Waiting,
            due_at_ms,
            now,
            "ambiguous ready incarnation for waiting agent",
        )?;
        let (owing_message_id, owing_delivery) = record_cancellation_side(
            &tx,
            owing_agent,
            ask_message_id,
            reason,
            owing_body,
            CancellationAudience::Owing,
            owing_due_at_ms,
            now,
            "ambiguous ready incarnation for owing agent",
        )?;
        tx.execute(
            "UPDATE obligations SET cancellation_response_message_id = ?1,
             cancellation_owing_message_id = ?2
             WHERE ask_message_id = ?3",
            params![
                message_id.to_string(),
                owing_message_id.to_string(),
                ask_message_id.to_string()
            ],
        )?;
        let changed = tx.execute(
            "UPDATE obligations SET state = 'cancelled', last_activity_at_ms = ?1,
             cancellation_requester_agent_id = ?2, cancellation_reason = ?3
             WHERE ask_message_id = ?4 AND waiting_agent_id = ?2
             AND state IN ('open', 'in_progress')",
            params![
                now,
                requester_agent_id.to_string(),
                reason,
                ask_message_id.to_string()
            ],
        )?;
        if changed != 1 {
            return Err(StoreError::Conflict(
                "obligation changed before cancellation committed".into(),
            ));
        }
        tx.commit()?;
        Ok(CreatedCancellation {
            message_id,
            delivery,
            owing_message_id,
            owing_delivery,
        })
    }

    /// The original ask and reason a cancellation operation was composed for.
    ///
    /// The deferred fire path renders the response envelope long after the
    /// cancel request is gone, and it must render exactly what the immediate
    /// path would have: the same ask id and the same reason. Both live in the
    /// operation intent.
    ///
    /// # Errors
    ///
    /// Returns a conflict when the operation is absent or its intent is missing
    /// either field, and an error for malformed IDs or JSON.
    pub fn cancellation_rendering_for_operation(
        &self,
        operation_id: OperationId,
    ) -> Result<(MessageId, String, CancellationAudience), StoreError> {
        let intent: String = self.connection.query_row(
            "SELECT intent_json FROM operations WHERE id = ?1",
            [operation_id.to_string()],
            |row| row.get(0),
        )?;
        let value: serde_json::Value =
            serde_json::from_str(&intent).map_err(|error| invalid_json(&error))?;
        let ask = value
            .pointer("/cancelled_ask")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| {
                StoreError::InvalidRecord(
                    "cancellation operation intent is missing cancelled_ask".into(),
                )
            })?;
        let reason = value
            .pointer("/reason")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| {
                StoreError::InvalidRecord("cancellation operation intent is missing reason".into())
            })?;
        let audience = match value
            .pointer("/audience")
            .and_then(serde_json::Value::as_str)
        {
            Some("owing") => CancellationAudience::Owing,
            Some("waiting") | None => CancellationAudience::Waiting,
            Some(other) => {
                return Err(StoreError::InvalidRecord(format!(
                    "cancellation operation intent has unknown audience {other}"
                )));
            }
        };
        let ask_id = MessageId::parse(ask)
            .ok_or_else(|| StoreError::InvalidRecord(format!("invalid ask message id {ask}")))?;
        Ok((ask_id, reason.to_string(), audience))
    }

    /// Re-read one ask's durable content and parties by its message id — the
    /// amnesia-recovery read behind a reminder's reply-to id. Read-only.
    ///
    /// # Errors
    ///
    /// Returns a conflict when the id does not name an ask obligation.
    pub fn ask_info(&self, ask_message_id: MessageId) -> Result<AskInfo, StoreError> {
        type AskInfoRow = (
            String,
            String,
            String,
            String,
            String,
            String,
            i64,
            i64,
            Option<String>,
        );
        let row: Option<AskInfoRow> = self
            .connection
            .query_row(
                "SELECT m.body, o.waiting_agent_id, wa.public_name, o.owing_agent_id,
                            ra.public_name, o.state, o.created_at_ms, o.last_activity_at_ms,
                            o.cancellation_reason
                     FROM obligations o
                     JOIN messages m ON m.id = o.ask_message_id
                     JOIN logical_agents wa ON wa.id = o.waiting_agent_id
                     JOIN logical_agents ra ON ra.id = o.owing_agent_id
                     WHERE o.ask_message_id = ?1",
                [ask_message_id.to_string()],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                        row.get(6)?,
                        row.get(7)?,
                        row.get(8)?,
                    ))
                },
            )
            .optional()?;
        let Some((
            body,
            asker_agent_id,
            asker_name,
            responder_agent_id,
            responder_name,
            state,
            created,
            last_activity,
            reason,
        )) = row
        else {
            return Err(StoreError::Conflict(
                "ask message does not name an obligation".into(),
            ));
        };
        Ok(AskInfo {
            ask_message_id: ask_message_id.to_string(),
            body,
            asker_agent_id,
            asker_name,
            responder_agent_id,
            responder_name,
            state,
            created_at_ms: created,
            last_activity_at_ms: last_activity,
            cancellation_reason: reason,
        })
    }

    /// Cancellations of this agent's waits that no pane has received, from the
    /// Cancellations of this agent's waits that no pane has received, from the
    /// lifetime of the binding before the current one. Read-only.
    ///
    /// The window is (creation of the second-newest incarnation, creation of
    /// the newest Ready incarnation]: everything cancelled after the agent's
    /// previous binding came into existence and up to and including the moment
    /// the current one did. Responses already delivered to a pane are excluded
    /// — the asker has them. An agent with no incarnation at all sees every
    /// undelivered cancellation, capped at the twenty most recent.
    ///
    /// # Errors
    ///
    /// Returns a conflict when the logical agent is absent.
    pub fn cancelled_while_away(
        &self,
        waiting_agent_id: LogicalAgentId,
    ) -> Result<Vec<CancelledWhileAway>, StoreError> {
        let exists: bool = self.connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM logical_agents WHERE id = ?1)",
            [waiting_agent_id.to_string()],
            |row| row.get(0),
        )?;
        if !exists {
            return Err(StoreError::Conflict("logical agent is absent".into()));
        }
        let mut statement = self.connection.prepare(
            "SELECT o.ask_message_id, o.cancellation_reason,
                    o.cancellation_requester_agent_id, o.last_activity_at_ms
             FROM obligations o
             WHERE o.waiting_agent_id = ?1 AND o.state = 'cancelled'
               AND NOT EXISTS (SELECT 1 FROM deliveries d
                               WHERE d.message_id = o.cancellation_response_message_id
                                 AND d.outcome = 'accepted')
               AND o.last_activity_at_ms > COALESCE(
                   (SELECT MAX(i2.created_at_ms) FROM incarnations i2
                    WHERE i2.logical_agent_id = ?1
                      AND i2.created_at_ms < (SELECT MAX(i3.created_at_ms)
                                              FROM incarnations i3
                                              WHERE i3.logical_agent_id = ?1)),
                   0)
               AND o.last_activity_at_ms <= COALESCE(
                   (SELECT MAX(i.created_at_ms) FROM incarnations i
                    WHERE i.logical_agent_id = ?1 AND i.state = 'ready'),
                   9223372036854775807)
             ORDER BY o.last_activity_at_ms DESC
             LIMIT 20",
        )?;
        let rows = statement.query_map([waiting_agent_id.to_string()], |row| {
            Ok(CancelledWhileAway {
                ask_message_id: row.get(0)?,
                reason: row.get(1)?,
                cancelled_by: row.get(2)?,
                cancelled_at_ms: row.get(3)?,
            })
        })?;
        let mut cancelled = Vec::new();
        for row in rows {
            cancelled.push(row?);
        }
        Ok(cancelled)
    }

    /// Cancellations of asks this agent owed that no pane has received, from
    /// the lifetime of the binding before the current one. Read-only.
    ///
    /// Same window as [`Self::cancelled_while_away`], keyed on the owing agent
    /// and its stop-notice rather than the asker's response.
    ///
    /// # Errors
    ///
    /// Returns a conflict when the logical agent is absent.
    pub fn cancelled_owing_while_away(
        &self,
        owing_agent_id: LogicalAgentId,
    ) -> Result<Vec<CancelledWhileAway>, StoreError> {
        let exists: bool = self.connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM logical_agents WHERE id = ?1)",
            [owing_agent_id.to_string()],
            |row| row.get(0),
        )?;
        if !exists {
            return Err(StoreError::Conflict("logical agent is absent".into()));
        }
        let mut statement = self.connection.prepare(
            "SELECT o.ask_message_id, o.cancellation_reason,
                    o.cancellation_requester_agent_id, o.last_activity_at_ms
             FROM obligations o
             WHERE o.owing_agent_id = ?1 AND o.state = 'cancelled'
               AND o.cancellation_owing_message_id IS NOT NULL
               AND NOT EXISTS (SELECT 1 FROM deliveries d
                               WHERE d.message_id = o.cancellation_owing_message_id
                                 AND d.outcome = 'accepted')
               AND o.last_activity_at_ms > COALESCE(
                   (SELECT MAX(i2.created_at_ms) FROM incarnations i2
                    WHERE i2.logical_agent_id = ?1
                      AND i2.created_at_ms < (SELECT MAX(i3.created_at_ms)
                                              FROM incarnations i3
                                              WHERE i3.logical_agent_id = ?1)),
                   0)
               AND o.last_activity_at_ms <= COALESCE(
                   (SELECT MAX(i.created_at_ms) FROM incarnations i
                    WHERE i.logical_agent_id = ?1 AND i.state = 'ready'),
                   9223372036854775807)
             ORDER BY o.last_activity_at_ms DESC
             LIMIT 20",
        )?;
        let rows = statement.query_map([owing_agent_id.to_string()], |row| {
            Ok(CancelledWhileAway {
                ask_message_id: row.get(0)?,
                reason: row.get(1)?,
                cancelled_by: row.get(2)?,
                cancelled_at_ms: row.get(3)?,
            })
        })?;
        let mut cancelled = Vec::new();
        for row in rows {
            cancelled.push(row?);
        }
        Ok(cancelled)
    }

    /// Cancel a queued delivery only before the first Herdr write.
    ///
    /// After `submitted`, existing no-resend and unknown rules apply: a tell
    /// cannot be unsent, and an ask falls through to obligation cancel.
    ///
    /// # Errors
    ///
    /// Returns a conflict for an empty reason, absent message, ownership
    /// mismatch, a submitted attempt, or a delivery that is not queued.
    pub fn cancel_queued_delivery(
        &mut self,
        requester_agent_id: LogicalAgentId,
        message_id: MessageId,
        reason: &str,
    ) -> Result<bool, StoreError> {
        if reason.trim().is_empty() {
            return Err(StoreError::Conflict(
                "cancellation reason must not be empty".into(),
            ));
        }
        let now = now_millis()?;
        let tx = self.connection.transaction()?;
        let row: Option<(String, Option<String>, String, String)> = tx
            .query_row(
                "SELECT m.kind, m.sender_agent_id, d.outcome, d.operation_id
                 FROM messages m
                 JOIN deliveries d ON d.message_id = m.id
                 WHERE m.id = ?1",
                [message_id.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .optional()?;
        let Some((kind, sender, outcome, operation_id)) = row else {
            return Ok(false);
        };
        if outcome != "queued" {
            return Ok(false);
        }
        if kind == "tell" && sender.as_deref() != Some(&requester_agent_id.to_string()) {
            return Err(StoreError::Conflict(
                "requester does not own the scheduled tell".into(),
            ));
        }
        if kind == "ask" {
            let waiting: Option<String> = tx
                .query_row(
                    "SELECT waiting_agent_id FROM obligations WHERE ask_message_id = ?1",
                    [message_id.to_string()],
                    |row| row.get(0),
                )
                .optional()?;
            if waiting.as_deref() != Some(&requester_agent_id.to_string()) {
                return Err(StoreError::Conflict(
                    "requester does not own the obligation".into(),
                ));
            }
        }
        let submitted: i64 = tx.query_row(
            "SELECT COUNT(*) FROM operation_attempts
             WHERE operation_id = ?1 AND phase != 'prepared'",
            [&operation_id],
            |row| row.get(0),
        )?;
        if submitted > 0 {
            return Err(StoreError::Conflict(
                "scheduled delivery already submitted to Herdr is not cancellable".into(),
            ));
        }
        let changed = tx.execute(
            "UPDATE deliveries SET outcome = 'superseded', resolved_at_ms = ?1,
             cancelled_at_ms = ?1, cancellation_requester_agent_id = ?2,
             cancellation_reason = ?3
             WHERE operation_id = ?4 AND outcome = 'queued'",
            params![now, requester_agent_id.to_string(), reason, operation_id],
        )?;
        if changed != 1 {
            return Err(StoreError::Conflict(
                "queued delivery changed before cancellation committed".into(),
            ));
        }
        let operation_changed = tx.execute(
            "UPDATE operations SET outcome = 'superseded', resolved_at_ms = ?1
             WHERE id = ?2 AND outcome = 'pending'",
            params![now, operation_id],
        )?;
        if operation_changed != 1 {
            return Err(StoreError::Conflict(
                "queued operation changed before cancellation committed".into(),
            ));
        }
        if kind == "ask" {
            let obligation_changed = tx.execute(
                "UPDATE obligations SET state = 'cancelled', last_activity_at_ms = ?1,
                 cancellation_requester_agent_id = ?2, cancellation_reason = ?3
                 WHERE ask_message_id = ?4 AND waiting_agent_id = ?2
                 AND state IN ('open', 'in_progress')",
                params![
                    now,
                    requester_agent_id.to_string(),
                    reason,
                    message_id.to_string()
                ],
            )?;
            if obligation_changed != 1 {
                return Err(StoreError::Conflict(
                    "obligation changed before scheduled cancellation committed".into(),
                ));
            }
        }
        tx.commit()?;
        Ok(true)
    }

    /// Retire queued ask deliveries whose obligation is already settled.
    ///
    /// An ask is durable when created, not when delivered, so a recipient can
    /// answer one whose scheduled delivery has not fired yet. Firing it then
    /// would present settled work as a fresh demand — the recipient has no way
    /// to tell a late envelope from a new request — so the delivery is
    /// superseded instead. Cancellation is the waiter's to request; this is not
    /// cancellation, it is Kelpie refusing to contradict its own record.
    ///
    /// # Errors
    ///
    /// Returns an error if the update cannot be committed.
    pub fn supersede_settled_queued_asks(&mut self, now_ms: i64) -> Result<usize, StoreError> {
        let changed = self.connection.execute(
            "UPDATE deliveries SET outcome = 'superseded', resolved_at_ms = ?1,
             cancelled_at_ms = ?1,
             cancellation_reason = 'obligation settled before the scheduled delivery fired'
             WHERE outcome = 'queued' AND message_id IN (
                SELECT ask_message_id FROM obligations
                 WHERE state NOT IN ('open', 'in_progress')
             )",
            params![now_ms],
        )?;
        Ok(changed)
    }

    /// Queued tell/ask deliveries that are due on the store clock.
    ///
    /// # Errors
    ///
    /// Returns an error if durable IDs or kinds are malformed.
    pub fn due_deliveries(&self, now_ms: i64) -> Result<Vec<DueDelivery>, StoreError> {
        let mut statement = self.connection.prepare(
            "SELECT d.operation_id, m.id, m.kind,
                    CASE
                      WHEN m.kind = 'ask' THEN COALESCE(m.sender_agent_id, o.waiting_agent_id)
                      ELSE m.sender_agent_id
                    END,
                    m.recipient_agent_id,
                    d.recipient_incarnation_id, m.body, d.scheduled_at_ms
              FROM deliveries d
              JOIN messages m ON m.id = d.message_id
              LEFT JOIN obligations o ON o.ask_message_id = m.id
              WHERE d.outcome = 'queued' AND d.scheduled_at_ms <= ?1
                AND d.delivery_transport = 'herdr_prompt'
               AND NOT EXISTS (SELECT 1 FROM renews r
                               WHERE r.incarnation_id = d.recipient_incarnation_id
                                  AND r.phase IN ('ready','clearing'))
               AND NOT EXISTS (SELECT 1 FROM operations clear
                               WHERE clear.kind = 'clear'
                                 AND clear.target_incarnation_id = d.recipient_incarnation_id
                                 AND clear.outcome IN ('pending','accepted'))
             ORDER BY d.scheduled_at_ms, m.created_at_ms",
        )?;
        let rows = statement.query_map([now_ms], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, String>(6)?,
                row.get::<_, i64>(7)?,
            ))
        })?;
        let mut due = Vec::new();
        for row in rows {
            let (operation, message, kind, sender, recipient, incarnation, body, scheduled_at_ms) =
                row?;
            due.push(DueDelivery {
                operation_id: OperationId::parse(&operation).ok_or_else(|| {
                    StoreError::InvalidRecord(format!("invalid operation id {operation}"))
                })?,
                message_id: MessageId::parse(&message).ok_or_else(|| {
                    StoreError::InvalidRecord(format!("invalid message id {message}"))
                })?,
                kind: parse_message_kind(&kind)?,
                sender: sender
                    .map(|sender| {
                        LogicalAgentId::parse(&sender).ok_or_else(|| {
                            StoreError::InvalidRecord(format!("invalid sender id {sender}"))
                        })
                    })
                    .transpose()?,
                recipient: LogicalAgentId::parse(&recipient).ok_or_else(|| {
                    StoreError::InvalidRecord(format!("invalid recipient id {recipient}"))
                })?,
                recipient_incarnation: IncarnationId::parse(&incarnation).ok_or_else(|| {
                    StoreError::InvalidRecord(format!("invalid incarnation id {incarnation}"))
                })?,
                body,
                scheduled_at_ms,
            });
        }
        Ok(due)
    }

    /// Return overdue reminders bound to one exact Ready owing incarnation.
    ///
    /// # Errors
    ///
    /// Returns an error if durable IDs are malformed.
    pub fn due_reminders(&self, now_ms: i64) -> Result<Vec<DueReminder>, StoreError> {
        let mut statement = self.connection.prepare(
            "SELECT r.ask_message_id, o.owing_agent_id, o.waiting_agent_id,
                    i.id, i.observed_pane_id, i.observed_terminal_id, r.interval_ms,
                    m.body
             FROM obligation_reminders r
             JOIN obligations o ON o.ask_message_id = r.ask_message_id
             JOIN messages m ON m.id = r.ask_message_id
             JOIN incarnations i ON i.logical_agent_id = o.owing_agent_id
             WHERE o.state IN ('open','in_progress')
               AND r.disabled_at_ms IS NULL AND r.suspended_at_ms IS NULL
               AND r.next_due_at_ms IS NOT NULL
               AND MAX(r.next_due_at_ms, COALESCE(r.snoozed_until_ms, 0)) <= ?1
               AND i.state = 'ready'
               AND NOT EXISTS (SELECT 1 FROM operations clear
                               WHERE clear.kind = 'clear'
                                 AND clear.target_incarnation_id = i.id
                                 AND clear.outcome IN ('pending','accepted'))
               AND (SELECT COUNT(*) FROM incarnations current
                    WHERE current.logical_agent_id = o.owing_agent_id
                      AND current.state = 'ready') = 1
             ORDER BY r.next_due_at_ms, o.creation_sequence",
        )?;
        let rows = statement.query_map([now_ms], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, i64>(6)?,
                row.get::<_, String>(7)?,
            ))
        })?;
        let mut due = Vec::new();
        for row in rows {
            let (ask, owing, waiting, incarnation, pane, terminal, interval_ms, body) = row?;
            due.push(DueReminder {
                ask_message_id: parse_message_id(&ask)?,
                owing_agent_id: parse_logical_agent_id(&owing)?,
                waiting_agent_id: parse_logical_agent_id(&waiting)?,
                recipient_incarnation: parse_incarnation_id(&incarnation)?,
                pane_id: pane,
                terminal_id: terminal,
                interval_ms,
                body,
            });
        }
        Ok(due)
    }

    /// Return never-answered asks that need a lifecycle-boundary observation.
    ///
    /// # Errors
    ///
    /// Returns an error if durable IDs are malformed.
    pub fn boundary_reminders(&self, now_ms: i64) -> Result<Vec<BoundaryReminder>, StoreError> {
        let mut statement = self.connection.prepare(
            "SELECT r.ask_message_id, o.owing_agent_id, o.waiting_agent_id,
                    i.id, i.observed_pane_id, i.observed_terminal_id, r.interval_ms,
                    r.saw_working_at_ms IS NOT NULL, m.body
             FROM obligation_reminders r
             JOIN obligations o ON o.ask_message_id = r.ask_message_id
             JOIN messages m ON m.id = r.ask_message_id
             JOIN incarnations i ON i.logical_agent_id = o.owing_agent_id
             WHERE o.state = 'open'
               AND r.disabled_at_ms IS NULL AND r.suspended_at_ms IS NULL
               AND r.last_accepted_at_ms IS NULL
               AND r.boundary_check_at_ms IS NOT NULL AND r.boundary_check_at_ms <= ?1
               AND COALESCE(r.snoozed_until_ms, 0) <= ?1
               AND i.state = 'ready'
               AND NOT EXISTS (SELECT 1 FROM operations clear
                               WHERE clear.kind = 'clear'
                                 AND clear.target_incarnation_id = i.id
                                 AND clear.outcome IN ('pending','accepted'))
               AND (SELECT COUNT(*) FROM incarnations current
                    WHERE current.logical_agent_id = o.owing_agent_id
                      AND current.state = 'ready') = 1
             ORDER BY r.boundary_check_at_ms, o.creation_sequence",
        )?;
        let rows = statement.query_map([now_ms], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, i64>(6)?,
                row.get::<_, bool>(7)?,
                row.get::<_, String>(8)?,
            ))
        })?;
        let mut reminders = Vec::new();
        for row in rows {
            let (ask, owing, waiting, incarnation, pane, terminal, interval_ms, saw_working, body) =
                row?;
            reminders.push(BoundaryReminder {
                reminder: DueReminder {
                    ask_message_id: parse_message_id(&ask)?,
                    owing_agent_id: parse_logical_agent_id(&owing)?,
                    waiting_agent_id: parse_logical_agent_id(&waiting)?,
                    recipient_incarnation: parse_incarnation_id(&incarnation)?,
                    pane_id: pane,
                    terminal_id: terminal,
                    interval_ms,
                    body,
                },
                saw_working,
            });
        }
        Ok(reminders)
    }

    /// Record one fresh lifecycle observation for an unanswered ask.
    ///
    /// # Errors
    ///
    /// Returns a conflict unless the reminder remains boundary-eligible.
    pub fn observe_reminder_lifecycle(
        &mut self,
        ask: MessageId,
        saw_working: bool,
        next_check_at_ms: i64,
    ) -> Result<(), StoreError> {
        let changed = self.connection.execute(
            "UPDATE obligation_reminders
             SET boundary_check_at_ms = ?1,
                 saw_working_at_ms = CASE WHEN ?2 THEN COALESCE(saw_working_at_ms, ?1)
                                          ELSE saw_working_at_ms END
             WHERE ask_message_id = ?3 AND disabled_at_ms IS NULL
               AND suspended_at_ms IS NULL AND last_accepted_at_ms IS NULL
               AND EXISTS (SELECT 1 FROM obligations o WHERE o.ask_message_id = ?3
                           AND o.state = 'open')",
            params![next_check_at_ms, saw_working, ask.to_string()],
        )?;
        if changed != 1 {
            return Err(StoreError::Conflict(
                "reminder is no longer eligible for lifecycle observation".into(),
            ));
        }
        Ok(())
    }

    /// Journal a reminder before its Herdr write boundary.
    ///
    /// # Errors
    ///
    /// Returns a conflict unless the exact obligation and incarnation remain eligible.
    pub fn prepare_reminder_attempt(
        &mut self,
        reminder: &DueReminder,
        request_id: &str,
        now_ms: i64,
    ) -> Result<(), StoreError> {
        let tx = self.connection.transaction()?;
        let eligible: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM obligation_reminders r
             JOIN obligations o ON o.ask_message_id = r.ask_message_id
             JOIN incarnations i ON i.logical_agent_id = o.owing_agent_id
             WHERE r.ask_message_id = ?1 AND o.state IN ('open','in_progress')
               AND r.disabled_at_ms IS NULL AND r.suspended_at_ms IS NULL
               AND (MAX(r.next_due_at_ms, COALESCE(r.snoozed_until_ms, 0)) <= ?2
                    OR (o.state = 'open' AND r.last_accepted_at_ms IS NULL
                        AND r.saw_working_at_ms IS NOT NULL
                        AND r.boundary_check_at_ms <= ?2
                        AND COALESCE(r.snoozed_until_ms, 0) <= ?2))
               AND i.id = ?3 AND i.state = 'ready')",
            params![
                reminder.ask_message_id.to_string(),
                now_ms,
                reminder.recipient_incarnation.to_string()
            ],
            |row| row.get(0),
        )?;
        if !eligible {
            return Err(StoreError::Conflict(
                "reminder is no longer due for the exact Ready incarnation".into(),
            ));
        }
        tx.execute(
            "INSERT INTO reminder_attempts
             (ask_message_id, recipient_incarnation_id, request_id, started_at_ms, phase)
             VALUES (?1, ?2, ?3, ?4, 'prepared')",
            params![
                reminder.ask_message_id.to_string(),
                reminder.recipient_incarnation.to_string(),
                request_id,
                now_ms
            ],
        )?;
        tx.commit()?;
        Ok(())
    }

    /// Mark a prepared reminder as submitted immediately before the Herdr write.
    ///
    /// # Errors
    ///
    /// Returns a conflict unless the exact attempt is prepared.
    pub fn submit_reminder_attempt(&mut self, request_id: &str) -> Result<(), StoreError> {
        let changed = self.connection.execute(
            "UPDATE reminder_attempts SET phase = 'submitted'
             WHERE request_id = ?1 AND phase = 'prepared'",
            [request_id],
        )?;
        if changed != 1 {
            return Err(StoreError::Conflict(
                "reminder attempt is not prepared".into(),
            ));
        }
        Ok(())
    }

    /// Resolve a reminder and advance its cooldown or suspend unknown retries.
    ///
    /// # Errors
    ///
    /// Returns a conflict unless the exact attempt is submitted.
    pub fn resolve_reminder_attempt(
        &mut self,
        request_id: &str,
        outcome: &str,
        evidence: Option<&str>,
        now_ms: i64,
    ) -> Result<(), StoreError> {
        if !matches!(outcome, "accepted" | "rejected" | "unknown") {
            return Err(StoreError::InvalidRecord(format!(
                "invalid reminder outcome {outcome}"
            )));
        }
        let tx = self.connection.transaction()?;
        let ask: Option<String> = tx
            .query_row(
                "SELECT ask_message_id FROM reminder_attempts
             WHERE request_id = ?1 AND phase = 'submitted'",
                [request_id],
                |row| row.get(0),
            )
            .optional()?;
        let Some(ask) = ask else {
            return Err(StoreError::Conflict(
                "reminder attempt is not submitted".into(),
            ));
        };
        tx.execute(
            "UPDATE reminder_attempts SET phase = ?1, resolved_at_ms = ?2, evidence_json = ?3
             WHERE request_id = ?4 AND phase = 'submitted'",
            params![
                outcome,
                now_ms,
                evidence.map(|detail| serde_json::json!({"detail": detail}).to_string()),
                request_id
            ],
        )?;
        if outcome == "unknown" {
            tx.execute(
                "UPDATE obligation_reminders SET suspended_at_ms = ?1 WHERE ask_message_id = ?2",
                params![now_ms, ask],
            )?;
        } else {
            tx.execute(
                "UPDATE obligation_reminders
                 SET next_due_at_ms = ?1 + interval_ms,
                     last_accepted_at_ms = CASE WHEN ?2 = 'accepted' THEN ?1 ELSE last_accepted_at_ms END,
                     snoozed_until_ms = NULL,
                     boundary_check_at_ms = NULL WHERE ask_message_id = ?3",
                params![now_ms, outcome, ask],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    /// Postpone a due reminder after a fresh snapshot reports a busy lifecycle.
    ///
    /// # Errors
    ///
    /// Returns a conflict unless the exact policy remains active and unresolved.
    pub fn defer_busy_reminder(
        &mut self,
        ask: MessageId,
        now_ms: i64,
        recheck_ms: i64,
    ) -> Result<(), StoreError> {
        let changed = self.connection.execute(
            "UPDATE obligation_reminders SET next_due_at_ms = ?1 + ?2
             WHERE ask_message_id = ?3 AND disabled_at_ms IS NULL AND suspended_at_ms IS NULL
               AND EXISTS (SELECT 1 FROM obligations o WHERE o.ask_message_id = ?3
                           AND o.state IN ('open','in_progress'))",
            params![now_ms, recheck_ms, ask.to_string()],
        )?;
        if changed != 1 {
            return Err(StoreError::Conflict(
                "reminder is no longer active for busy deferral".into(),
            ));
        }
        Ok(())
    }

    /// Suspend reminder attempts left at or beyond the external write boundary.
    ///
    /// Prepared attempts are deleted because no write was possible. Submitted
    /// attempts become unknown and suspend their obligation's automatic retries.
    ///
    /// # Errors
    ///
    /// Returns an error if reconciliation cannot commit atomically.
    pub fn reconcile_reminder_attempts(&mut self) -> Result<usize, StoreError> {
        let now = now_millis()?;
        let tx = self.connection.transaction()?;
        tx.execute("DELETE FROM reminder_attempts WHERE phase = 'prepared'", [])?;
        let submitted: Vec<String> = {
            let mut statement = tx.prepare(
                "SELECT DISTINCT ask_message_id FROM reminder_attempts WHERE phase = 'submitted'",
            )?;
            statement
                .query_map([], |row| row.get(0))?
                .collect::<Result<_, _>>()?
        };
        let changed = tx.execute(
            "UPDATE reminder_attempts SET phase = 'unknown', resolved_at_ms = ?1,
             evidence_json = ?2 WHERE phase = 'submitted'",
            params![
                now,
                serde_json::json!({"detail": "kelpied restarted after reminder submission"})
                    .to_string()
            ],
        )?;
        for ask in submitted {
            tx.execute(
                "UPDATE obligation_reminders SET suspended_at_ms = ?1 WHERE ask_message_id = ?2",
                params![now, ask],
            )?;
        }
        tx.commit()?;
        Ok(changed)
    }

    /// Snooze reminders without resolving the owned obligation.
    ///
    /// # Errors
    ///
    /// Returns a conflict for invalid ownership, terminal state, or past time.
    pub fn snooze_reminder(
        &mut self,
        requester: LogicalAgentId,
        ask: MessageId,
        until_ms: i64,
    ) -> Result<(), StoreError> {
        let now = now_millis()?;
        if until_ms <= now {
            return Err(StoreError::Conflict("snooze must end in the future".into()));
        }
        let changed = self.connection.execute(
            "UPDATE obligation_reminders SET snoozed_until_ms = ?1
             WHERE ask_message_id = ?2 AND disabled_at_ms IS NULL
               AND EXISTS (SELECT 1 FROM obligations o WHERE o.ask_message_id = ?2
                           AND o.owing_agent_id = ?3 AND o.state IN ('open','in_progress'))",
            params![until_ms, ask.to_string(), requester.to_string()],
        )?;
        if changed != 1 {
            return Err(StoreError::Conflict(
                "reminder is absent, disabled, terminal, or not owned by requester".into(),
            ));
        }
        Ok(())
    }

    /// Disable reminders without resolving the owned obligation.
    ///
    /// # Errors
    ///
    /// Returns a conflict for invalid ownership or terminal/absent policy.
    pub fn disable_reminder(
        &mut self,
        requester: LogicalAgentId,
        ask: MessageId,
    ) -> Result<(), StoreError> {
        let changed = self.connection.execute(
            "UPDATE obligation_reminders SET disabled_at_ms = ?1
             WHERE ask_message_id = ?2 AND disabled_at_ms IS NULL
               AND EXISTS (SELECT 1 FROM obligations o WHERE o.ask_message_id = ?2
                           AND o.owing_agent_id = ?3 AND o.state IN ('open','in_progress'))",
            params![now_millis()?, ask.to_string(), requester.to_string()],
        )?;
        if changed != 1 {
            return Err(StoreError::Conflict(
                "reminder is absent, disabled, terminal, or not owned by requester".into(),
            ));
        }
        Ok(())
    }

    /// Persist one renew intent before any Herdr write.
    ///
    /// Both prompts and the timeout disposition become durable here. That
    /// ordering is the whole recoverability argument: once the clear is
    /// submitted, the resume prompt is already stored, so no crash can leave an
    /// incarnation cleared with nothing to re-seed it.
    ///
    /// # Errors
    ///
    /// Returns a conflict unless the incarnation is an exact Ready binding of
    /// that logical agent with no other active renew.
    pub fn create_renew(&mut self, intent: &RenewIntent) -> Result<RenewId, StoreError> {
        let tx = self.connection.transaction()?;
        let ready: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM incarnations
             WHERE id = ?1 AND logical_agent_id = ?2 AND state = 'ready')",
            params![
                intent.incarnation_id.to_string(),
                intent.logical_agent_id.to_string()
            ],
            |row| row.get(0),
        )?;
        if !ready {
            return Err(StoreError::Conflict(
                "renew target is not an exact Ready incarnation of that logical agent".into(),
            ));
        }
        let active: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM renews WHERE incarnation_id = ?1
             AND phase NOT IN ('done','aborted','terminated'))",
            [intent.incarnation_id.to_string()],
            |row| row.get(0),
        )?;
        if active {
            return Err(StoreError::Conflict(
                "incarnation already has an active renew".into(),
            ));
        }
        let active_clear: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM operations WHERE kind = 'clear'
             AND target_incarnation_id = ?1 AND outcome IN ('pending','accepted'))",
            [intent.incarnation_id.to_string()],
            |row| row.get(0),
        )?;
        if active_clear {
            return Err(StoreError::Conflict(
                "incarnation already has an active clear".into(),
            ));
        }
        let renew_id = RenewId::new();
        let now = now_millis()?;
        tx.execute(
            "INSERT INTO renews
             (id, logical_agent_id, incarnation_id, requester_agent_id, prepare_prompt,
              resume_prompt, on_timeout, prepare_timeout_ms, every_ms, cycle,
              scheduled_at_ms, phase, created_at_ms)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 1, ?10, 'scheduled', ?11)",
            params![
                renew_id.to_string(),
                intent.logical_agent_id.to_string(),
                intent.incarnation_id.to_string(),
                intent.requester_agent_id.to_string(),
                intent.prepare_prompt,
                intent.resume_prompt,
                renew_timeout_text(intent.on_timeout),
                intent.prepare_timeout_ms,
                intent.every_ms,
                intent.scheduled_at_ms,
                now
            ],
        )?;
        tx.commit()?;
        Ok(renew_id)
    }

    /// Return renews whose next phase transition is owed.
    ///
    /// Only renews whose incarnation is still an exact Ready binding are
    /// returned; a policy whose incarnation has gone is ended through
    /// [`Self::terminable_renews`] instead of advanced.
    ///
    /// # Errors
    ///
    /// Returns an error if durable IDs or enum text are malformed.
    pub fn actionable_renews(&self, now_ms: i64) -> Result<Vec<DueRenew>, StoreError> {
        let mut statement = self.connection.prepare(
            "SELECT r.id, r.logical_agent_id, r.incarnation_id, r.requester_agent_id,
                    r.prepare_prompt, r.resume_prompt, i.observed_pane_id,
                    i.observed_terminal_id, r.phase, r.on_timeout, r.prepare_timeout_ms,
                    r.every_ms, r.cycle, r.ask_message_id, r.pre_clear_session_json,
                    r.prepare_deadline_ms, r.clear_deadline_ms,
                    r.clear_stall_notified_at_ms, r.inject_not_before_ms,
                    (SELECT o.state FROM obligations o
                     WHERE o.ask_message_id = r.ask_message_id),
                    (SELECT o.last_activity_at_ms FROM obligations o
                     WHERE o.ask_message_id = r.ask_message_id
                       AND o.state = 'resolved')
             FROM renews r
             JOIN incarnations i ON i.id = r.incarnation_id
             WHERE i.state = 'ready'
               AND NOT EXISTS (SELECT 1 FROM operations clear
                               WHERE clear.kind = 'clear'
                                 AND clear.target_incarnation_id = r.incarnation_id
                                 AND clear.outcome IN ('pending','accepted'))
               AND ((r.phase = 'scheduled' AND r.scheduled_at_ms <= ?1)
                    OR r.phase IN ('preparing','ready','clearing','injected','timed_out'))
             ORDER BY r.created_at_ms",
        )?;
        let rows = statement.query_map([now_ms], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, String>(6)?,
                row.get::<_, String>(7)?,
                row.get::<_, String>(8)?,
                row.get::<_, String>(9)?,
                row.get::<_, i64>(10)?,
                row.get::<_, Option<i64>>(11)?,
                row.get::<_, i64>(12)?,
                row.get::<_, Option<String>>(13)?,
                row.get::<_, Option<String>>(14)?,
                row.get::<_, Option<i64>>(15)?,
                row.get::<_, Option<i64>>(16)?,
                row.get::<_, Option<i64>>(17)?,
                row.get::<_, Option<i64>>(18)?,
                row.get::<_, Option<String>>(19)?,
                row.get::<_, Option<i64>>(20)?,
            ))
        })?;
        let mut due = Vec::new();
        for row in rows {
            let row = row?;
            let ask = match row.13 {
                Some(ref value) => Some(parse_message_id(value)?),
                None => None,
            };
            let obligation = match row.19 {
                Some(ref value) => Some(parse_obligation_state(value)?),
                None => None,
            };
            due.push(DueRenew {
                renew_id: parse_renew_id(&row.0)?,
                logical_agent_id: parse_logical_agent_id(&row.1)?,
                incarnation_id: parse_incarnation_id(&row.2)?,
                requester_agent_id: parse_logical_agent_id(&row.3)?,
                prepare_prompt: row.4,
                resume_prompt: row.5,
                pane_id: row.6,
                terminal_id: row.7,
                phase: parse_renew_phase(&row.8)?,
                on_timeout: parse_renew_timeout(&row.9)?,
                prepare_timeout_ms: row.10,
                every_ms: row.11,
                cycle: row.12,
                ask_message_id: ask,
                pre_clear_session_json: row.14,
                prepare_deadline_ms: row.15,
                clear_deadline_ms: row.16,
                clear_stall_notified: row.17.is_some(),
                inject_not_before_ms: row.18,
                prepare_obligation_state: obligation,
                prepare_settled_at_ms: row.20,
            });
        }
        Ok(due)
    }

    /// Return non-terminal renews whose incarnation is no longer Ready.
    ///
    /// This is the only terminating condition for a renew policy. A prepare
    /// timeout is reported and retried; a vanished incarnation ends the rule.
    ///
    /// # Errors
    ///
    /// Returns an error if durable IDs are malformed.
    pub fn terminable_renews(&self) -> Result<Vec<RenewId>, StoreError> {
        let mut statement = self.connection.prepare(
            "SELECT r.id FROM renews r
             WHERE r.phase NOT IN ('done','aborted','terminated')
               AND NOT EXISTS (SELECT 1 FROM incarnations i
                               WHERE i.id = r.incarnation_id AND i.state = 'ready')
             ORDER BY r.created_at_ms",
        )?;
        let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
        let mut ids = Vec::new();
        for row in rows {
            ids.push(parse_renew_id(&row?)?);
        }
        Ok(ids)
    }

    /// Identify a renew well enough to name it after it has ended.
    ///
    /// `terminable_renews` yields ids, and a notice about a policy ending must
    /// name the agent that lost it, the exact incarnation it was bound to, and
    /// whether it was a standing rule or a one-shot. None of that is derivable
    /// from the id.
    ///
    /// # Errors
    ///
    /// Returns an error if durable IDs are malformed.
    pub fn renew_identity(&self, renew_id: RenewId) -> Result<Option<RenewIdentity>, StoreError> {
        self.connection
            .query_row(
                "SELECT logical_agent_id, incarnation_id, cycle, every_ms
                 FROM renews WHERE id = ?1",
                [renew_id.to_string()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, Option<i64>>(3)?,
                    ))
                },
            )
            .optional()?
            .map(|(agent, incarnation, cycle, every_ms)| {
                Ok(RenewIdentity {
                    logical_agent_id: parse_logical_agent_id(&agent)?,
                    incarnation_id: parse_incarnation_id(&incarnation)?,
                    cycle,
                    every_ms,
                })
            })
            .transpose()
    }

    /// Record that a renew's prepare ask was delivered and is now awaited.
    ///
    /// # Errors
    ///
    /// Returns a conflict unless the renew is still scheduled.
    pub fn mark_renew_preparing(
        &mut self,
        renew_id: RenewId,
        ask: MessageId,
        deadline_ms: i64,
    ) -> Result<(), StoreError> {
        let changed = self.connection.execute(
            "UPDATE renews SET phase = 'preparing', ask_message_id = ?1,
                 prepare_deadline_ms = ?2
             WHERE id = ?3 AND phase = 'scheduled'",
            params![ask.to_string(), deadline_ms, renew_id.to_string()],
        )?;
        if changed != 1 {
            return Err(StoreError::Conflict("renew is not scheduled".into()));
        }
        Ok(())
    }

    /// Record that the agent confirmed its checkpoint and may be cleared.
    ///
    /// Reaching `Ready` from `TimedOut` is the `proceed` disposition acting
    /// without the reply it asked for, so the prepare ask is settled here: the
    /// cycle has stopped waiting for it and nothing later will.
    ///
    /// # Errors
    ///
    /// Returns a conflict unless the renew is awaiting a prepare reply.
    pub fn mark_renew_ready(&mut self, renew_id: RenewId) -> Result<(), StoreError> {
        let tx = self.connection.transaction()?;
        let changed = tx.execute(
            "UPDATE renews SET phase = 'ready'
             WHERE id = ?1 AND phase IN ('preparing','timed_out')",
            [renew_id.to_string()],
        )?;
        if changed != 1 {
            return Err(StoreError::Conflict(
                "renew is not awaiting a prepare reply".into(),
            ));
        }
        cancel_unanswered_prepare(
            &tx,
            renew_id,
            "renew proceeded without the prepare reply it asked for",
        )?;
        tx.commit()?;
        Ok(())
    }

    /// Record the pre-clear session reference immediately before the clear.
    ///
    /// The stored reference is the only way to prove the clear landed, so it is
    /// durable before the write rather than after it. The stall deadline is
    /// written in the same statement, because a crash between the two would
    /// leave a clearing renew whose silence nothing is timing.
    ///
    /// # Errors
    ///
    /// Returns a conflict unless the renew is ready to be cleared.
    pub fn mark_renew_clearing(
        &mut self,
        renew_id: RenewId,
        pre_clear_session_json: &str,
        clear_deadline_ms: i64,
        inject_not_before_ms: Option<i64>,
    ) -> Result<(), StoreError> {
        let changed = self.connection.execute(
            "UPDATE renews SET phase = 'clearing', pre_clear_session_json = ?1,
                 clear_deadline_ms = ?2, inject_not_before_ms = ?3
             WHERE id = ?4 AND phase = 'ready'",
            params![
                pre_clear_session_json,
                clear_deadline_ms,
                inject_not_before_ms,
                renew_id.to_string()
            ],
        )?;
        if changed != 1 {
            return Err(StoreError::Conflict("renew is not ready to clear".into()));
        }
        Ok(())
    }

    /// Record that this renew's stalled clear has been reported once.
    ///
    /// Returns whether this call is the one that claimed the notice. The claim
    /// is conditional in SQL so two passes racing the same stall cannot both
    /// report it.
    ///
    /// # Errors
    ///
    /// Returns an error if the update fails.
    pub fn claim_renew_clear_stall_notice(
        &mut self,
        renew_id: RenewId,
        now_ms: i64,
    ) -> Result<bool, StoreError> {
        let changed = self.connection.execute(
            // `injected` counts too: a backend that rotates on its next prompt
            // is only proven after the injection, so its stall is discovered a
            // phase later than one that rotates on the clear.
            "UPDATE renews SET clear_stall_notified_at_ms = ?1
             WHERE id = ?2 AND phase IN ('clearing','injected')
               AND clear_stall_notified_at_ms IS NULL",
            params![now_ms, renew_id.to_string()],
        )?;
        Ok(changed == 1)
    }

    /// Record that the resume prompt was accepted by the cleared incarnation.
    ///
    /// # Errors
    ///
    /// Returns a conflict unless the renew is clearing.
    pub fn mark_renew_injected(&mut self, renew_id: RenewId) -> Result<(), StoreError> {
        let changed = self.connection.execute(
            "UPDATE renews SET phase = 'injected' WHERE id = ?1 AND phase = 'clearing'",
            [renew_id.to_string()],
        )?;
        if changed != 1 {
            return Err(StoreError::Conflict("renew is not clearing".into()));
        }
        Ok(())
    }

    /// Complete a renew and, for a policy, arm its next cycle.
    ///
    /// The successor is a new row so the completed cycle keeps its own evidence
    /// and the one-active-renew index stays satisfied.
    ///
    /// # Errors
    ///
    /// Returns a conflict unless the renew has been injected.
    pub fn complete_renew(&mut self, renew_id: RenewId) -> Result<Option<RenewId>, StoreError> {
        let now = now_millis()?;
        let tx = self.connection.transaction()?;
        let changed = tx.execute(
            "UPDATE renews SET phase = 'done', resolved_at_ms = ?1
             WHERE id = ?2 AND phase = 'injected'",
            params![now, renew_id.to_string()],
        )?;
        if changed != 1 {
            return Err(StoreError::Conflict("renew is not injected".into()));
        }
        let next_id = arm_next_renew_cycle(&tx, renew_id, now)?;
        tx.commit()?;
        Ok(next_id)
    }

    /// Record that the prepare deadline elapsed with no final reply.
    ///
    /// # Errors
    ///
    /// Returns a conflict unless the renew is preparing.
    pub fn mark_renew_timed_out(&mut self, renew_id: RenewId) -> Result<(), StoreError> {
        let changed = self.connection.execute(
            "UPDATE renews SET phase = 'timed_out' WHERE id = ?1 AND phase = 'preparing'",
            [renew_id.to_string()],
        )?;
        if changed != 1 {
            return Err(StoreError::Conflict("renew is not preparing".into()));
        }
        Ok(())
    }

    /// Abandon a renew without clearing, preserving the agent's context.
    ///
    /// For a policy this still arms the next cycle: one cycle the agent did not
    /// confirm is a cycle skipped, not a standing rule silently disarmed. The
    /// operator notice reports the skip; the policy keeps bounding the context.
    ///
    /// The prepare ask dies with the cycle that asked it. Leaving it open would
    /// leave a durable reply obligation, and its reminders, for an answer no
    /// cycle is waiting on any more.
    ///
    /// # Errors
    ///
    /// Returns a conflict unless the renew is still before its clear.
    pub fn abort_renew(
        &mut self,
        renew_id: RenewId,
        reason: &str,
    ) -> Result<Option<RenewId>, StoreError> {
        let now = now_millis()?;
        let tx = self.connection.transaction()?;
        let changed = tx.execute(
            "UPDATE renews SET phase = 'aborted', resolved_at_ms = ?1, termination_reason = ?2
             WHERE id = ?3 AND phase IN ('scheduled','preparing','timed_out','ready')",
            params![now, reason, renew_id.to_string()],
        )?;
        if changed != 1 {
            return Err(StoreError::Conflict(
                "renew is absent, terminal, or already past its clear".into(),
            ));
        }
        cancel_unanswered_prepare(&tx, renew_id, reason)?;
        let next_id = arm_next_renew_cycle(&tx, renew_id, now)?;
        tx.commit()?;
        Ok(next_id)
    }

    /// End a renew whose clear was never proven, and arm the next cycle.
    ///
    /// The injection is not taken back — it was submitted, and after a stall it
    /// has been retried until it was accepted. What ends here is only the wait
    /// for a rotation that is not coming. A policy that stopped at this cycle
    /// would be a supervision chain broken by exactly the cycle that already
    /// went wrong, so the successor is armed and the notice says the proof
    /// never arrived.
    ///
    /// Legal only from `injected`. A renew still `clearing` has a context that
    /// may already be gone and no resume prompt in it, and abandoning there is
    /// the one thing this design never does.
    ///
    /// # Errors
    ///
    /// Returns a conflict unless the renew has been injected.
    pub fn abandon_renew_proof(
        &mut self,
        renew_id: RenewId,
        reason: &str,
    ) -> Result<Option<RenewId>, StoreError> {
        let now = now_millis()?;
        let tx = self.connection.transaction()?;
        let changed = tx.execute(
            "UPDATE renews SET phase = 'terminated', resolved_at_ms = ?1, termination_reason = ?2
             WHERE id = ?3 AND phase = 'injected'",
            params![now, reason, renew_id.to_string()],
        )?;
        if changed != 1 {
            return Err(StoreError::Conflict("renew is not injected".into()));
        }
        let next_id = arm_next_renew_cycle(&tx, renew_id, now)?;
        tx.commit()?;
        Ok(next_id)
    }

    /// End a renew because its incarnation is no longer Ready.
    ///
    /// A prepare ask still open here is owed by an incarnation that is gone, so
    /// it is settled with the cycle rather than left to remind forever.
    ///
    /// # Errors
    ///
    /// Returns a conflict unless the renew is non-terminal.
    /// End a renew and record its operator notice as one durable fact.
    ///
    /// A policy ending is the event nothing else reports, so the notice is not
    /// a follow-up write that a crash may drop: terminating without announcing
    /// would leave exactly the silent loss this notice exists to prevent, and
    /// nothing would re-raise it because the renew is terminal and
    /// `terminable_renews` will not yield it again.
    ///
    /// # Errors
    ///
    /// Returns a conflict unless the renew is non-terminal.
    pub fn terminate_renew_announced(
        &mut self,
        renew_id: RenewId,
        reason: &str,
        notice: &str,
    ) -> Result<OperatorNoticeId, StoreError> {
        let now = now_millis()?;
        let tx = self.connection.transaction()?;
        let changed = tx.execute(
            "UPDATE renews SET phase = 'terminated', resolved_at_ms = ?1, termination_reason = ?2
             WHERE id = ?3 AND phase NOT IN ('done','aborted','terminated')",
            params![now, reason, renew_id.to_string()],
        )?;
        if changed != 1 {
            return Err(StoreError::Conflict("renew is absent or terminal".into()));
        }
        cancel_unanswered_prepare(&tx, renew_id, reason)?;
        let notice_id = OperatorNoticeId::new();
        tx.execute(
            "INSERT INTO operator_notices (id, body, created_at_ms) VALUES (?1, ?2, ?3)",
            params![notice_id.to_string(), notice, now],
        )?;
        tx.commit()?;
        Ok(notice_id)
    }

    /// End a renew on purpose, at the request of an agent entitled to end it.
    ///
    /// Only the policy's requester or its target may cancel. A cancel any agent
    /// could call would be a way to silently disarm another agent's
    /// supervision, which is the same failure as a policy that ends unnoticed.
    ///
    /// Refused while the cycle is `clearing`. The clear has already gone out and
    /// the resume prompt has not; abandoning it there leaves the agent with an
    /// emptied context and nothing to restore it, which the injection must
    /// never do. The caller may cancel once the cycle finishes.
    ///
    /// # Errors
    ///
    /// Returns a conflict when the renew is absent, terminal, or clearing, and
    /// when the caller is neither the requester nor the target.
    pub fn cancel_renew(
        &mut self,
        renew_id: RenewId,
        requester_agent_id: LogicalAgentId,
        reason: &str,
        notice: &str,
    ) -> Result<OperatorNoticeId, StoreError> {
        let now = now_millis()?;
        let tx = self.connection.transaction()?;
        let changed = tx.execute(
            "UPDATE renews SET phase = 'terminated', resolved_at_ms = ?1, termination_reason = ?2
             WHERE id = ?3 AND phase NOT IN ('done','aborted','terminated','clearing')
               AND (requester_agent_id = ?4 OR logical_agent_id = ?4)",
            params![
                now,
                reason,
                renew_id.to_string(),
                requester_agent_id.to_string()
            ],
        )?;
        if changed != 1 {
            return Err(StoreError::Conflict(cancel_renew_refusal(
                &tx,
                renew_id,
                requester_agent_id,
            )?));
        }
        cancel_unanswered_prepare(&tx, renew_id, reason)?;
        let notice_id = OperatorNoticeId::new();
        tx.execute(
            "INSERT INTO operator_notices (id, body, created_at_ms) VALUES (?1, ?2, ?3)",
            params![notice_id.to_string(), notice, now],
        )?;
        tx.commit()?;
        Ok(notice_id)
    }

    /// End a renew because its incarnation is no longer Ready.
    ///
    /// A prepare ask still open here is owed by an incarnation that is gone, so
    /// it is settled with the cycle rather than left to remind forever.
    ///
    /// Prefer [`Store::terminate_renew_announced`] on the supervision path:
    /// ending a policy without recording why is the silence this operation's
    /// callers exist to avoid.
    ///
    /// # Errors
    ///
    /// Returns a conflict unless the renew is non-terminal.
    pub fn terminate_renew(&mut self, renew_id: RenewId, reason: &str) -> Result<(), StoreError> {
        let now = now_millis()?;
        let tx = self.connection.transaction()?;
        let changed = tx.execute(
            "UPDATE renews SET phase = 'terminated', resolved_at_ms = ?1, termination_reason = ?2
             WHERE id = ?3 AND phase NOT IN ('done','aborted','terminated')",
            params![now, reason, renew_id.to_string()],
        )?;
        if changed != 1 {
            return Err(StoreError::Conflict("renew is absent or terminal".into()));
        }
        cancel_unanswered_prepare(&tx, renew_id, reason)?;
        tx.commit()?;
        Ok(())
    }

    /// Open an attempt record for one of a renew's two external effects.
    ///
    /// # Errors
    ///
    /// Returns an error if the insert fails.
    pub fn prepare_renew_attempt(
        &mut self,
        renew_id: RenewId,
        incarnation_id: IncarnationId,
        step: RenewStep,
        request_id: &str,
        now_ms: i64,
    ) -> Result<(), StoreError> {
        self.connection.execute(
            "INSERT INTO renew_attempts
             (renew_id, incarnation_id, step, request_id, started_at_ms, phase)
             VALUES (?1, ?2, ?3, ?4, ?5, 'prepared')",
            params![
                renew_id.to_string(),
                incarnation_id.to_string(),
                renew_step_text(step),
                request_id,
                now_ms
            ],
        )?;
        Ok(())
    }

    /// Mark a prepared renew attempt as submitted immediately before the write.
    ///
    /// # Errors
    ///
    /// Returns a conflict unless the exact attempt is prepared.
    pub fn submit_renew_attempt(&mut self, request_id: &str) -> Result<(), StoreError> {
        let changed = self.connection.execute(
            "UPDATE renew_attempts SET phase = 'submitted'
             WHERE request_id = ?1 AND phase = 'prepared'",
            [request_id],
        )?;
        if changed != 1 {
            return Err(StoreError::Conflict("renew attempt is not prepared".into()));
        }
        Ok(())
    }

    /// Resolve a submitted renew attempt with its terminal outcome.
    ///
    /// # Errors
    ///
    /// Returns a conflict unless the exact attempt is submitted.
    pub fn resolve_renew_attempt(
        &mut self,
        request_id: &str,
        outcome: &str,
        evidence: Option<&str>,
        now_ms: i64,
    ) -> Result<(), StoreError> {
        if !matches!(outcome, "accepted" | "rejected" | "unknown") {
            return Err(StoreError::InvalidRecord(format!(
                "invalid renew outcome {outcome}"
            )));
        }
        let changed = self.connection.execute(
            "UPDATE renew_attempts SET phase = ?1, resolved_at_ms = ?2, evidence_json = ?3
             WHERE request_id = ?4 AND phase = 'submitted'",
            params![
                outcome,
                now_ms,
                evidence.map(|detail| serde_json::json!({"detail": detail}).to_string()),
                request_id
            ],
        )?;
        if changed != 1 {
            return Err(StoreError::Conflict(
                "renew attempt is not submitted".into(),
            ));
        }
        Ok(())
    }

    /// Whether a renew already submitted the given step to Herdr.
    ///
    /// A clear that reached Herdr must never be sent twice; the context is
    /// already gone, and a second clear would discard the resumed one.
    ///
    /// # Errors
    ///
    /// Returns an error if the lookup fails.
    pub fn renew_step_submitted(
        &self,
        renew_id: RenewId,
        step: RenewStep,
    ) -> Result<bool, StoreError> {
        let submitted: bool = self.connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM renew_attempts
             WHERE renew_id = ?1 AND step = ?2 AND phase != 'prepared')",
            params![renew_id.to_string(), renew_step_text(step)],
            |row| row.get(0),
        )?;
        Ok(submitted)
    }

    /// Replace the observed backend-native session reference after a clear.
    ///
    /// Every other writer of this column refuses to overwrite a recorded value,
    /// so a late or duplicate observation cannot rewrite history. A renew is not
    /// an observation: it is the event that makes the stored reference false, so
    /// it is the one operation permitted to replace it.
    ///
    /// # Errors
    ///
    /// Returns a conflict unless the incarnation exists.
    pub fn replace_observed_native_session(
        &mut self,
        incarnation_id: IncarnationId,
        native_session: &serde_json::Value,
    ) -> Result<(), StoreError> {
        let changed = self.connection.execute(
            "UPDATE incarnations SET observed_native_session_json = ?1 WHERE id = ?2",
            params![native_session.to_string(), incarnation_id.to_string()],
        )?;
        if changed != 1 {
            return Err(StoreError::Conflict("incarnation is absent".into()));
        }
        Ok(())
    }

    /// Backend kind recorded for one incarnation.
    ///
    /// A renew needs this before any durable intent, because a clear command is
    /// defined per backend and an undefined one must be refused rather than
    /// guessed.
    ///
    /// # Errors
    ///
    /// Returns a conflict unless the incarnation exists.
    pub fn incarnation_backend_kind(
        &self,
        incarnation_id: IncarnationId,
    ) -> Result<String, StoreError> {
        self.connection
            .query_row(
                "SELECT backend_kind FROM incarnations WHERE id = ?1",
                [incarnation_id.to_string()],
                |row| row.get(0),
            )
            .optional()?
            .ok_or_else(|| StoreError::Conflict("incarnation is absent".into()))
    }

    /// Earliest scheduled renew due time, if any.
    ///
    /// # Errors
    ///
    /// Returns an error if the lookup fails.
    pub fn next_renew_due_at_ms(&self) -> Result<Option<i64>, StoreError> {
        let next: Option<i64> = self.connection.query_row(
            "SELECT MIN(scheduled_at_ms) FROM renews WHERE phase = 'scheduled'",
            [],
            |row| row.get(0),
        )?;
        Ok(next)
    }

    /// Earliest remaining queued due time, if any.
    ///
    /// # Errors
    ///
    /// Returns an error if the lookup fails.
    /// Append one adapter observation. Does not overwrite prior rows.
    ///
    /// # Errors
    ///
    /// Returns an error when the incarnation is missing or the insert fails.
    pub fn record_observed_attribution(
        &mut self,
        incarnation_id: IncarnationId,
        native_session: Option<&serde_json::Value>,
        observed: &crate::attribution::ObservedAttribution,
    ) -> Result<(), StoreError> {
        let now = now_millis()?;
        let exists: Option<String> = self
            .connection
            .query_row(
                "SELECT id FROM incarnations WHERE id = ?1",
                [incarnation_id.to_string()],
                |row| row.get(0),
            )
            .optional()?;
        if exists.is_none() {
            return Err(StoreError::Conflict(format!(
                "incarnation {incarnation_id} is absent"
            )));
        }
        self.connection.execute(
            "INSERT INTO observed_attributions (
                incarnation_id, recorded_at_ms, adapter, native_session_json,
                model_status, model_value, provider_status, provider_value,
                effort_status, effort_value
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            params![
                incarnation_id.to_string(),
                now,
                observed.adapter,
                native_session.map(ToString::to_string),
                observed_status(&observed.model),
                observed_value(&observed.model),
                observed_status(&observed.provider),
                observed_value(&observed.provider),
                observed_status(&observed.effort),
                observed_value(&observed.effort),
            ],
        )?;
        Ok(())
    }

    /// Latest append-only observation for one incarnation, if any.
    ///
    /// # Errors
    ///
    /// Returns an error when the row is malformed.
    pub fn latest_observed_attribution(
        &self,
        incarnation_id: IncarnationId,
    ) -> Result<Option<crate::attribution::ObservedAttribution>, StoreError> {
        type ObservedRow = (
            String,
            String,
            Option<String>,
            String,
            Option<String>,
            String,
            Option<String>,
        );
        let row: Option<ObservedRow> = self
            .connection
            .query_row(
                "SELECT adapter, model_status, model_value, provider_status, provider_value,
                        effort_status, effort_value
                 FROM observed_attributions
                 WHERE incarnation_id = ?1
                 ORDER BY recorded_at_ms DESC, id DESC
                 LIMIT 1",
                [incarnation_id.to_string()],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                        row.get(6)?,
                    ))
                },
            )
            .optional()?;
        row.map(
            |(adapter, model_s, model_v, provider_s, provider_v, effort_s, effort_v)| {
                Ok(crate::attribution::ObservedAttribution {
                    adapter,
                    model: parse_observed_field(&model_s, model_v)?,
                    provider: parse_observed_field(&provider_s, provider_v)?,
                    effort: parse_observed_field(&effort_s, effort_v)?,
                })
            },
        )
        .transpose()
    }

    /// Requested launch configuration for one incarnation.
    ///
    /// # Errors
    ///
    /// Returns a conflict when the incarnation is absent.
    pub fn requested_attribution(
        &self,
        incarnation_id: IncarnationId,
    ) -> Result<crate::attribution::RequestedAttribution, StoreError> {
        let row: Option<(Option<String>, Option<String>, Option<String>)> = self
            .connection
            .query_row(
                "SELECT requested_model, requested_provider, requested_effort
                 FROM incarnations WHERE id = ?1",
                [incarnation_id.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()?;
        let Some((model, provider, effort)) = row else {
            return Err(StoreError::Conflict(format!(
                "incarnation {incarnation_id} is absent"
            )));
        };
        Ok(crate::attribution::RequestedAttribution {
            model,
            provider,
            effort,
        })
    }

    /// Every logical agent, incarnation, and reply obligation Kelpie holds.
    ///
    /// Incarnations come newest first so a consumer can render one row per agent
    /// without walking retired history. Nothing is interpreted: a state is
    /// reported as recorded, and whether it warrants attention is caller policy.
    ///
    /// # Errors
    ///
    /// Returns an error when a stored row cannot be decoded.
    pub fn report(&self) -> Result<FleetReport, StoreError> {
        let mut agents = self.report_agents()?;
        for (agent_id, incarnation) in self.report_incarnations()? {
            if let Some(agent) = agents
                .iter_mut()
                .find(|agent| agent.id.to_string() == agent_id)
            {
                agent.incarnations.push(incarnation);
            }
        }
        Ok(FleetReport {
            generated_at_ms: now_millis()?,
            agents,
            obligations: self.report_obligations()?,
        })
    }

    fn report_agents(&self) -> Result<Vec<ReportAgent>, StoreError> {
        let mut agents_by_id: Vec<ReportAgent> = Vec::new();
        let mut statement = self.connection.prepare(
            "SELECT id, public_name, parent_agent_id, explicitly_parentless, created_at_ms
             FROM logical_agents ORDER BY created_at_ms, id",
        )?;
        let rows = statement.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, i64>(4)?,
            ))
        })?;
        for row in rows {
            let (id, public_name, parent, parentless, created_at_ms) = row?;
            agents_by_id.push(ReportAgent {
                id: LogicalAgentId::parse(&id)
                    .ok_or_else(|| StoreError::InvalidRecord(format!("invalid agent id {id}")))?,
                public_name,
                parent_agent_id: parent
                    .as_deref()
                    .map(|parent| {
                        LogicalAgentId::parse(parent).ok_or_else(|| {
                            StoreError::InvalidRecord(format!("invalid parent id {parent}"))
                        })
                    })
                    .transpose()?,
                explicitly_parentless: parentless == 1,
                created_at_ms,
                incarnations: Vec::new(),
            });
        }
        drop(statement);
        Ok(agents_by_id)
    }

    fn report_incarnations(&self) -> Result<Vec<(String, ReportIncarnation)>, StoreError> {
        let mut statement = self.connection.prepare(
            "SELECT i.logical_agent_id, i.id, i.state, i.backend_kind, i.working_directory,
                    i.herdr_session, i.intended_pane_id, i.expected_terminal_id,
                    i.observed_pane_id, i.observed_terminal_id, i.backend_args_json,
                    i.requested_model, i.requested_provider, i.requested_effort,
                    i.created_at_ms, i.terminal_at_ms, i.terminal_reason,
                    i.native_session_rotated_at_ms,
                    o.id, o.kind, o.outcome,
                    r.id, r.phase, r.cycle, r.every_ms, r.scheduled_at_ms
             FROM incarnations i
             LEFT JOIN operations o ON o.id = (
                SELECT id FROM operations
                 WHERE target_incarnation_id = i.id
                 ORDER BY created_at_ms DESC, id DESC LIMIT 1
             )
             LEFT JOIN renews r ON r.incarnation_id = i.id
                AND r.phase NOT IN ('done','aborted','terminated')
             ORDER BY i.created_at_ms DESC, i.id DESC",
        )?;
        let rows = statement.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, String>(6)?,
                row.get::<_, String>(7)?,
                row.get::<_, Option<String>>(8)?,
                row.get::<_, Option<String>>(9)?,
                row.get::<_, String>(10)?,
                row.get::<_, Option<String>>(11)?,
                row.get::<_, Option<String>>(12)?,
                row.get::<_, Option<String>>(13)?,
                row.get::<_, i64>(14)?,
                row.get::<_, Option<i64>>(15)?,
                row.get::<_, Option<String>>(16)?,
                row.get::<_, Option<i64>>(17)?,
                row.get::<_, Option<String>>(18)?,
                row.get::<_, Option<String>>(19)?,
                row.get::<_, Option<String>>(20)?,
                row.get::<_, Option<String>>(21)?,
                row.get::<_, Option<String>>(22)?,
                row.get::<_, Option<i64>>(23)?,
                row.get::<_, Option<i64>>(24)?,
                row.get::<_, Option<i64>>(25)?,
            ))
        })?;
        let mut incarnations: Vec<(String, ReportIncarnation)> = Vec::new();
        for row in rows {
            let row = row?;
            incarnations.push((
                row.0,
                ReportIncarnation {
                    id: IncarnationId::parse(&row.1).ok_or_else(|| {
                        StoreError::InvalidRecord(format!("invalid incarnation id {}", row.1))
                    })?,
                    state: parse_incarnation_state(&row.2)?,
                    backend_kind: row.3,
                    working_directory: row.4,
                    herdr_session: row.5,
                    intended_pane_id: row.6,
                    expected_terminal_id: row.7,
                    observed_pane_id: row.8,
                    observed_terminal_id: row.9,
                    requested_backend_args: serde_json::from_str(&row.10)
                        .map_err(|error| invalid_json(&error))?,
                    requested: crate::attribution::RequestedAttribution {
                        model: row.11,
                        provider: row.12,
                        effort: row.13,
                    },
                    created_at_ms: row.14,
                    terminal_at_ms: row.15,
                    terminal_reason: row.16,
                    native_session_rotated_at_ms: row.17,
                    latest_operation: match (row.18, row.19, row.20) {
                        (Some(id), Some(kind), Some(outcome)) => Some((
                            OperationId::parse(&id).ok_or_else(|| {
                                StoreError::InvalidRecord(format!("invalid operation id {id}"))
                            })?,
                            kind,
                            parse_operation_outcome(&outcome)?,
                        )),
                        _ => None,
                    },
                    renew: report_renew(row.21, row.22, row.23, row.24, row.25)?,
                },
            ));
        }
        drop(statement);
        Ok(incarnations)
    }

    fn report_obligations(&self) -> Result<Vec<ReportObligation>, StoreError> {
        let mut statement = self.connection.prepare(
            "SELECT ask_message_id, owing_agent_id, waiting_agent_id, state,
                    created_at_ms, last_activity_at_ms, resolving_message_id
             FROM obligations ORDER BY creation_sequence",
        )?;
        let rows = statement.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, i64>(4)?,
                row.get::<_, i64>(5)?,
                row.get::<_, Option<String>>(6)?,
            ))
        })?;
        let mut obligations = Vec::new();
        for row in rows {
            let (ask, owing, waiting, state, created, activity, resolving) = row?;
            obligations.push(ReportObligation {
                ask_message_id: MessageId::parse(&ask).ok_or_else(|| {
                    StoreError::InvalidRecord(format!("invalid ask message id {ask}"))
                })?,
                owing_agent_id: LogicalAgentId::parse(&owing).ok_or_else(|| {
                    StoreError::InvalidRecord(format!("invalid owing agent id {owing}"))
                })?,
                waiting_agent_id: LogicalAgentId::parse(&waiting).ok_or_else(|| {
                    StoreError::InvalidRecord(format!("invalid waiting agent id {waiting}"))
                })?,
                state: parse_obligation_state(&state)?,
                created_at_ms: created,
                last_activity_at_ms: activity,
                resolving_message_id: resolving
                    .as_deref()
                    .map(|id| {
                        MessageId::parse(id).ok_or_else(|| {
                            StoreError::InvalidRecord(format!("invalid resolving id {id}"))
                        })
                    })
                    .transpose()?,
            });
        }
        drop(statement);
        Ok(obligations)
    }

    /// Newest incarnation of one logical agent.
    ///
    /// Selects deterministically by creation order so an agent id resolves
    /// without ambiguity. Exact verification should address an incarnation.
    ///
    /// # Errors
    ///
    /// Returns a conflict when the agent is absent or has no incarnation.
    pub fn newest_incarnation_for_agent(
        &self,
        agent_id: LogicalAgentId,
    ) -> Result<IncarnationId, StoreError> {
        let exists: bool = self.connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM logical_agents WHERE id = ?1)",
            [agent_id.to_string()],
            |row| row.get(0),
        )?;
        if !exists {
            return Err(StoreError::Conflict(format!(
                "logical agent {agent_id} is absent"
            )));
        }
        let newest: Option<String> = self
            .connection
            .query_row(
                "SELECT id FROM incarnations WHERE logical_agent_id = ?1
                 ORDER BY created_at_ms DESC, id DESC LIMIT 1",
                [agent_id.to_string()],
                |row| row.get(0),
            )
            .optional()?;
        let newest = newest.ok_or_else(|| {
            StoreError::Conflict(format!("logical agent {agent_id} has no incarnation"))
        })?;
        IncarnationId::parse(&newest)
            .ok_or_else(|| StoreError::InvalidRecord(format!("invalid incarnation id {newest}")))
    }

    /// Requested and observed attribution for one exact incarnation.
    ///
    /// Observations are append-only and returned oldest first. An empty list
    /// means nothing has been observed, which callers must not read as an
    /// `Undetermined` observation.
    ///
    /// # Errors
    ///
    /// Returns a conflict when the incarnation is absent.
    pub fn attribution_evidence(
        &self,
        incarnation_id: IncarnationId,
    ) -> Result<AttributionEvidence, StoreError> {
        type IdentityRow = (
            String,
            String,
            String,
            String,
            String,
            Option<String>,
            Option<String>,
            Option<String>,
        );
        let identity: Option<IdentityRow> = self
            .connection
            .query_row(
                "SELECT i.logical_agent_id, a.public_name, i.backend_kind, i.state,
                        i.backend_args_json,
                        i.requested_model, i.requested_provider, i.requested_effort
                 FROM incarnations i
                 JOIN logical_agents a ON a.id = i.logical_agent_id
                 WHERE i.id = ?1",
                [incarnation_id.to_string()],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                        row.get(6)?,
                        row.get(7)?,
                    ))
                },
            )
            .optional()?;
        let Some((agent, public_name, backend_kind, state, backend_args, model, provider, effort)) =
            identity
        else {
            return Err(StoreError::Conflict(format!(
                "incarnation {incarnation_id} is absent"
            )));
        };
        let logical_agent_id = LogicalAgentId::parse(&agent)
            .ok_or_else(|| StoreError::InvalidRecord(format!("invalid agent id {agent}")))?;

        let mut statement = self.connection.prepare(
            "SELECT recorded_at_ms, adapter, model_status, model_value,
                    provider_status, provider_value, effort_status, effort_value
             FROM observed_attributions
             WHERE incarnation_id = ?1
             ORDER BY recorded_at_ms ASC, id ASC",
        )?;
        let rows = statement.query_map([incarnation_id.to_string()], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, Option<String>>(5)?,
                row.get::<_, String>(6)?,
                row.get::<_, Option<String>>(7)?,
            ))
        })?;
        let mut observations = Vec::new();
        for row in rows {
            let (recorded_at_ms, adapter, model_s, model_v, provider_s, provider_v, eff_s, eff_v) =
                row?;
            observations.push(RecordedObservation {
                recorded_at_ms,
                observed: crate::attribution::ObservedAttribution {
                    adapter,
                    model: parse_observed_field(&model_s, model_v)?,
                    provider: parse_observed_field(&provider_s, provider_v)?,
                    effort: parse_observed_field(&eff_s, eff_v)?,
                },
            });
        }

        Ok(AttributionEvidence {
            logical_agent_id,
            incarnation_id,
            public_name,
            backend_kind,
            incarnation_state: parse_incarnation_state(&state)?,
            requested: crate::attribution::RequestedAttribution {
                model,
                provider,
                effort,
            },
            requested_backend_args: serde_json::from_str(&backend_args)
                .map_err(|error| invalid_json(&error))?,
            observations,
        })
    }

    /// Record the intent to move a Ready binding to a new public name.
    ///
    /// Durable before Herdr is asked, so a crash between intent and effect
    /// leaves a knowably pending rename rather than a binding whose live name
    /// silently disagrees with the stored one.
    ///
    /// # Errors
    ///
    /// Returns a conflict when the incarnation is not Ready, when a rename is
    /// already pending, or when another Ready agent already holds the name.
    pub fn declare_rename(
        &mut self,
        incarnation_id: IncarnationId,
        new_name: &str,
    ) -> Result<(), StoreError> {
        let tx = self.connection.transaction()?;
        // A live alias must be unique among Ready agents, so this fails closed
        // rather than creating two agents answering to one name.
        let taken: Option<String> = tx
            .query_row(
                "SELECT i.id FROM incarnations i
                 JOIN logical_agents l ON l.id = i.logical_agent_id
                 WHERE i.state = 'ready' AND l.public_name = ?1 AND i.id != ?2",
                params![new_name, incarnation_id.to_string()],
                |row| row.get(0),
            )
            .optional()?;
        if let Some(existing) = taken {
            return Err(StoreError::Conflict(format!(
                "ready alias {new_name} is already bound to incarnation {existing}"
            )));
        }
        refuse_name_held_by_socket_waiter(&tx, new_name)?;
        let changed = tx.execute(
            "UPDATE incarnations SET pending_rename_to = ?1
             WHERE id = ?2 AND state = 'ready' AND pending_rename_to IS NULL",
            params![new_name, incarnation_id.to_string()],
        )?;
        if changed != 1 {
            return Err(StoreError::Conflict(
                "only a ready incarnation with no pending rename can be renamed".into(),
            ));
        }
        tx.commit()?;
        Ok(())
    }

    /// Commit a rename Herdr has confirmed.
    ///
    /// The alias lives on the logical agent, so this renames that agent rather
    /// than one binding. The incarnation is untouched: a rename is not a new
    /// attempt to bind a runtime, and recording one would misreport history.
    ///
    /// # Errors
    ///
    /// Returns a conflict unless this exact rename is still pending.
    pub fn commit_rename(
        &mut self,
        incarnation_id: IncarnationId,
        new_name: &str,
    ) -> Result<(), StoreError> {
        let tx = self.connection.transaction()?;
        let owner: Option<String> = tx
            .query_row(
                "SELECT logical_agent_id FROM incarnations
                 WHERE id = ?1 AND state = 'ready' AND pending_rename_to = ?2",
                params![incarnation_id.to_string(), new_name],
                |row| row.get(0),
            )
            .optional()?;
        let Some(agent_id) = owner else {
            return Err(StoreError::Conflict(
                "no matching pending rename for this ready incarnation".into(),
            ));
        };
        refuse_name_held_by_socket_waiter(&tx, new_name)?;
        tx.execute(
            "UPDATE logical_agents SET public_name = ?1 WHERE id = ?2",
            params![new_name, agent_id],
        )?;
        tx.execute(
            "UPDATE incarnations SET pending_rename_to = NULL WHERE id = ?1",
            [incarnation_id.to_string()],
        )?;
        tx.commit()?;
        Ok(())
    }

    /// Drop a pending rename Herdr refused, leaving the committed name in place.
    ///
    /// # Errors
    ///
    /// Returns an error if the update fails.
    pub fn abandon_rename(&mut self, incarnation_id: IncarnationId) -> Result<(), StoreError> {
        self.connection.execute(
            "UPDATE incarnations SET pending_rename_to = NULL WHERE id = ?1",
            [incarnation_id.to_string()],
        )?;
        Ok(())
    }

    /// Record a native session that a backend only created after binding.
    ///
    /// Fills the field while it is empty, and only for a Ready incarnation still
    /// bound to the exact pane and terminal given. Some backends create their
    /// session on the first turn, well after readiness, so learning it later is
    /// normal. Overwriting an existing value is not: a different session on the
    /// same pane means a replacement, which recovery must see as such rather
    /// than as new information about this incarnation.
    ///
    /// Returns whether the field was filled.
    ///
    /// # Errors
    ///
    /// Returns an error if the update fails.
    pub fn fill_observed_native_session(
        &mut self,
        incarnation_id: IncarnationId,
        pane_id: &str,
        terminal_id: &str,
        session: &serde_json::Value,
    ) -> Result<bool, StoreError> {
        let changed = self.connection.execute(
            "UPDATE incarnations SET observed_native_session_json = ?1
             WHERE id = ?2 AND state = 'ready'
               AND observed_pane_id = ?3 AND observed_terminal_id = ?4
               AND observed_native_session_json IS NULL",
            params![
                session.to_string(),
                incarnation_id.to_string(),
                pane_id,
                terminal_id
            ],
        )?;
        Ok(changed == 1)
    }

    /// Recorded native session JSON for a Ready incarnation, if any.
    ///
    /// # Errors
    ///
    /// Returns a conflict when the incarnation is absent.
    pub fn observed_native_session(
        &self,
        incarnation_id: IncarnationId,
    ) -> Result<Option<serde_json::Value>, StoreError> {
        let row: Option<Option<String>> = self
            .connection
            .query_row(
                "SELECT observed_native_session_json FROM incarnations WHERE id = ?1",
                [incarnation_id.to_string()],
                |row| row.get(0),
            )
            .optional()?;
        let Some(raw) = row else {
            return Err(StoreError::Conflict(format!(
                "incarnation {incarnation_id} is absent"
            )));
        };
        raw.map(|text| serde_json::from_str(&text).map_err(|error| invalid_json(&error)))
            .transpose()
    }

    /// Earliest remaining queued due time, if any.
    ///
    /// # Errors
    ///
    /// Returns an error if the lookup fails.
    pub fn next_queued_due_at_ms(&self) -> Result<Option<i64>, StoreError> {
        self.connection
            .query_row(
                "SELECT MIN(scheduled_at_ms) FROM deliveries WHERE outcome = 'queued'",
                [],
                |row| row.get(0),
            )
            .map_err(StoreError::Sql)
    }

    /// Earliest enabled reminder due time, if any.
    ///
    /// # Errors
    ///
    /// Returns an error if the lookup fails.
    pub fn next_reminder_due_at_ms(&self) -> Result<Option<i64>, StoreError> {
        self.connection
            .query_row(
                "SELECT MIN(MAX(r.next_due_at_ms, COALESCE(r.snoozed_until_ms, 0)))
                 FROM obligation_reminders r JOIN obligations o
                   ON o.ask_message_id = r.ask_message_id
                 WHERE o.state IN ('open','in_progress')
                   AND r.disabled_at_ms IS NULL AND r.suspended_at_ms IS NULL",
                [],
                |row| row.get(0),
            )
            .map_err(StoreError::Sql)
    }

    /// Earliest unanswered-ask lifecycle check, if any.
    ///
    /// # Errors
    ///
    /// Returns an error if the lookup fails.
    pub fn next_boundary_check_at_ms(&self) -> Result<Option<i64>, StoreError> {
        self.connection
            .query_row(
                "SELECT MIN(MAX(r.boundary_check_at_ms, COALESCE(r.snoozed_until_ms, 0)))
                 FROM obligation_reminders r JOIN obligations o
                   ON o.ask_message_id = r.ask_message_id
                 WHERE o.state = 'open' AND r.last_accepted_at_ms IS NULL
                   AND r.disabled_at_ms IS NULL AND r.suspended_at_ms IS NULL",
                [],
                |row| row.get(0),
            )
            .map_err(StoreError::Sql)
    }

    fn reconcile_ready_incarnations(
        &mut self,
        snapshot: &Snapshot,
    ) -> Result<(usize, usize), StoreError> {
        let ready = {
            let mut statement = self.connection.prepare(
                "SELECT i.id, i.observed_pane_id, i.observed_terminal_id,
                        l.public_name, i.backend_kind, i.observed_native_session_json,
                        i.pending_rename_to
                 FROM incarnations i JOIN logical_agents l ON l.id = i.logical_agent_id
                 WHERE i.state = 'ready'",
            )?;
            let rows = statement.query_map([], |row| {
                Ok(ReadyBindingRow {
                    id: row.get(0)?,
                    pane_id: row.get(1)?,
                    terminal_id: row.get(2)?,
                    public_name: row.get(3)?,
                    backend_kind: row.get(4)?,
                    native_session: row.get(5)?,
                    pending_rename_to: row.get(6)?,
                })
            })?;
            rows.collect::<Result<Vec<_>, _>>()?
        };
        let now = now_millis()?;
        let tx = self.connection.transaction()?;
        let mut marked_lost = 0;
        let mut sessions_refreshed = 0;
        for row in ready {
            let exact_live = snapshot.agents.iter().find(|agent| {
                exact_live_binding(
                    agent,
                    &row.pane_id,
                    &row.terminal_id,
                    &row.backend_kind,
                    &row.public_name,
                )
            });
            // A rename that reached Herdr but not the commit leaves the live
            // agent answering to the target name. That binding is exact, not
            // absent, so recovery finishes the rename instead of losing it.
            let renamed_live = row.pending_rename_to.as_deref().and_then(|target| {
                snapshot.agents.iter().find(|agent| {
                    exact_live_binding(
                        agent,
                        &row.pane_id,
                        &row.terminal_id,
                        &row.backend_kind,
                        target,
                    )
                })
            });
            let bound = renamed_live.or(exact_live);
            // Herdr owns the backend-native conversation reference; Kelpie owns
            // only its association with this incarnation. A live runtime
            // rotates that reference on its own — clear, resume, compaction,
            // fork — so a change is not evidence the runtime was replaced. It
            // is evidence that the recorded value has gone stale, and
            // attribution reads it to find the transcript to observe.
            // Herdr reporting no session at all is not evidence of a change,
            // so only a reported value that differs from a recorded one is a
            // rotation.
            let rotated = bound
                .and_then(|agent| agent.agent_session.as_ref())
                .map(ToString::to_string)
                .filter(|live| {
                    row.native_session
                        .as_ref()
                        .is_some_and(|stored| stored != live)
                });
            if let Some(live) = rotated {
                // A rotation is the only conversation boundary Kelpie can
                // observe without new Herdr traffic, so the age measurement is
                // stamped in the same write rather than costing a second one.
                // It is when the boundary was *seen*, not when it happened;
                // reconciliation reads snapshots, so the two differ by up to
                // one reconcile interval.
                sessions_refreshed += tx.execute(
                    "UPDATE incarnations
                        SET observed_native_session_json = ?1,
                            native_session_rotated_at_ms = ?2
                     WHERE id = ?3 AND state = 'ready'",
                    params![live, now, row.id],
                )?;
            }
            if renamed_live.is_some() {
                let target = row.pending_rename_to.as_deref().unwrap_or_default();
                tx.execute(
                    "UPDATE logical_agents SET public_name = ?1
                     WHERE id = (SELECT logical_agent_id FROM incarnations WHERE id = ?2)",
                    params![target, row.id],
                )?;
                tx.execute(
                    "UPDATE incarnations SET pending_rename_to = NULL WHERE id = ?1",
                    [&row.id],
                )?;
                continue;
            }
            if exact_live.is_some() {
                // The rename never took effect in Herdr; keep the committed name.
                if row.pending_rename_to.is_some() {
                    tx.execute(
                        "UPDATE incarnations SET pending_rename_to = NULL WHERE id = ?1",
                        [&row.id],
                    )?;
                }
            } else {
                marked_lost += tx.execute(
                    "UPDATE incarnations SET state = 'lost', terminal_at_ms = ?1,
                     terminal_reason = 'authoritative_binding_absence'
                     WHERE id = ?2 AND state = 'ready'",
                    params![now, row.id],
                )?;
            }
        }
        tx.commit()?;
        Ok((marked_lost, sessions_refreshed))
    }

    /// Release a Ready alias binding the snapshot says is no longer live.
    ///
    /// Adoption refuses a public name that a Ready incarnation already holds.
    /// That refusal is a durable fact standing in for a live one, and between
    /// reconciliations the two can disagree: a closed pane leaves Kelpie
    /// asserting a Ready agent Herdr does not have, and every adoption of that
    /// name fails against a runtime nobody can reach. Herdr is the authority on
    /// liveness, so the refusal is checked against a snapshot rather than
    /// trusted.
    ///
    /// This is not adoption guessing. The binding is released only when the
    /// snapshot contains no agent at its exact pane and terminal, which is the
    /// same evidence and the same `authoritative_binding_absence` reason
    /// recovery uses. A binding that is still live is left alone and the
    /// conflict stands.
    ///
    /// # Errors
    ///
    /// Returns an error if the snapshot cannot be applied.
    pub fn release_absent_alias_binding(
        &mut self,
        public_name: &str,
        snapshot: &Snapshot,
    ) -> Result<Option<IncarnationId>, StoreError> {
        let bound: Option<(String, Option<String>, Option<String>)> = self
            .connection
            .query_row(
                "SELECT i.id, i.observed_pane_id, i.observed_terminal_id
                 FROM incarnations i
                 JOIN logical_agents l ON l.id = i.logical_agent_id
                 WHERE i.state = 'ready' AND l.public_name = ?1",
                [public_name],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()?;
        let Some((incarnation_id, pane_id, terminal_id)) = bound else {
            return Ok(None);
        };
        let (Some(pane_id), Some(terminal_id)) = (pane_id, terminal_id) else {
            return Ok(None);
        };
        let live = snapshot
            .agents
            .iter()
            .any(|agent| agent.pane_id == pane_id && agent.terminal_id == terminal_id);
        if live {
            return Ok(None);
        }
        let changed = self.connection.execute(
            "UPDATE incarnations SET state = 'lost', terminal_at_ms = ?1,
             terminal_reason = 'authoritative_binding_absence'
             WHERE id = ?2 AND state = 'ready'",
            params![now_millis()?, &incarnation_id],
        )?;
        if changed != 1 {
            return Ok(None);
        }
        Ok(Some(parse_incarnation_id(&incarnation_id)?))
    }

    /// Complete a retirement when the exact binding is gone from a snapshot.
    ///
    /// Public wrapper over the same reconciliation recovery performs, so a
    /// caller that just released a pane settles the retirement immediately
    /// instead of waiting for the next recover.
    ///
    /// # Errors
    ///
    /// Returns an error if the update fails.
    pub fn complete_retirement_if_absent(
        &mut self,
        operation_id: OperationId,
        incarnation_id: IncarnationId,
        pane_id: &str,
        terminal_id: &str,
        snapshot: &Snapshot,
    ) -> Result<bool, StoreError> {
        self.reconcile_retirement(operation_id, incarnation_id, pane_id, terminal_id, snapshot)
    }

    fn reconcile_retirement(
        &mut self,
        operation_id: OperationId,
        incarnation_id: IncarnationId,
        pane_id: &str,
        terminal_id: &str,
        snapshot: &Snapshot,
    ) -> Result<bool, StoreError> {
        let exact_live = snapshot
            .agents
            .iter()
            .any(|agent| agent.pane_id == pane_id && agent.terminal_id == terminal_id);
        if exact_live {
            return Ok(false);
        }
        let now = now_millis()?;
        let tx = self.connection.transaction()?;
        let changed = tx.execute(
            "UPDATE incarnations SET state = 'retired', terminal_at_ms = ?1,
             terminal_reason = 'authoritative_absence'
             WHERE id = ?2 AND state = 'retiring'",
            params![now, incarnation_id.to_string()],
        )?;
        if changed != 1 {
            return Err(StoreError::Conflict(
                "retirement result cannot mutate this incarnation".into(),
            ));
        }
        tx.execute(
            "UPDATE operations SET outcome = 'succeeded', resolved_at_ms = ?1
             WHERE id = ?2 AND outcome = 'accepted'",
            params![now, operation_id.to_string()],
        )?;
        tx.commit()?;
        Ok(true)
    }

    fn create_unknown_prompt_notice(
        &mut self,
        operation_id: OperationId,
        incarnation_id: IncarnationId,
    ) -> Result<(), StoreError> {
        self.create_operator_notice(&format!(
            "prompt operation {operation_id} has unknown delivery outcome for incarnation {incarnation_id}"
        ))?;
        Ok(())
    }

    /// Reconcile interrupted operations against one fresh Herdr snapshot.
    ///
    /// This method never causes an external effect. An attempted start succeeds
    /// only from exact terminal, pane, public-name, and readiness evidence.
    /// Ready incarnations stay live only on exact pane, terminal, backend,
    /// public name, and recorded native session.
    /// Attempted prompts become `unknown` because a snapshot cannot prove
    /// terminal-input delivery. Intents with no attempt remain pending.
    ///
    /// # Errors
    ///
    /// Returns an error when durable records are malformed or a transition conflicts.
    #[allow(clippy::too_many_lines)]
    pub fn reconcile(&mut self, snapshot: &Snapshot) -> Result<RecoveryReport, StoreError> {
        let candidates = {
            let mut statement = self.connection.prepare(
                "SELECT o.id, o.target_incarnation_id, o.kind, i.intended_pane_id,
                        i.expected_terminal_id, o.intent_json,
                        EXISTS(SELECT 1 FROM operation_attempts a
                               WHERE a.operation_id = o.id AND a.phase != 'prepared')
                 FROM operations o JOIN incarnations i ON i.id = o.target_incarnation_id
                 WHERE o.outcome IN ('pending', 'accepted')",
            )?;
            let rows = statement.query_map([], |row| {
                let operation: String = row.get(0)?;
                let incarnation: String = row.get(1)?;
                Ok((
                    operation,
                    incarnation,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                ))
            })?;
            let mut values = Vec::new();
            for row in rows {
                let (operation, incarnation, kind, pane_id, terminal_id, intent_json, attempted) =
                    row?;
                values.push(RecoveryCandidate {
                    operation_id: OperationId::parse(&operation).ok_or_else(|| {
                        StoreError::InvalidRecord(format!("invalid operation id {operation}"))
                    })?,
                    incarnation_id: IncarnationId::parse(&incarnation).ok_or_else(|| {
                        StoreError::InvalidRecord(format!("invalid incarnation id {incarnation}"))
                    })?,
                    kind,
                    pane_id,
                    terminal_id,
                    intent_json,
                    attempted,
                });
            }
            values
        };

        let now = now_millis()?;
        let (incarnations_marked_lost, native_sessions_refreshed) =
            self.reconcile_ready_incarnations(snapshot)?;
        let mut report = RecoveryReport {
            incarnations_marked_lost,
            native_sessions_refreshed,
            outcomes_marked_unknown: self.reconcile_missed_due_wakes(now)?,
            ..RecoveryReport::default()
        };
        for candidate in candidates {
            if candidate.kind == "retire" {
                if self.reconcile_retirement(
                    candidate.operation_id,
                    candidate.incarnation_id,
                    &candidate.pane_id,
                    &candidate.terminal_id,
                    snapshot,
                )? {
                    report.retirements_completed += 1;
                } else {
                    report.retirements_still_live += 1;
                }
                continue;
            }
            if !candidate.attempted {
                if candidate.kind == "clear" {
                    let changed = self.connection.execute(
                        "UPDATE operations SET outcome = 'failed', resolved_at_ms = ?1
                         WHERE id = ?2 AND kind = 'clear' AND outcome = 'pending'
                           AND NOT EXISTS(SELECT 1 FROM operation_attempts a
                                          WHERE a.operation_id = operations.id
                                            AND a.phase != 'prepared')",
                        params![now, candidate.operation_id.to_string()],
                    )?;
                    if changed == 1 {
                        self.create_operator_notice(&format!(
                            "clear operation {} failed during recovery before any Herdr write",
                            candidate.operation_id
                        ))?;
                        report.unattempted_clears_failed += 1;
                        continue;
                    }
                }
                report.untouched_pending_intents += 1;
                continue;
            }
            if candidate.kind == "start" {
                let intent: StartIntent = serde_json::from_str(&candidate.intent_json)
                    .map_err(|error| invalid_json(&error))?;
                if let Some(agent) = snapshot.agents.iter().find(|agent| {
                    agent.pane_id == candidate.pane_id
                        && agent.terminal_id == candidate.terminal_id
                        && agent.name.as_deref() == Some(intent.public_name.as_str())
                        && agent.agent.as_deref() == Some(intent.backend_kind.as_str())
                        && agent.interactive_ready
                        && !agent.launch_pending
                }) {
                    match self.accept_start_ready(
                        candidate.operation_id,
                        candidate.incarnation_id,
                        agent,
                        None,
                    ) {
                        Ok(()) => {
                            report.starts_recovered += 1;
                            continue;
                        }
                        Err(StoreError::Conflict(_)) => {}
                        Err(error) => return Err(error),
                    }
                }
            }
            if candidate.kind == "adopt" {
                let intended = adopt_intended_name(&candidate.intent_json)?;
                if let Some(agent) = snapshot.agents.iter().find(|agent| {
                    agent.pane_id == candidate.pane_id
                        && agent.terminal_id == candidate.terminal_id
                        && agent.name.as_deref() == Some(intended.as_str())
                        && !agent.launch_pending
                }) && self
                    .accept_adopt_ready(candidate.operation_id, candidate.incarnation_id, agent)
                    .is_ok()
                {
                    continue;
                }
            }
            let evidence =
                "fresh snapshot cannot prove the attempted external effect's terminal outcome";
            if candidate.kind == "clear" {
                let delay_ms = clear_prompt_settle_delay_ms(&candidate.intent_json)?;
                self.mark_clear_unknown(
                    candidate.operation_id,
                    candidate.incarnation_id,
                    evidence,
                    delay_ms,
                )?;
            } else {
                self.mark_unknown(candidate.operation_id, candidate.incarnation_id, evidence)?;
            }
            if candidate.kind == "prompt" {
                self.create_unknown_prompt_notice(
                    candidate.operation_id,
                    candidate.incarnation_id,
                )?;
            } else if candidate.kind == "clear" {
                self.create_operator_notice(&format!(
                    "clear operation {} has unknown outcome after recovery for incarnation {}",
                    candidate.operation_id, candidate.incarnation_id
                ))?;
            }
            report.outcomes_marked_unknown += 1;
        }
        Ok(report)
    }

    fn reconcile_missed_due_wakes(&mut self, now_ms: i64) -> Result<usize, StoreError> {
        let overdue = {
            let mut statement = self.connection.prepare(
                "SELECT d.operation_id, d.recipient_incarnation_id
                 FROM deliveries d
                 JOIN operations o ON o.id = d.operation_id
                 WHERE d.outcome = 'queued' AND d.scheduled_at_ms <= ?1
                   AND o.outcome IN ('pending', 'accepted')
                   AND NOT EXISTS (SELECT 1 FROM operations clear
                                   WHERE clear.kind = 'clear'
                                     AND clear.target_incarnation_id = d.recipient_incarnation_id
                                     AND clear.outcome IN ('pending','accepted'))",
            )?;
            let rows = statement.query_map([now_ms], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?;
            rows.collect::<Result<Vec<_>, _>>()?
        };
        let mut marked = 0;
        for (operation, incarnation) in overdue {
            let operation_id = OperationId::parse(&operation).ok_or_else(|| {
                StoreError::InvalidRecord(format!("invalid operation id {operation}"))
            })?;
            let incarnation_id = IncarnationId::parse(&incarnation).ok_or_else(|| {
                StoreError::InvalidRecord(format!("invalid incarnation id {incarnation}"))
            })?;
            self.mark_unknown(
                operation_id,
                incarnation_id,
                "due time elapsed while kelpied was not running; no new attempt is recorded",
            )?;
            self.create_unknown_prompt_notice(operation_id, incarnation_id)?;
            marked += 1;
        }
        Ok(marked)
    }
}

/// Arm the successor cycle of a renew policy, if the resolved renew was one.
///
/// The successor is a new row so the resolved cycle keeps its own evidence and
/// the one-active-renew index stays satisfied. A one-shot renew arms nothing.
/// Say which rule refused a cancel, so the caller knows what to do next.
///
/// The `UPDATE` folds three refusals into one row count. "Not permitted" and
/// "already over" ask opposite things of the caller — stop, or stop worrying —
/// and "clearing" is the only one that becomes permitted by waiting.
fn cancel_renew_refusal(
    tx: &Transaction<'_>,
    renew_id: RenewId,
    requester_agent_id: LogicalAgentId,
) -> Result<String, StoreError> {
    let found = tx
        .query_row(
            "SELECT phase, requester_agent_id, logical_agent_id FROM renews WHERE id = ?1",
            [renew_id.to_string()],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            },
        )
        .optional()?;
    let Some((phase, requester, target)) = found else {
        return Ok(format!("renew {renew_id} does not exist"));
    };
    let caller = requester_agent_id.to_string();
    if caller != requester && caller != target {
        return Ok(format!(
            "renew {renew_id} may be cancelled only by its requester {requester} or its target \
             {target}, not by {caller}"
        ));
    }
    if phase == "clearing" {
        return Ok(format!(
            "renew {renew_id} has already cleared this cycle and has not injected its resume \
             prompt yet; cancelling now would leave the agent with an emptied context. Cancel \
             once the cycle finishes"
        ));
    }
    Ok(format!("renew {renew_id} already ended ({phase})"))
}

/// Settle a prepare ask the cycle has moved past without an answer.
///
/// An obligation outlives its runtime, which is why the cycle that created this
/// one has to end it: nothing else ever will, and an ask left open is a reply
/// obligation reminding an agent about a checkpoint that no longer exists.
///
/// Cancellation, not resolution — no reply was ever delivered, and the record
/// says so. Attribution goes to the renew's requester because the requester is
/// the ask's waiting agent, which is who else would have had to cancel it by
/// hand. An already settled or absent obligation is left alone, so this is
/// idempotent and safe on every terminal path.
fn cancel_unanswered_prepare(
    tx: &Transaction<'_>,
    renew_id: RenewId,
    reason: &str,
) -> Result<bool, StoreError> {
    let renew: Option<(Option<String>, String)> = tx
        .query_row(
            "SELECT ask_message_id, requester_agent_id FROM renews WHERE id = ?1",
            [renew_id.to_string()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let Some((Some(ask_message_id), requester_agent_id)) = renew else {
        return Ok(false);
    };
    let changed = tx.execute(
        "UPDATE obligations SET state = 'cancelled', last_activity_at_ms = ?1,
         cancellation_requester_agent_id = ?2, cancellation_reason = ?3
         WHERE ask_message_id = ?4 AND state IN ('open', 'in_progress')",
        params![now_millis()?, requester_agent_id, reason, ask_message_id],
    )?;
    Ok(changed == 1)
}

fn arm_next_renew_cycle(
    tx: &Transaction<'_>,
    renew_id: RenewId,
    now: i64,
) -> Result<Option<RenewId>, StoreError> {
    let policy: Option<(Option<i64>, i64)> = tx
        .query_row(
            "SELECT every_ms, cycle FROM renews WHERE id = ?1",
            [renew_id.to_string()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let Some((Some(every_ms), cycle)) = policy else {
        return Ok(None);
    };
    let next_id = RenewId::new();
    tx.execute(
        "INSERT INTO renews
         (id, logical_agent_id, incarnation_id, requester_agent_id, prepare_prompt,
          resume_prompt, on_timeout, prepare_timeout_ms, every_ms, cycle,
          scheduled_at_ms, phase, created_at_ms)
         SELECT ?1, logical_agent_id, incarnation_id, requester_agent_id, prepare_prompt,
                resume_prompt, on_timeout, prepare_timeout_ms, every_ms, ?2,
                ?3, 'scheduled', ?4
         FROM renews WHERE id = ?5",
        params![
            next_id.to_string(),
            cycle + 1,
            now.saturating_add(every_ms),
            now,
            renew_id.to_string()
        ],
    )?;
    Ok(Some(next_id))
}

fn next_obligation_sequence(tx: &Transaction<'_>) -> Result<i64, StoreError> {
    tx.query_row(
        "SELECT COALESCE(MAX(creation_sequence), 0) + 1 FROM obligations",
        [],
        |row| row.get(0),
    )
    .map_err(StoreError::Sql)
}

fn insert_obligation(
    tx: &Transaction<'_>,
    ask_message_id: MessageId,
    owing_agent_id: LogicalAgentId,
    waiting_agent_id: LogicalAgentId,
    now: i64,
) -> Result<(), StoreError> {
    tx.execute(
        "INSERT INTO obligations
         (ask_message_id, owing_agent_id, waiting_agent_id, creation_sequence,
          created_at_ms, last_activity_at_ms, state)
         VALUES (?1, ?2, ?3, ?4, ?5, ?5, 'open')",
        params![
            ask_message_id.to_string(),
            owing_agent_id.to_string(),
            waiting_agent_id.to_string(),
            next_obligation_sequence(tx)?,
            now
        ],
    )?;
    Ok(())
}

fn refresh_reminder_activity(
    tx: &Transaction<'_>,
    ask_message_id: MessageId,
    now_ms: i64,
) -> Result<(), StoreError> {
    tx.execute(
        "UPDATE obligation_reminders
         SET next_due_at_ms = ?1 + interval_ms, snoozed_until_ms = NULL
         WHERE ask_message_id = ?2 AND disabled_at_ms IS NULL AND suspended_at_ms IS NULL",
        params![now_ms, ask_message_id.to_string()],
    )?;
    Ok(())
}

fn parse_message_id(value: &str) -> Result<MessageId, StoreError> {
    MessageId::parse(value)
        .ok_or_else(|| StoreError::InvalidRecord(format!("invalid message id {value}")))
}

fn parse_logical_agent_id(value: &str) -> Result<LogicalAgentId, StoreError> {
    LogicalAgentId::parse(value)
        .ok_or_else(|| StoreError::InvalidRecord(format!("invalid logical agent id {value}")))
}

fn parse_renew_id(value: &str) -> Result<RenewId, StoreError> {
    RenewId::parse(value)
        .ok_or_else(|| StoreError::InvalidRecord(format!("invalid renew id {value}")))
}

/// Build the armed-renew view of one report row.
///
/// Split out of `report_incarnations` so the row mapping there stays within one
/// screen; the LEFT JOIN yields all five columns or none.
fn report_renew(
    id: Option<String>,
    phase: Option<String>,
    cycle: Option<i64>,
    every_ms: Option<i64>,
    scheduled_at_ms: Option<i64>,
) -> Result<Option<ReportRenew>, StoreError> {
    let (Some(id), Some(phase), Some(cycle), Some(scheduled_at_ms)) =
        (id, phase, cycle, scheduled_at_ms)
    else {
        return Ok(None);
    };
    Ok(Some(ReportRenew {
        id: parse_renew_id(&id)?,
        phase: parse_renew_phase(&phase)?,
        cycle,
        every_ms,
        scheduled_at_ms,
    }))
}

fn parse_renew_phase(value: &str) -> Result<RenewPhase, StoreError> {
    match value {
        "scheduled" => Ok(RenewPhase::Scheduled),
        "preparing" => Ok(RenewPhase::Preparing),
        "ready" => Ok(RenewPhase::Ready),
        "clearing" => Ok(RenewPhase::Clearing),
        "injected" => Ok(RenewPhase::Injected),
        "done" => Ok(RenewPhase::Done),
        "timed_out" => Ok(RenewPhase::TimedOut),
        "aborted" => Ok(RenewPhase::Aborted),
        "terminated" => Ok(RenewPhase::Terminated),
        other => Err(StoreError::InvalidRecord(format!(
            "unknown renew phase {other}"
        ))),
    }
}

fn parse_renew_timeout(value: &str) -> Result<RenewTimeout, StoreError> {
    match value {
        "abort" => Ok(RenewTimeout::Abort),
        "proceed" => Ok(RenewTimeout::Proceed),
        other => Err(StoreError::InvalidRecord(format!(
            "unknown renew timeout disposition {other}"
        ))),
    }
}

fn renew_timeout_text(value: RenewTimeout) -> &'static str {
    match value {
        RenewTimeout::Abort => "abort",
        RenewTimeout::Proceed => "proceed",
    }
}

fn renew_step_text(value: RenewStep) -> &'static str {
    match value {
        RenewStep::Clear => "clear",
        RenewStep::Inject => "inject",
    }
}

fn parse_incarnation_id(value: &str) -> Result<IncarnationId, StoreError> {
    IncarnationId::parse(value)
        .ok_or_else(|| StoreError::InvalidRecord(format!("invalid incarnation id {value}")))
}

fn configure(connection: &Connection) -> Result<(), StoreError> {
    connection.pragma_update(None, "foreign_keys", "ON")?;
    connection.pragma_update(None, "journal_mode", "WAL")?;
    connection.pragma_update(None, "synchronous", "FULL")?;
    Ok(())
}

fn ensure_outside_repository(path: &Path) -> Result<(), StoreError> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(|error| StoreError::InvalidRecord(error.to_string()))?
            .join(path)
    };
    let directory = absolute.parent().unwrap_or(&absolute);
    if directory
        .ancestors()
        .any(|ancestor| ancestor.join(".git").exists())
    {
        return Err(StoreError::UnsafeLocation(absolute));
    }
    Ok(())
}

#[allow(clippy::too_many_lines)]
fn migrate(connection: &Connection) -> Result<(), StoreError> {
    let mut version: i64 = connection.query_row("PRAGMA user_version", [], |row| row.get(0))?;
    if version == 0 {
        connection.execute_batch(include_str!("../migrations/001_initial.sql"))?;
        version = 1;
    }
    if version == 1 {
        connection.execute_batch(include_str!("../migrations/002_operator_notices.sql"))?;
        version = 2;
    }
    if version == 2 {
        connection.execute_batch(include_str!(
            "../migrations/003_operator_message_sender.sql"
        ))?;
        version = 3;
    }
    if version == 3 {
        connection.execute_batch(include_str!(
            "../migrations/004_obligation_creation_sequence.sql"
        ))?;
        version = 4;
    }
    if version == 4 {
        connection.execute_batch(include_str!(
            "../migrations/005_obligation_cancellation.sql"
        ))?;
        version = 5;
    }
    if version == 5 {
        connection.execute_batch(include_str!("../migrations/006_adopt_operation.sql"))?;
        version = 6;
    }
    if version == 6 {
        connection.execute_batch(include_str!("../migrations/007_name_authority.sql"))?;
        backfill_name_authority(connection)?;
        version = 7;
    }
    if version == 7 {
        connection.execute_batch(include_str!("../migrations/008_scheduled_delivery.sql"))?;
        version = 8;
    }
    if version == 8 {
        connection.execute_batch(include_str!("../migrations/009_observed_attribution.sql"))?;
        version = 9;
    }
    if version == 9 {
        connection.execute_batch(include_str!("../migrations/010_obligation_reminders.sql"))?;
        version = 10;
    }
    if version == 10 {
        connection.execute_batch(include_str!("../migrations/011_pending_rename.sql"))?;
        version = 11;
    }
    if version == 11 {
        connection.execute_batch(include_str!("../migrations/012_renew.sql"))?;
        version = 12;
    }
    if version == 12 {
        connection.execute_batch(include_str!("../migrations/013_conversation_age.sql"))?;
        version = 13;
    }
    if version == 13 {
        connection.execute_batch(include_str!("../migrations/014_renew_clear_stall.sql"))?;
        version = 14;
    }
    if version == 14 {
        connection.execute_batch(include_str!("../migrations/015_lazy_rotation.sql"))?;
        version = 15;
    }
    if version == 15 {
        connection.execute_batch(include_str!("../migrations/016_clear_operation.sql"))?;
        version = 16;
    }
    if version == 16 {
        connection.execute_batch(include_str!(
            "../migrations/017_settle_stranded_prepare_asks.sql"
        ))?;
        version = 17;
    }
    if version == 17 {
        connection.execute_batch(include_str!("../migrations/018_cancellation_message.sql"))?;
        version = 18;
    }
    if version == 18 {
        connection.execute_batch(include_str!(
            "../migrations/019_cancellation_response_link.sql"
        ))?;
        version = 19;
    }
    if version == 19 {
        connection.execute_batch(include_str!("../migrations/020_socket_waiter.sql"))?;
        version = 20;
    }
    if version == 20 {
        connection.execute_batch(include_str!("../migrations/021_socket_inbox_keys.sql"))?;
        version = 21;
    }
    if version == 21 {
        connection.execute_batch(include_str!("../migrations/022_owing_cancellation.sql"))?;
        version = 22;
    }
    if version != SCHEMA_VERSION {
        return Err(StoreError::InvalidRecord(format!(
            "unsupported schema version {version}"
        )));
    }
    Ok(())
}

fn ready_incarnation_for_agent(
    tx: &Connection,
    logical_agent_id: LogicalAgentId,
) -> Result<IncarnationId, StoreError> {
    let mut statement = tx.prepare(
        "SELECT id FROM incarnations
         WHERE logical_agent_id = ?1 AND state = 'ready'
         ORDER BY created_at_ms ASC",
    )?;
    let rows = statement.query_map([logical_agent_id.to_string()], |row| {
        row.get::<_, String>(0)
    })?;
    let mut matches = Vec::new();
    for row in rows {
        matches.push(row?);
    }
    match matches.as_slice() {
        [incarnation_id] => IncarnationId::parse(incarnation_id).ok_or_else(|| {
            StoreError::InvalidRecord(format!("invalid incarnation id {incarnation_id}"))
        }),
        [] => Err(StoreError::Conflict(format!(
            "no ready incarnation for waiting agent {logical_agent_id}"
        ))),
        _ => Err(StoreError::Conflict(format!(
            "ambiguous ready incarnation for waiting agent {logical_agent_id}"
        ))),
    }
}

#[derive(Debug)]
struct ReadyBindingRow {
    id: String,
    pane_id: String,
    terminal_id: String,
    public_name: String,
    backend_kind: String,
    native_session: Option<String>,
    pending_rename_to: Option<String>,
}

fn adopt_intended_name(intent_json: &str) -> Result<String, StoreError> {
    let value: serde_json::Value =
        serde_json::from_str(intent_json).map_err(|error| invalid_json(&error))?;
    value
        .pointer("/evidence/public_name")
        .and_then(serde_json::Value::as_str)
        .filter(|name| !name.is_empty())
        .map(ToOwned::to_owned)
        .ok_or_else(|| StoreError::InvalidRecord("adopt intent is missing public_name".into()))
}

fn clear_prompt_settle_delay_ms(intent_json: &str) -> Result<i64, StoreError> {
    let intent: serde_json::Value =
        serde_json::from_str(intent_json).map_err(|error| invalid_json(&error))?;
    intent
        .get("prompt_settle_delay_ms")
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| {
            StoreError::InvalidRecord("clear intent is missing prompt_settle_delay_ms".into())
        })
}

fn empty_to_none(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn observed_status(field: &crate::attribution::ObservedField) -> &'static str {
    match field {
        crate::attribution::ObservedField::Undetermined => "undetermined",
        crate::attribution::ObservedField::Reported(_) => "reported",
    }
}

fn observed_value(field: &crate::attribution::ObservedField) -> Option<&str> {
    match field {
        crate::attribution::ObservedField::Undetermined => None,
        crate::attribution::ObservedField::Reported(value) => Some(value.as_str()),
    }
}

fn parse_observed_field(
    status: &str,
    value: Option<String>,
) -> Result<crate::attribution::ObservedField, StoreError> {
    match (status, value) {
        ("undetermined", None) => Ok(crate::attribution::ObservedField::Undetermined),
        ("reported", Some(value)) => Ok(crate::attribution::ObservedField::Reported(value)),
        _ => Err(StoreError::InvalidRecord(format!(
            "invalid observed field status {status}"
        ))),
    }
}

/// Whether a live observation is this incarnation's exact binding.
///
/// The backend-native session reference is deliberately absent. It identifies a
/// conversation, not a runtime, and a live agent rotates it on clear, resume,
/// compaction, or fork. Requiring it to match read those rotations as proof the
/// runtime had gone.
fn exact_live_binding(
    agent: &crate::herdr::AgentObservation,
    pane_id: &str,
    terminal_id: &str,
    backend_kind: &str,
    public_name: &str,
) -> bool {
    agent.pane_id == pane_id
        && agent.terminal_id == terminal_id
        && agent.agent.as_deref() == Some(backend_kind)
        && agent.name.as_deref() == Some(public_name)
}

fn backfill_name_authority(connection: &Connection) -> Result<(), StoreError> {
    let mut statement = connection.prepare(
        "SELECT i.id, o.intent_json
         FROM incarnations i
         JOIN operations o ON o.target_incarnation_id = i.id
         WHERE o.kind = 'adopt'",
    )?;
    let rows = statement.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })?;
    let mut updates = Vec::new();
    for row in rows {
        let (id, intent_json) = row?;
        let value: serde_json::Value =
            serde_json::from_str(&intent_json).map_err(|error| invalid_json(&error))?;
        let session = value.pointer("/evidence/native_agent_session").cloned();
        updates.push((id, session));
    }
    drop(statement);
    for (id, session) in updates {
        if let Some(session) = session.filter(|value| !value.is_null()) {
            connection.execute(
                "UPDATE incarnations SET observed_native_session_json = ?1 WHERE id = ?2",
                params![session.to_string(), id],
            )?;
        }
    }
    Ok(())
}

fn find_ready_identity_query(
    store: &Store,
    sql: &str,
    key: &str,
    kind: &str,
) -> Result<Option<ReadyIdentity>, StoreError> {
    let mut statement = store.connection.prepare(sql)?;
    let rows = statement.query_map([key], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
        ))
    })?;
    let mut matches = Vec::new();
    for row in rows {
        matches.push(row?);
    }
    match matches.as_slice() {
        [(agent, incarnation, name)] => Ok(Some(ReadyIdentity {
            logical_agent_id: LogicalAgentId::parse(agent).ok_or_else(|| {
                StoreError::InvalidRecord(format!("invalid logical agent id {agent}"))
            })?,
            incarnation_id: IncarnationId::parse(incarnation).ok_or_else(|| {
                StoreError::InvalidRecord(format!("invalid incarnation id {incarnation}"))
            })?,
            public_name: name.clone(),
        })),
        [] => Ok(None),
        _ => Err(StoreError::Conflict(format!(
            "{kind} {key} is ambiguous among ready agents"
        ))),
    }
}

fn insert_logical_agent(
    tx: &Transaction<'_>,
    id: LogicalAgentId,
    name: &str,
    parent: Parent,
    delivery_transport: DeliveryTransport,
    now: i64,
) -> Result<(), StoreError> {
    let parent_id = match parent {
        Parent::Parentless => None,
        Parent::Agent(id) => Some(id.to_string()),
    };
    tx.execute(
        "INSERT INTO logical_agents
         (id, public_name, parent_agent_id, explicitly_parentless, created_at_ms, delivery_transport)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![
            id.to_string(),
            name,
            parent_id,
            i64::from(matches!(parent, Parent::Parentless)),
            now,
            delivery_transport.as_str()
        ],
    )?;
    Ok(())
}

fn refuse_pane_bind_of_socket_inbox(
    tx: &Transaction<'_>,
    logical_agent_id: LogicalAgentId,
) -> Result<(), StoreError> {
    let transport: Option<String> = tx
        .query_row(
            "SELECT delivery_transport FROM logical_agents WHERE id = ?1",
            [logical_agent_id.to_string()],
            |row| row.get(0),
        )
        .optional()?;
    if transport.as_deref() == Some("socket_inbox") {
        return Err(StoreError::Conflict(format!(
            "cannot bind a pane to socket-inbox logical agent {logical_agent_id}"
        )));
    }
    Ok(())
}

fn refuse_live_or_pending_alias(tx: &Transaction<'_>, name: &str) -> Result<(), StoreError> {
    let held: Option<String> = tx
        .query_row(
            "SELECT i.id FROM incarnations i
             JOIN logical_agents l ON l.id = i.logical_agent_id
             WHERE l.public_name = ?1
               AND i.state IN ('starting','ready')",
            [name],
            |row| row.get(0),
        )
        .optional()?;
    if let Some(existing) = held {
        return Err(StoreError::Conflict(format!(
            "alias {name} is already bound to incarnation {existing}"
        )));
    }
    let pending: Option<String> = tx
        .query_row(
            "SELECT id FROM incarnations
             WHERE pending_rename_to = ?1 AND state = 'ready'",
            [name],
            |row| row.get(0),
        )
        .optional()?;
    if let Some(existing) = pending {
        return Err(StoreError::Conflict(format!(
            "alias {name} is already a pending rename target for incarnation {existing}"
        )));
    }
    Ok(())
}

fn require_active_socket_waiter(
    conn: &Connection,
    logical_agent_id: LogicalAgentId,
) -> Result<(), StoreError> {
    let row: Option<(String, Option<i64>)> = conn
        .query_row(
            "SELECT delivery_transport, targeting_ended_at_ms
             FROM logical_agents WHERE id = ?1",
            [logical_agent_id.to_string()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    match row {
        Some((transport, ended)) if transport == "socket_inbox" && ended.is_none() => Ok(()),
        Some(_) => Err(StoreError::Conflict(format!(
            "logical agent {logical_agent_id} is not an active socket waiter"
        ))),
        None => Err(StoreError::Conflict(format!(
            "socket waiter {logical_agent_id} is absent"
        ))),
    }
}

fn refuse_name_held_by_socket_waiter(tx: &Transaction<'_>, name: &str) -> Result<(), StoreError> {
    let held: Option<String> = tx
        .query_row(
            "SELECT id FROM logical_agents
             WHERE public_name = ?1
               AND delivery_transport = 'socket_inbox'
               AND targeting_ended_at_ms IS NULL",
            [name],
            |row| row.get(0),
        )
        .optional()?;
    if let Some(existing) = held {
        return Err(StoreError::Conflict(format!(
            "socket waiter {existing} already holds public name {name}"
        )));
    }
    Ok(())
}

fn parse_delivery_transport(value: &str) -> Result<DeliveryTransport, StoreError> {
    match value {
        "herdr_prompt" => Ok(DeliveryTransport::HerdrPrompt),
        "socket_inbox" => Ok(DeliveryTransport::SocketInbox),
        other => Err(StoreError::InvalidRecord(format!(
            "unknown delivery_transport {other}"
        ))),
    }
}

struct WaiterIdentity {
    transport: DeliveryTransport,
    ended: bool,
}

fn waiter_identity(
    conn: &Connection,
    logical_agent_id: LogicalAgentId,
) -> Result<WaiterIdentity, StoreError> {
    let row: Option<(String, Option<i64>)> = conn
        .query_row(
            "SELECT delivery_transport, targeting_ended_at_ms
             FROM logical_agents WHERE id = ?1",
            [logical_agent_id.to_string()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let Some((transport, ended_at)) = row else {
        return Err(StoreError::Conflict(format!(
            "waiting agent {logical_agent_id} is absent"
        )));
    };
    let transport = parse_delivery_transport(&transport)?;
    Ok(WaiterIdentity {
        ended: matches!(transport, DeliveryTransport::SocketInbox) && ended_at.is_some(),
        transport,
    })
}

fn queue_socket_inbox_delivery(
    tx: &Transaction<'_>,
    message_id: MessageId,
    recipient_agent_id: LogicalAgentId,
    now: i64,
) -> Result<(), StoreError> {
    tx.execute(
        "INSERT INTO deliveries
         (message_id, delivery_transport, recipient_incarnation_id, recipient_agent_id,
          attempt_number, scheduled_at_ms, outcome)
         VALUES (?1, 'socket_inbox', NULL, ?2, 1, ?3, 'queued')",
        params![message_id.to_string(), recipient_agent_id.to_string(), now],
    )?;
    Ok(())
}

fn optional_ready_incarnation(
    tx: &Transaction<'_>,
    logical_agent_id: LogicalAgentId,
    conflict: &str,
) -> Result<Option<IncarnationId>, StoreError> {
    let mut statement = tx.prepare(
        "SELECT id FROM incarnations
         WHERE logical_agent_id = ?1 AND state = 'ready'
         ORDER BY created_at_ms ASC",
    )?;
    let rows = statement.query_map([logical_agent_id.to_string()], |row| {
        row.get::<_, String>(0)
    })?;
    let mut ready = Vec::new();
    for row in rows {
        ready.push(row?);
    }
    drop(statement);
    match ready.as_slice() {
        [incarnation] => IncarnationId::parse(incarnation)
            .ok_or_else(|| {
                StoreError::InvalidRecord(format!("invalid incarnation id {incarnation}"))
            })
            .map(Some),
        [] => Ok(None),
        [_, _, ..] => Err(StoreError::Conflict(conflict.into())),
    }
}

#[allow(clippy::too_many_arguments)]
fn insert_cancellation_prompt(
    tx: &Transaction<'_>,
    message_id: MessageId,
    ask_message_id: MessageId,
    reason: &str,
    recipient_incarnation: IncarnationId,
    audience: CancellationAudience,
    due_at_ms: Option<i64>,
    now: i64,
) -> Result<OperationId, StoreError> {
    let operation_id = OperationId::new();
    let audience_label = match audience {
        CancellationAudience::Waiting => "waiting",
        CancellationAudience::Owing => "owing",
    };
    let intent = serde_json::json!({
        "message_id": message_id,
        "cancelled_ask": ask_message_id,
        "reason": reason,
        "audience": audience_label,
        "recipient_incarnation_id": recipient_incarnation
    });
    let idempotency = match audience {
        CancellationAudience::Waiting => format!("kelpie:cancellation:{ask_message_id}"),
        CancellationAudience::Owing => format!("kelpie:owing-cancellation:{ask_message_id}"),
    };
    tx.execute(
        "INSERT INTO operations
         (id, idempotency_key, kind, target_incarnation_id, intent_json,
          created_at_ms, outcome)
         VALUES (?1, ?2, 'prompt', ?3, ?4, ?5, 'pending')",
        params![
            operation_id.to_string(),
            idempotency,
            recipient_incarnation.to_string(),
            intent.to_string(),
            now
        ],
    )
    .map_err(map_constraint)?;
    tx.execute(
        "INSERT INTO deliveries
         (message_id, recipient_incarnation_id, attempt_number,
          scheduled_at_ms, outcome, operation_id)
         VALUES (?1, ?2, 1, ?3, ?4, ?5)",
        params![
            message_id.to_string(),
            recipient_incarnation.to_string(),
            due_at_ms.unwrap_or(now),
            if due_at_ms.is_some() {
                "queued"
            } else {
                "pending"
            },
            operation_id.to_string()
        ],
    )?;
    Ok(operation_id)
}

#[allow(clippy::too_many_arguments)]
fn record_cancellation_side(
    tx: &Transaction<'_>,
    recipient_agent: LogicalAgentId,
    ask_message_id: MessageId,
    reason: &str,
    body: &str,
    audience: CancellationAudience,
    due_at_ms: Option<i64>,
    now: i64,
    ambiguous_ready: &str,
) -> Result<(MessageId, Option<(OperationId, IncarnationId)>), StoreError> {
    let message_id = MessageId::new();
    tx.execute(
        "INSERT INTO messages
         (id, sender_agent_id, recipient_agent_id, kind, body, created_at_ms,
          creates_obligation)
         VALUES (?1, NULL, ?2, 'cancellation', ?3, ?4, 0)",
        params![
            message_id.to_string(),
            recipient_agent.to_string(),
            body,
            now
        ],
    )?;
    let identity = waiter_identity(tx, recipient_agent)?;
    let delivery = match identity.transport {
        DeliveryTransport::SocketInbox => {
            if !identity.ended {
                queue_socket_inbox_delivery(tx, message_id, recipient_agent, now)?;
            }
            None
        }
        DeliveryTransport::HerdrPrompt => {
            match optional_ready_incarnation(tx, recipient_agent, ambiguous_ready)? {
                Some(recipient_incarnation) => {
                    let operation_id = insert_cancellation_prompt(
                        tx,
                        message_id,
                        ask_message_id,
                        reason,
                        recipient_incarnation,
                        audience,
                        due_at_ms,
                        now,
                    )?;
                    Some((operation_id, recipient_incarnation))
                }
                None => None,
            }
        }
    };
    Ok((message_id, delivery))
}

fn apply_reply_obligation_activity(
    tx: &Transaction<'_>,
    reply_to: MessageId,
    disposition: ReplyDisposition,
    now: i64,
) -> Result<(), StoreError> {
    match disposition {
        ReplyDisposition::Progress => {
            tx.execute(
                "UPDATE obligations SET state = 'in_progress', last_activity_at_ms = ?1
                 WHERE ask_message_id = ?2",
                params![now, reply_to.to_string()],
            )?;
            refresh_reminder_activity(tx, reply_to, now)?;
        }
        ReplyDisposition::Final => {
            tx.execute(
                "UPDATE obligations SET last_activity_at_ms = ?1
                 WHERE ask_message_id = ?2 AND state IN ('open', 'in_progress')",
                params![now, reply_to.to_string()],
            )?;
            refresh_reminder_activity(tx, reply_to, now)?;
        }
    }
    Ok(())
}

fn resolve_socket_inbox_final_reply(
    tx: &Transaction<'_>,
    message_id: MessageId,
    now: i64,
) -> Result<(), StoreError> {
    let final_reply: Option<(String, String)> = tx
        .query_row(
            "SELECT reply_to_message_id, id FROM messages
             WHERE id = ?1 AND kind = 'reply' AND disposition = 'final'",
            [message_id.to_string()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let Some((ask_message_id, resolving_message_id)) = final_reply else {
        return Ok(());
    };
    tx.execute(
        "UPDATE obligations SET state = 'resolved', last_activity_at_ms = ?1,
         resolving_message_id = ?2
         WHERE ask_message_id = ?3 AND state IN ('open', 'in_progress')",
        params![now, resolving_message_id, ask_message_id],
    )?;
    Ok(())
}

struct DeliverySchedule {
    scheduled_at_ms: i64,
    outcome: &'static str,
}

fn delivery_schedule(now_ms: i64, due_at_ms: Option<i64>) -> Result<DeliverySchedule, StoreError> {
    match due_at_ms {
        None => Ok(DeliverySchedule {
            scheduled_at_ms: now_ms,
            outcome: "pending",
        }),
        Some(due_at_ms) if due_at_ms < 0 => Err(StoreError::InvalidRecord(
            "due_at_ms must not be negative".into(),
        )),
        Some(due_at_ms) => Ok(DeliverySchedule {
            scheduled_at_ms: due_at_ms,
            outcome: "queued",
        }),
    }
}

fn parse_message_kind(value: &str) -> Result<MessageKind, StoreError> {
    match value {
        "tell" => Ok(MessageKind::Tell),
        "ask" => Ok(MessageKind::Ask),
        "reply" => Ok(MessageKind::Reply),
        "cancellation" => Ok(MessageKind::Cancellation),
        other => Err(StoreError::InvalidRecord(format!(
            "unknown message kind {other}"
        ))),
    }
}

fn now_millis() -> Result<i64, StoreError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| {
            StoreError::InvalidRecord(format!("system clock precedes Unix epoch: {error}"))
        })?;
    i64::try_from(duration.as_millis())
        .map_err(|_| StoreError::InvalidRecord("timestamp exceeds SQLite integer".into()))
}

fn invalid_json(error: &serde_json::Error) -> StoreError {
    StoreError::InvalidRecord(error.to_string())
}

fn map_constraint(error: rusqlite::Error) -> StoreError {
    if matches!(error, rusqlite::Error::SqliteFailure(ref inner, _) if inner.extended_code == rusqlite::ffi::SQLITE_CONSTRAINT_UNIQUE)
    {
        StoreError::Conflict("idempotency key already exists".into())
    } else {
        StoreError::Sql(error)
    }
}

fn disposition_name(value: ReplyDisposition) -> &'static str {
    match value {
        ReplyDisposition::Progress => "progress",
        ReplyDisposition::Final => "final",
    }
}

fn parse_reply_disposition(value: &str) -> Result<ReplyDisposition, StoreError> {
    match value {
        "progress" => Ok(ReplyDisposition::Progress),
        "final" => Ok(ReplyDisposition::Final),
        other => Err(StoreError::InvalidRecord(format!(
            "unknown reply disposition {other}"
        ))),
    }
}

fn delivery_outcome_name(value: DeliveryOutcome) -> &'static str {
    match value {
        DeliveryOutcome::Pending => "pending",
        DeliveryOutcome::Submitted => "submitted",
        DeliveryOutcome::Accepted => "accepted",
        DeliveryOutcome::Queued => "queued",
        DeliveryOutcome::Unknown => "unknown",
        DeliveryOutcome::Rejected => "rejected",
        DeliveryOutcome::TargetUnavailable => "target_unavailable",
        DeliveryOutcome::Superseded => "superseded",
    }
}

fn parse_delivery_outcome(value: &str) -> Result<DeliveryOutcome, StoreError> {
    match value {
        "pending" => Ok(DeliveryOutcome::Pending),
        "submitted" => Ok(DeliveryOutcome::Submitted),
        "accepted" => Ok(DeliveryOutcome::Accepted),
        "queued" => Ok(DeliveryOutcome::Queued),
        "unknown" => Ok(DeliveryOutcome::Unknown),
        "rejected" => Ok(DeliveryOutcome::Rejected),
        "target_unavailable" => Ok(DeliveryOutcome::TargetUnavailable),
        "superseded" => Ok(DeliveryOutcome::Superseded),
        other => Err(StoreError::InvalidRecord(format!(
            "unknown delivery outcome {other}"
        ))),
    }
}

fn parse_operation_outcome(value: &str) -> Result<OperationOutcome, StoreError> {
    match value {
        "pending" => Ok(OperationOutcome::Pending),
        "accepted" => Ok(OperationOutcome::Accepted),
        "succeeded" => Ok(OperationOutcome::Succeeded),
        "failed" => Ok(OperationOutcome::Failed),
        "superseded" => Ok(OperationOutcome::Superseded),
        "unknown" => Ok(OperationOutcome::Unknown),
        other => Err(StoreError::InvalidRecord(format!(
            "unknown operation outcome {other}"
        ))),
    }
}

fn parse_obligation_state(value: &str) -> Result<ObligationState, StoreError> {
    match value {
        "open" => Ok(ObligationState::Open),
        "in_progress" => Ok(ObligationState::InProgress),
        "resolved" => Ok(ObligationState::Resolved),
        "cancelled" => Ok(ObligationState::Cancelled),
        "orphaned" => Ok(ObligationState::Orphaned),
        other => Err(StoreError::InvalidRecord(format!(
            "unknown obligation state {other}"
        ))),
    }
}

fn parse_incarnation_state(value: &str) -> Result<crate::domain::IncarnationState, StoreError> {
    use crate::domain::IncarnationState;
    match value {
        "declared" => Ok(IncarnationState::Declared),
        "starting" => Ok(IncarnationState::Starting),
        "ready" => Ok(IncarnationState::Ready),
        "failed" => Ok(IncarnationState::Failed),
        "unknown" => Ok(IncarnationState::Unknown),
        "retiring" => Ok(IncarnationState::Retiring),
        "retired" => Ok(IncarnationState::Retired),
        "lost" => Ok(IncarnationState::Lost),
        "superseded" => Ok(IncarnationState::Superseded),
        other => Err(StoreError::InvalidRecord(format!(
            "unknown incarnation state {other}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn intent(name: &str, terminal: &str, key: &str) -> StartIntent {
        StartIntent {
            public_name: name.into(),
            logical_agent_id: None,
            parent: Parent::Parentless,
            herdr_session: "test".into(),
            pane_id: "w1:p1".into(),
            expected_terminal_id: terminal.into(),
            backend_kind: "codex".into(),
            backend_args: vec![],
            initial_message: crate::domain::InitialMessageIntent {
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

    fn mark_ready(store: &mut Store, declared: DeclaredStart, name: &str, terminal: &str) {
        store
            .begin_attempt(
                declared.operation_id,
                declared.incarnation_id,
                "ready-request",
            )
            .expect("attempt");
        store
            .accept_start_submission(
                declared.operation_id,
                declared.incarnation_id,
                "w1:p1",
                terminal,
            )
            .expect("submission");
        store
            .accept_start_ready(
                declared.operation_id,
                declared.incarnation_id,
                &crate::herdr::AgentObservation {
                    terminal_id: terminal.into(),
                    pane_id: "w1:p1".into(),
                    name: Some(name.into()),
                    agent: Some("codex".into()),
                    interactive_ready: true,
                    launch_pending: false,
                    agent_session: None,
                },
                None,
            )
            .expect("ready");
    }

    #[test]
    fn requested_is_not_observed_and_start_ready_persists_native_session() {
        let directory = tempfile::tempdir().expect("tempdir");
        let mut store = Store::open(directory.path().join("kelpie.sqlite3")).expect("store");
        let mut intent = intent("worker", "term-1", "start-attr");
        intent.requested_model = Some("requested-model".into());
        intent.requested_provider = Some("requested-provider".into());
        let declared = store.declare_start(&intent).expect("declare");
        store
            .begin_attempt(declared.operation_id, declared.incarnation_id, "req")
            .expect("attempt");
        store
            .accept_start_submission(
                declared.operation_id,
                declared.incarnation_id,
                "w1:p1",
                "term-1",
            )
            .expect("accepted");
        let session = serde_json::json!({"agent":"codex","kind":"id","value":"sess-1"});
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
                    agent_session: Some(session.clone()),
                },
                None,
            )
            .expect("ready");
        assert_eq!(
            store
                .observed_native_session(declared.incarnation_id)
                .expect("session"),
            Some(session.clone())
        );
        let requested = store
            .requested_attribution(declared.incarnation_id)
            .expect("requested");
        assert_eq!(requested.model.as_deref(), Some("requested-model"));
        assert!(
            store
                .latest_observed_attribution(declared.incarnation_id)
                .expect("no row yet")
                .is_none()
        );
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
        let observed = store
            .latest_observed_attribution(declared.incarnation_id)
            .expect("latest")
            .expect("row");
        assert_eq!(
            observed.model,
            crate::attribution::ObservedField::Undetermined
        );
        assert_ne!(requested.model.as_deref(), Some(""));
        let requested_json = serde_json::to_value(&requested).expect("req json");
        let observed_json = serde_json::to_value(&observed.model).expect("obs json");
        assert_ne!(requested_json["model"], observed_json);
        store
            .record_observed_attribution(
                declared.incarnation_id,
                Some(&session),
                &crate::attribution::observe(
                    "codex",
                    Some(&session),
                    &crate::attribution::SessionRoots::default(),
                ),
            )
            .expect("second");
        assert_eq!(
            store
                .connection
                .query_row(
                    "SELECT COUNT(*) FROM observed_attributions WHERE incarnation_id = ?1",
                    [declared.incarnation_id.to_string()],
                    |row| row.get::<_, i64>(0),
                )
                .expect("count"),
            2
        );
    }

    #[test]
    fn report_exposes_lineage_incarnations_and_obligations_without_judging_them() {
        let directory = tempfile::tempdir().expect("tempdir");
        let mut store = Store::open(directory.path().join("kelpie.sqlite3")).expect("store");

        let mut parent_intent = intent("coordinator", "term-p", "report-parent");
        parent_intent.requested_model = Some("openai/gpt-5.6-sol".into());
        parent_intent.backend_args = vec!["--model".into(), "openai/gpt-5.6-sol".into()];
        let parent = store.declare_start(&parent_intent).expect("parent");
        mark_ready(&mut store, parent, "coordinator", "term-p");

        let mut child_intent = intent("worker", "term-c", "report-child");
        child_intent.parent = Parent::Agent(parent.logical_agent_id);
        child_intent.expected_terminal_id = "term-c".into();
        let child = store.declare_start(&child_intent).expect("child");
        mark_ready(&mut store, child, "worker", "term-c");

        // A second incarnation of the child, so newest-first ordering matters.
        let mut continued = intent("worker", "term-c2", "report-child-2");
        continued.logical_agent_id = Some(child.logical_agent_id);
        continued.parent = Parent::Agent(parent.logical_agent_id);
        continued.expected_terminal_id = "term-c2".into();
        let newer = store.declare_start(&continued).expect("continue");

        let ask = store
            .create_ask(
                parent.logical_agent_id,
                child.logical_agent_id,
                child.incarnation_id,
                "please review",
                "report-ask",
            )
            .expect("ask");

        let report = store.report().expect("report");
        assert!(report.generated_at_ms > 0);

        let parent_row = report
            .agents
            .iter()
            .find(|agent| agent.id == parent.logical_agent_id)
            .expect("parent in report");
        assert_eq!(parent_row.public_name, "coordinator");
        assert!(parent_row.explicitly_parentless);
        assert_eq!(parent_row.parent_agent_id, None);
        // A stranded runtime must be joinable to the operation that produced it:
        // a caller that lost its receipt, or restarted, has no other handle.
        let (operation_id, kind, _) = parent_row.incarnations[0]
            .latest_operation
            .as_ref()
            .expect("incarnation reports its latest operation");
        assert_eq!(*operation_id, parent.operation_id);
        assert_eq!(kind, "start");
        // Launch intent is reported as intent, never as observed evidence.
        assert_eq!(
            parent_row.incarnations[0].requested.model.as_deref(),
            Some("openai/gpt-5.6-sol")
        );
        assert_eq!(
            parent_row.incarnations[0].requested_backend_args,
            vec!["--model".to_string(), "openai/gpt-5.6-sol".to_string()]
        );

        let child_row = report
            .agents
            .iter()
            .find(|agent| agent.id == child.logical_agent_id)
            .expect("child in report");
        assert_eq!(child_row.parent_agent_id, Some(parent.logical_agent_id));
        assert!(!child_row.explicitly_parentless);
        assert_eq!(child_row.incarnations.len(), 2);
        // Newest first, so one row per agent renders the current incarnation.
        assert_eq!(child_row.incarnations[0].id, newer.incarnation_id);

        let obligation = report
            .obligations
            .iter()
            .find(|obligation| obligation.ask_message_id == ask.message_id)
            .expect("obligation edge");
        assert_eq!(obligation.owing_agent_id, child.logical_agent_id);
        assert_eq!(obligation.waiting_agent_id, parent.logical_agent_id);
        assert_eq!(obligation.state, ObligationState::Open);
        assert!(obligation.resolving_message_id.is_none());
    }

    #[test]
    fn rename_keeps_the_incarnation_and_fails_closed_on_a_taken_name() {
        let directory = tempfile::tempdir().expect("tempdir");
        let mut store = Store::open(directory.path().join("kelpie.sqlite3")).expect("store");
        let declared = store
            .declare_start(&intent("old-name", "term-1", "rename-1"))
            .expect("declare");
        mark_ready(&mut store, declared, "old-name", "term-1");

        store
            .declare_rename(declared.incarnation_id, "new-name")
            .expect("declare rename");
        // Nothing is visible until Herdr confirms; the committed name still holds.
        assert_eq!(
            store
                .agent_address(declared.logical_agent_id)
                .expect("name"),
            "old-name"
        );
        store
            .commit_rename(declared.incarnation_id, "new-name")
            .expect("commit");
        assert_eq!(
            store
                .agent_address(declared.logical_agent_id)
                .expect("name"),
            "new-name"
        );
        // A rename binds no new runtime, so it must not mint an incarnation.
        let report = store.report().expect("report");
        let agent = report
            .agents
            .iter()
            .find(|agent| agent.id == declared.logical_agent_id)
            .expect("agent");
        assert_eq!(agent.incarnations.len(), 1);
        assert_eq!(agent.incarnations[0].id, declared.incarnation_id);
        assert_eq!(
            agent.incarnations[0].state,
            crate::domain::IncarnationState::Ready
        );

        // A name another Ready agent holds is refused before any Herdr call.
        let other = store
            .declare_start(&intent("taken", "term-2", "rename-2"))
            .expect("other");
        mark_ready(&mut store, other, "taken", "term-2");
        let error = store
            .declare_rename(declared.incarnation_id, "taken")
            .expect_err("alias is taken");
        assert!(matches!(error, StoreError::Conflict(_)));
    }

    #[test]
    fn recovery_settles_a_rename_that_reached_herdr_but_not_the_commit() {
        let directory = tempfile::tempdir().expect("tempdir");
        let mut store = Store::open(directory.path().join("kelpie.sqlite3")).expect("store");
        let declared = store
            .declare_start(&intent("old-name", "term-1", "rename-crash"))
            .expect("declare");
        mark_ready(&mut store, declared, "old-name", "term-1");
        store
            .declare_rename(declared.incarnation_id, "new-name")
            .expect("declare rename");

        // Herdr took the rename; Kelpie died before committing it. The live
        // agent answers to the target, so this binding is exact, not absent.
        let snapshot = Snapshot {
            protocol: 20,
            panes: vec![],
            agents: vec![crate::herdr::AgentObservation {
                terminal_id: "term-1".into(),
                pane_id: "w1:p1".into(),
                name: Some("new-name".into()),
                agent: Some("codex".into()),
                interactive_ready: true,
                launch_pending: false,
                agent_session: None,
            }],
        };
        let report = store.reconcile(&snapshot).expect("reconcile");
        assert_eq!(report.incarnations_marked_lost, 0);
        assert_eq!(
            store
                .agent_address(declared.logical_agent_id)
                .expect("name"),
            "new-name"
        );
        assert_eq!(
            store
                .incarnation_state(declared.incarnation_id)
                .expect("state"),
            crate::domain::IncarnationState::Ready
        );
    }

    #[test]
    fn recovery_drops_a_rename_herdr_never_applied() {
        let directory = tempfile::tempdir().expect("tempdir");
        let mut store = Store::open(directory.path().join("kelpie.sqlite3")).expect("store");
        let declared = store
            .declare_start(&intent("old-name", "term-1", "rename-noop"))
            .expect("declare");
        mark_ready(&mut store, declared, "old-name", "term-1");
        store
            .declare_rename(declared.incarnation_id, "new-name")
            .expect("declare rename");

        // The live agent still answers to the committed name, so the rename
        // never took effect and the intent is discarded rather than applied.
        let snapshot = Snapshot {
            protocol: 20,
            panes: vec![],
            agents: vec![crate::herdr::AgentObservation {
                terminal_id: "term-1".into(),
                pane_id: "w1:p1".into(),
                name: Some("old-name".into()),
                agent: Some("codex".into()),
                interactive_ready: true,
                launch_pending: false,
                agent_session: None,
            }],
        };
        store.reconcile(&snapshot).expect("reconcile");
        assert_eq!(
            store
                .agent_address(declared.logical_agent_id)
                .expect("name"),
            "old-name"
        );
        // The intent is cleared, so a later rename can be declared.
        store
            .declare_rename(declared.incarnation_id, "new-name")
            .expect("rename can be declared again");
    }

    #[test]
    fn report_shows_agents_sharing_one_public_name() {
        let directory = tempfile::tempdir().expect("tempdir");
        let mut store = Store::open(directory.path().join("kelpie.sqlite3")).expect("store");
        // Two distinct logical agents under one alias is a real fleet mistake,
        // so the report must show both rather than collapse them.
        let first = store
            .declare_start(&intent("reviewer", "term-1", "dup-1"))
            .expect("first");
        let second = store
            .declare_start(&intent("reviewer", "term-2", "dup-2"))
            .expect("second");
        assert_ne!(first.logical_agent_id, second.logical_agent_id);

        let report = store.report().expect("report");
        let sharing: Vec<_> = report
            .agents
            .iter()
            .filter(|agent| agent.public_name == "reviewer")
            .collect();
        assert_eq!(sharing.len(), 2);
    }

    #[test]
    fn retryable_rejection_keeps_the_operation_pending_for_a_new_attempt() {
        let directory = tempfile::tempdir().expect("tempdir");
        let mut store = Store::open(directory.path().join("kelpie.sqlite3")).expect("store");
        let declared = store
            .declare_start(&intent("worker", "term-1", "busy-retry"))
            .expect("declare");
        let first = store
            .begin_attempt(declared.operation_id, declared.incarnation_id, "req-1")
            .expect("first attempt");
        store
            .mark_submitted(declared.operation_id, first, "req-1")
            .expect("submitted");
        store
            .reject_attempt(
                declared.operation_id,
                declared.incarnation_id,
                "agent_pane_busy",
            )
            .expect("retryable rejection");

        // The operation stays pending, so a further attempt is legal.
        assert_eq!(
            store
                .operation_outcome(declared.operation_id)
                .expect("outcome"),
            OperationOutcome::Pending
        );
        let second = store
            .begin_attempt(declared.operation_id, declared.incarnation_id, "req-2")
            .expect("second attempt");
        assert_eq!(second, 2);

        // The refused attempt keeps its own honest evidence.
        let (phase, evidence): (String, Option<String>) = store
            .connection
            .query_row(
                "SELECT phase, evidence_json FROM operation_attempts
                 WHERE operation_id = ?1 AND attempt_number = 1",
                [declared.operation_id.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("first attempt row");
        assert_eq!(phase, "rejected");
        assert!(evidence.expect("evidence").contains("agent_pane_busy"));

        // A decisive rejection still ends the operation.
        store
            .mark_rejected(
                declared.operation_id,
                declared.incarnation_id,
                "gave up",
                DeliveryOutcome::Rejected,
            )
            .expect("final rejection");
        assert!(
            store
                .begin_attempt(declared.operation_id, declared.incarnation_id, "req-3")
                .is_err()
        );
    }

    #[test]
    fn attribution_evidence_separates_no_data_undetermined_and_absent() {
        let directory = tempfile::tempdir().expect("tempdir");
        let mut store = Store::open(directory.path().join("kelpie.sqlite3")).expect("store");
        let mut intent = intent("worker", "term-1", "evidence-1");
        intent.requested_model = Some("requested-model".into());
        intent.requested_effort = Some("high".into());
        let declared = store.declare_start(&intent).expect("declare");

        // No adapter has reported yet: an empty history, never Undetermined.
        let evidence = store
            .attribution_evidence(declared.incarnation_id)
            .expect("evidence");
        assert!(evidence.observations.is_empty());
        assert!(evidence.latest().is_none());
        assert_eq!(evidence.requested.model.as_deref(), Some("requested-model"));
        assert_eq!(evidence.backend_kind, "codex");
        assert_eq!(evidence.public_name, "worker");
        assert_eq!(evidence.logical_agent_id, declared.logical_agent_id);

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
            .expect("observe grok");

        // An adapter reported Undetermined: present in history, distinct from none.
        let evidence = store
            .attribution_evidence(declared.incarnation_id)
            .expect("evidence");
        assert_eq!(evidence.observations.len(), 1);
        let latest = evidence.latest().expect("latest");
        assert_eq!(latest.observed.adapter, "grok");
        assert_eq!(
            latest.observed.model,
            crate::attribution::ObservedField::Undetermined
        );

        // Requested must never be mistaken for observed evidence.
        assert_ne!(
            serde_json::to_value(&evidence.requested).expect("req")["model"],
            serde_json::to_value(&latest.observed.model).expect("obs")
        );

        // An absent incarnation is a conflict, not an empty answer.
        let absent = store
            .attribution_evidence(IncarnationId::new())
            .expect_err("absent incarnation");
        assert!(matches!(absent, StoreError::Conflict(_)));
    }

    #[test]
    fn attribution_history_is_append_only_and_oldest_first() {
        let directory = tempfile::tempdir().expect("tempdir");
        let mut store = Store::open(directory.path().join("kelpie.sqlite3")).expect("store");
        let declared = store
            .declare_start(&intent("worker", "term-1", "evidence-2"))
            .expect("declare");
        let roots = crate::attribution::SessionRoots::default();
        for adapter in ["grok", "codex", "claude"] {
            store
                .record_observed_attribution(
                    declared.incarnation_id,
                    None,
                    &crate::attribution::observe(adapter, None, &roots),
                )
                .expect("observe");
        }
        let evidence = store
            .attribution_evidence(declared.incarnation_id)
            .expect("evidence");
        let adapters: Vec<&str> = evidence
            .observations
            .iter()
            .map(|recorded| recorded.observed.adapter.as_str())
            .collect();
        assert_eq!(adapters, ["grok", "codex", "claude"]);
        assert_eq!(
            evidence.latest().expect("latest").observed.adapter,
            "claude"
        );
    }

    #[test]
    fn newest_incarnation_resolves_and_fails_closed() {
        let directory = tempfile::tempdir().expect("tempdir");
        let mut store = Store::open(directory.path().join("kelpie.sqlite3")).expect("store");
        let first = store
            .declare_start(&intent("worker", "term-1", "newest-1"))
            .expect("declare");
        let mut continued = intent("worker", "term-2", "newest-2");
        continued.logical_agent_id = Some(first.logical_agent_id);
        let second = store.declare_start(&continued).expect("continue");
        assert_ne!(first.incarnation_id, second.incarnation_id);
        assert_eq!(
            store
                .newest_incarnation_for_agent(first.logical_agent_id)
                .expect("newest"),
            second.incarnation_id
        );
        let absent = store
            .newest_incarnation_for_agent(LogicalAgentId::new())
            .expect_err("absent agent");
        assert!(matches!(absent, StoreError::Conflict(_)));
    }

    fn deliver_reply(
        store: &mut Store,
        reply_to: MessageId,
        requester: LogicalAgentId,
        body: &str,
        disposition: ReplyDisposition,
        key: &str,
        waiting_terminal: &str,
    ) -> CreatedReply {
        let created = store
            .create_reply(reply_to, requester, body, disposition, key)
            .expect("create reply");
        let operation_id = created.operation_id.expect("pane reply operation");
        let recipient_incarnation = created
            .recipient_incarnation
            .expect("pane reply incarnation");
        store
            .begin_attempt(
                operation_id,
                recipient_incarnation,
                &format!("reply-request-{key}"),
            )
            .expect("reply attempt");
        store
            .mark_submitted(operation_id, 1, &format!("reply-request-{key}"))
            .expect("reply submitted");
        store
            .accept_delivery(
                operation_id,
                recipient_incarnation,
                "w1:p1",
                waiting_terminal,
            )
            .expect("reply accepted");
        created
    }

    fn observed_agent(terminal: &str) -> crate::herdr::AgentObservation {
        crate::herdr::AgentObservation {
            terminal_id: terminal.into(),
            pane_id: "w1:p1".into(),
            name: Some("worker".into()),
            agent: Some("codex".into()),
            interactive_ready: true,
            launch_pending: false,
            agent_session: None,
        }
    }

    #[test]
    fn durable_store_rejects_repository_paths() {
        let result = Store::open("kelpie-state.sqlite3");
        assert!(matches!(result, Err(StoreError::UnsafeLocation(_))));
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn version_five_store_migrates_operations_to_adopt_kind() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("v5.sqlite3");
        let agent_id = LogicalAgentId::new();
        let incarnation_id = IncarnationId::new();
        let operation_id = OperationId::new();
        {
            let connection = Connection::open(&path).expect("db");
            for migration in [
                include_str!("../migrations/001_initial.sql"),
                include_str!("../migrations/002_operator_notices.sql"),
                include_str!("../migrations/003_operator_message_sender.sql"),
                include_str!("../migrations/004_obligation_creation_sequence.sql"),
                include_str!("../migrations/005_obligation_cancellation.sql"),
            ] {
                connection.execute_batch(migration).expect("migrate step");
            }
            connection
                .execute(
                    "INSERT INTO logical_agents
                     (id, public_name, explicitly_parentless, created_at_ms)
                     VALUES (?1, 'worker', 1, 1)",
                    [agent_id.to_string()],
                )
                .expect("agent");
            connection
                .execute(
                    "INSERT INTO incarnations (
                        id, logical_agent_id, herdr_session, intended_pane_id,
                        expected_terminal_id, backend_kind, backend_args_json,
                        working_directory, created_at_ms, state
                     ) VALUES (?1, ?2, 's', 'w1:p1', 't1', 'codex', '[]', '/tmp', 1, 'ready')",
                    params![incarnation_id.to_string(), agent_id.to_string()],
                )
                .expect("incarnation");
            connection
                .execute(
                    "INSERT INTO operations (
                        id, idempotency_key, kind, target_incarnation_id, intent_json,
                        created_at_ms, outcome
                     ) VALUES (?1, 'k', 'start', ?2, '{}', 1, 'succeeded')",
                    params![operation_id.to_string(), incarnation_id.to_string()],
                )
                .expect("operation");
            connection
                .execute(
                    "INSERT INTO operation_attempts (
                        operation_id, attempt_number, request_id, started_at_ms, phase
                     ) VALUES (?1, 1, 'req', 1, 'response_committed')",
                    [operation_id.to_string()],
                )
                .expect("attempt");
        }
        let mut store = Store::open(&path).expect("open migrates to v9");
        let version: i64 = store
            .connection
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .expect("version");
        assert_eq!(version, SCHEMA_VERSION);
        let kind: String = store
            .connection
            .query_row(
                "SELECT kind FROM operations WHERE id = ?1",
                [operation_id.to_string()],
                |row| row.get(0),
            )
            .expect("kind");
        assert_eq!(kind, "start");
        let attempts: i64 = store
            .connection
            .query_row(
                "SELECT COUNT(*) FROM operation_attempts WHERE operation_id = ?1",
                [operation_id.to_string()],
                |row| row.get(0),
            )
            .expect("attempts");
        assert_eq!(attempts, 1);
        store
            .declare_adopt(
                &crate::domain::AdoptIntent {
                    pane_id: "w9:p9".into(),
                    expected_terminal_id: "t9".into(),
                    public_name: Some("adopted".into()),
                    logical_agent_id: None,
                    parent: Parent::Parentless,
                    herdr_session: "s".into(),
                    backend_kind: Some("grok".into()),
                    backend_args: Vec::new(),
                    requested_model: None,
                    requested_provider: None,
                    requested_effort: None,
                    idempotency_key: "adopt-after-migrate".into(),
                },
                &AdoptEvidence {
                    pane_id: "w9:p9".into(),
                    terminal_id: "t9".into(),
                    public_name: "adopted".into(),
                    backend_kind: "grok".into(),
                    working_directory: "/tmp".into(),
                    interactive_ready: true,
                    launch_pending: false,
                    native_agent_session: None,
                },
            )
            .expect("adopt works after migrate");
    }

    #[test]
    fn version_three_store_backfills_deterministic_obligation_order() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("legacy.sqlite3");
        let agent_id = LogicalAgentId::new();
        let message_id = MessageId::new();
        let second_message_id = MessageId::new();
        {
            let connection = Connection::open(&path).expect("legacy database");
            connection
                .execute_batch(include_str!("../migrations/001_initial.sql"))
                .expect("version one");
            connection
                .execute_batch(include_str!("../migrations/002_operator_notices.sql"))
                .expect("version two");
            connection
                .execute_batch(include_str!(
                    "../migrations/003_operator_message_sender.sql"
                ))
                .expect("version three");
            connection
                .execute(
                    "INSERT INTO logical_agents
                     (id, public_name, explicitly_parentless, created_at_ms)
                     VALUES (?1, 'legacy', 1, 1)",
                    [agent_id.to_string()],
                )
                .expect("legacy agent");
            connection
                .execute(
                    "INSERT INTO messages
                     (id, sender_agent_id, recipient_agent_id, kind, body,
                      created_at_ms, creates_obligation)
                     VALUES (?1, ?2, ?2, 'tell', 'legacy body', 1, 0)",
                    params![message_id.to_string(), agent_id.to_string()],
                )
                .expect("legacy message");
            connection
                .execute(
                    "INSERT INTO messages
                     (id, sender_agent_id, recipient_agent_id, kind, body,
                      created_at_ms, creates_obligation)
                     VALUES (?1, ?2, ?2, 'ask', 'second legacy body', 1, 1)",
                    params![second_message_id.to_string(), agent_id.to_string()],
                )
                .expect("second legacy message");
            connection
                .execute(
                    "UPDATE messages SET kind = 'ask', creates_obligation = 1 WHERE id = ?1",
                    [message_id.to_string()],
                )
                .expect("first legacy ask");
            for ask_id in [second_message_id, message_id] {
                connection
                    .execute(
                        "INSERT INTO obligations
                         (ask_message_id, owing_agent_id, waiting_agent_id,
                          created_at_ms, last_activity_at_ms, state)
                         VALUES (?1, ?2, ?2, 1, 1, 'open')",
                        params![ask_id.to_string(), agent_id.to_string()],
                    )
                    .expect("legacy obligation");
            }
        }
        let store = Store::open(&path).expect("migrated store");
        let version: i64 = store
            .connection
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .expect("schema version");
        assert_eq!(version, SCHEMA_VERSION);
        let body: String = store
            .connection
            .query_row(
                "SELECT body FROM messages WHERE id = ?1",
                [message_id.to_string()],
                |row| row.get(0),
            )
            .expect("preserved message");
        assert_eq!(body, "legacy body");
        let mut expected = vec![message_id.to_string(), second_message_id.to_string()];
        expected.sort();
        let backfilled: Vec<String> = {
            let mut statement = store
                .connection
                .prepare("SELECT ask_message_id FROM obligations ORDER BY creation_sequence")
                .expect("statement");
            statement
                .query_map([], |row| row.get(0))
                .expect("obligations")
                .collect::<Result<_, _>>()
                .expect("obligation rows")
        };
        assert_eq!(backfilled, expected);
        let violations: i64 = store
            .connection
            .query_row("SELECT COUNT(*) FROM pragma_foreign_key_check", [], |row| {
                row.get(0)
            })
            .expect("foreign key check");
        assert_eq!(violations, 0);
    }

    #[test]
    fn exact_start_result_cannot_mutate_replacement() {
        let mut store = Store::in_memory().expect("store");
        let old = store
            .declare_start(&intent("worker", "term-old", "start-old"))
            .expect("old intent");
        store
            .begin_attempt(old.operation_id, old.incarnation_id, "request-old")
            .expect("old attempt");
        let new = store
            .declare_start(&intent("worker", "term-new", "start-new"))
            .expect("new intent");
        store
            .begin_attempt(new.operation_id, new.incarnation_id, "request-new")
            .expect("new attempt");
        let stale = store.accept_start_submission(
            new.operation_id,
            new.incarnation_id,
            "w1:p1",
            "term-old",
        );
        assert!(matches!(stale, Err(StoreError::Conflict(_))));
        assert_eq!(
            store
                .operation_outcome(new.operation_id)
                .expect("new outcome"),
            OperationOutcome::Pending
        );
    }

    #[test]
    fn raw_start_acceptance_remains_starting_until_authoritative_readiness() {
        let mut store = Store::in_memory().expect("store");
        let declared = store
            .declare_start(&intent("worker", "term-1", "accepted-start"))
            .expect("intent");
        store
            .begin_attempt(
                declared.operation_id,
                declared.incarnation_id,
                "accepted-request",
            )
            .expect("attempt");
        store
            .mark_submitted(declared.operation_id, 1, "accepted-request")
            .expect("submitted");
        store
            .accept_start_submission(
                declared.operation_id,
                declared.incarnation_id,
                "w1:p1",
                "term-1",
            )
            .expect("accepted");

        assert_eq!(
            store
                .operation_outcome(declared.operation_id)
                .expect("outcome"),
            OperationOutcome::Accepted
        );
        assert!(matches!(
            store.ready_binding(declared.incarnation_id),
            Err(StoreError::Conflict(_))
        ));
    }

    #[test]
    fn launch_pending_observation_cannot_mark_start_ready() {
        let mut store = Store::in_memory().expect("store");
        let declared = store
            .declare_start(&intent("worker", "term-1", "pending-launch"))
            .expect("intent");
        store
            .begin_attempt(declared.operation_id, declared.incarnation_id, "request")
            .expect("attempt");
        let result = store.accept_start_ready(
            declared.operation_id,
            declared.incarnation_id,
            &crate::herdr::AgentObservation {
                terminal_id: "term-1".into(),
                pane_id: "w1:p1".into(),
                name: Some("worker".into()),
                agent: Some("codex".into()),
                interactive_ready: true,
                launch_pending: true,
                agent_session: None,
            },
            None,
        );
        assert!(matches!(result, Err(StoreError::Conflict(_))));
        assert_eq!(
            store
                .operation_outcome(declared.operation_id)
                .expect("outcome"),
            OperationOutcome::Pending
        );
    }

    #[test]
    fn unknown_is_preserved_as_its_own_outcome() {
        let mut store = Store::in_memory().expect("store");
        let declared = store
            .declare_start(&intent("worker", "term-1", "start-1"))
            .expect("intent");
        store
            .begin_attempt(declared.operation_id, declared.incarnation_id, "request-1")
            .expect("attempt");
        store
            .mark_unknown(
                declared.operation_id,
                declared.incarnation_id,
                "disconnect after write",
            )
            .expect("unknown");
        assert_eq!(
            store
                .operation_outcome(declared.operation_id)
                .expect("outcome"),
            OperationOutcome::Unknown
        );
    }

    #[test]
    fn structured_rejection_is_failed_not_unknown() {
        let mut store = Store::in_memory().expect("store");
        let declared = store
            .declare_start(&intent("worker", "term-1", "rejected-start"))
            .expect("intent");
        store
            .begin_attempt(
                declared.operation_id,
                declared.incarnation_id,
                "rejected-request",
            )
            .expect("attempt");
        store
            .mark_submitted(declared.operation_id, 1, "rejected-request")
            .expect("submitted");
        store
            .mark_rejected(
                declared.operation_id,
                declared.incarnation_id,
                "agent_start_conflict",
                DeliveryOutcome::Rejected,
            )
            .expect("rejected");
        assert_eq!(
            store
                .operation_outcome(declared.operation_id)
                .expect("outcome"),
            OperationOutcome::Failed
        );
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn progress_does_not_resolve_and_wrong_reply_is_rejected() {
        let mut store = Store::in_memory().expect("store");
        let waiting = store
            .declare_start(&intent("waiting", "term-a", "start-a"))
            .expect("waiting");
        mark_ready(&mut store, waiting, "waiting", "term-a");
        let owing = store
            .declare_start(&intent("owing", "term-b", "start-b"))
            .expect("owing");
        let ask = store
            .create_ask(
                waiting.logical_agent_id,
                owing.logical_agent_id,
                owing.incarnation_id,
                "question",
                "ask-1",
            )
            .expect("ask");
        assert_eq!(
            store
                .pending_obligations(owing.logical_agent_id)
                .expect("open pending"),
            vec![PendingObligation {
                ask_message_id: ask.message_id,
                waiting_agent_id: waiting.logical_agent_id,
                state: ObligationState::Open,
            }]
        );
        let wrong_target = store.create_reply(
            MessageId::new(),
            owing.logical_agent_id,
            "wrong target",
            ReplyDisposition::Final,
            "wrong-target",
        );
        assert!(matches!(wrong_target, Err(StoreError::Conflict(_))));
        let progress = deliver_reply(
            &mut store,
            ask.message_id,
            owing.logical_agent_id,
            "working",
            ReplyDisposition::Progress,
            "progress-1",
            "term-a",
        );
        assert_eq!(
            store.obligation_state(ask.message_id).expect("state"),
            ObligationState::InProgress
        );
        assert_eq!(
            store
                .pending_obligations(owing.logical_agent_id)
                .expect("in-progress pending")[0]
                .state,
            ObligationState::InProgress
        );
        assert_eq!(
            store
                .delivery_outcome(progress.operation_id.expect("pane reply operation"))
                .expect("progress delivery"),
            DeliveryOutcome::Accepted
        );
        // Final is recorded but does not resolve until delivery is accepted.
        let pending_final = store
            .create_reply(
                ask.message_id,
                owing.logical_agent_id,
                "done",
                ReplyDisposition::Final,
                "final-pending",
            )
            .expect("create final");
        assert_eq!(
            store
                .obligation_state(ask.message_id)
                .expect("still open path"),
            ObligationState::InProgress
        );
        let pending_operation = pending_final.operation_id.expect("pane reply operation");
        let pending_incarnation = pending_final
            .recipient_incarnation
            .expect("pane reply incarnation");
        store
            .begin_attempt(pending_operation, pending_incarnation, "final-request")
            .expect("final attempt");
        store
            .mark_submitted(pending_operation, 1, "final-request")
            .expect("final submitted");
        store
            .accept_delivery(pending_operation, pending_incarnation, "w1:p1", "term-a")
            .expect("final accepted");
        let (reply_sender, reply_recipient) = store
            .message_parties(pending_final.message_id)
            .expect("reply parties");
        assert_eq!(reply_sender, owing.logical_agent_id);
        assert_eq!(reply_recipient, waiting.logical_agent_id);
        assert_eq!(
            store.obligation_state(ask.message_id).expect("state"),
            ObligationState::Resolved
        );
        assert!(
            store
                .pending_obligations(owing.logical_agent_id)
                .expect("resolved absent")
                .is_empty()
        );
    }

    #[test]
    fn explicit_initial_ask_creates_its_own_obligation_and_delivery() {
        let mut store = Store::in_memory().expect("store");
        let sender = store
            .declare_start(&intent("sender", "term-sender", "sender-start"))
            .expect("sender");
        let recipient = store
            .declare_start(&intent("worker", "term-1", "recipient-start"))
            .expect("recipient");
        store
            .begin_attempt(recipient.operation_id, recipient.incarnation_id, "request")
            .expect("attempt");
        store
            .accept_start_ready(
                recipient.operation_id,
                recipient.incarnation_id,
                &observed_agent("term-1"),
                None,
            )
            .expect("ready");
        let initial = store
            .create_initial_message(
                recipient.logical_agent_id,
                recipient.incarnation_id,
                &crate::domain::InitialMessageIntent {
                    sender: Some(sender.logical_agent_id),
                    kind: InitialMessageKind::Ask,
                    body: "explicit question".into(),
                },
                "initial-ask",
            )
            .expect("initial ask");
        assert_eq!(
            store
                .obligation_state(initial.message_id)
                .expect("obligation"),
            ObligationState::Open
        );
        assert_eq!(
            store
                .delivery_outcome(initial.operation_id)
                .expect("delivery"),
            DeliveryOutcome::Pending
        );
    }

    #[test]
    fn tell_persists_delivery_without_reply_obligation() {
        let mut store = Store::in_memory().expect("store");
        let recipient = store
            .declare_start(&intent("worker", "term-1", "tell-recipient"))
            .expect("recipient");
        store
            .begin_attempt(recipient.operation_id, recipient.incarnation_id, "request")
            .expect("attempt");
        store
            .accept_start_ready(
                recipient.operation_id,
                recipient.incarnation_id,
                &observed_agent("term-1"),
                None,
            )
            .expect("ready");
        let tell = store
            .create_tell(
                recipient.logical_agent_id,
                recipient.logical_agent_id,
                recipient.incarnation_id,
                "informational",
                "tell-1",
            )
            .expect("tell");
        assert_eq!(
            store.delivery_outcome(tell.operation_id).expect("delivery"),
            DeliveryOutcome::Pending
        );
        let (kind, creates_obligation): (String, bool) = store
            .connection
            .query_row(
                "SELECT kind, creates_obligation FROM messages WHERE id = ?1",
                [tell.message_id.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("message");
        assert_eq!(kind, "tell");
        assert!(!creates_obligation);
        let obligation_count: i64 = store
            .connection
            .query_row(
                "SELECT COUNT(*) FROM obligations WHERE ask_message_id = ?1",
                [tell.message_id.to_string()],
                |row| row.get(0),
            )
            .expect("obligations");
        assert_eq!(obligation_count, 0);
    }

    #[test]
    fn scheduled_tell_persists_queued_and_cancels_before_submit() {
        let mut store = Store::in_memory().expect("store");
        let recipient = store
            .declare_start(&intent("worker", "term-1", "due-tell-recipient"))
            .expect("recipient");
        mark_ready(&mut store, recipient, "worker", "term-1");
        let due_at = store_clock_ms().expect("clock") + 60_000;
        let tell = store
            .create_tell_with_due(
                recipient.logical_agent_id,
                recipient.logical_agent_id,
                recipient.incarnation_id,
                "later",
                "due-tell-1",
                Some(due_at),
            )
            .expect("queued tell");
        assert_eq!(
            store.delivery_outcome(tell.operation_id).expect("delivery"),
            DeliveryOutcome::Queued
        );
        assert!(
            store
                .due_deliveries(due_at - 1)
                .expect("not due")
                .is_empty()
        );
        assert_eq!(store.due_deliveries(due_at).expect("due").len(), 1);
        assert!(
            store
                .cancel_queued_delivery(recipient.logical_agent_id, tell.message_id, "changed mind")
                .expect("cancel")
        );
        assert_eq!(
            store
                .delivery_outcome(tell.operation_id)
                .expect("cancelled"),
            DeliveryOutcome::Superseded
        );
        assert_eq!(
            store.operation_outcome(tell.operation_id).expect("op"),
            OperationOutcome::Superseded
        );
        let (requester, reason): (String, String) = store
            .connection
            .query_row(
                "SELECT cancellation_requester_agent_id, cancellation_reason
                 FROM deliveries WHERE operation_id = ?1",
                [tell.operation_id.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("reason");
        assert_eq!(requester, recipient.logical_agent_id.to_string());
        assert_eq!(reason, "changed mind");
        store
            .begin_attempt(tell.operation_id, recipient.incarnation_id, "too-late")
            .expect_err("superseded cannot begin");
    }

    #[test]
    fn a_handoff_never_leaves_two_ready_incarnations_of_one_agent() {
        let mut store = Store::in_memory().expect("store");
        let first = store
            .declare_start(&intent("coord", "term-a", "handoff-first"))
            .expect("first");
        mark_ready(&mut store, first, "coord", "term-a");

        // The successor continues the same logical agent from a new runtime.
        let mut successor = intent("coord", "term-b", "handoff-second");
        successor.logical_agent_id = Some(first.logical_agent_id);
        successor.pane_id = "w1:p2".into();
        successor.working_directory = "/tmp/new-checkout".into();
        let second = store.declare_start(&successor).expect("second");
        store
            .begin_attempt(second.operation_id, second.incarnation_id, "handoff-req")
            .expect("attempt");
        store
            .accept_start_submission(
                second.operation_id,
                second.incarnation_id,
                "w1:p2",
                "term-b",
            )
            .expect("submitted");

        // One transaction proves the successor Ready and demotes the predecessor.
        store
            .accept_start_ready(
                second.operation_id,
                second.incarnation_id,
                &crate::herdr::AgentObservation {
                    terminal_id: "term-b".into(),
                    pane_id: "w1:p2".into(),
                    name: Some("coord".into()),
                    agent: Some("codex".into()),
                    interactive_ready: true,
                    launch_pending: false,
                    agent_session: None,
                },
                Some(first.incarnation_id),
            )
            .expect("handoff");

        assert_eq!(
            store
                .incarnation_state(first.incarnation_id)
                .expect("predecessor"),
            crate::domain::IncarnationState::Superseded
        );
        assert_eq!(
            store
                .incarnation_state(second.incarnation_id)
                .expect("successor"),
            crate::domain::IncarnationState::Ready
        );
        // The whole point: exactly one Ready incarnation at every observable
        // moment, so alias resolution and reply correlation never go ambiguous.
        assert_eq!(
            store
                .find_ready_alias("coord")
                .expect("alias resolves")
                .map(|(_, incarnation)| incarnation),
            Some(second.incarnation_id)
        );
    }

    #[test]
    fn a_handoff_refuses_a_predecessor_that_is_not_this_agent_and_ready() {
        let mut store = Store::in_memory().expect("store");
        let mine = store
            .declare_start(&intent("mine", "term-a", "handoff-mine"))
            .expect("mine");
        let stranger = store
            .declare_start(&intent("stranger", "term-c", "handoff-stranger"))
            .expect("stranger");
        mark_ready(&mut store, stranger, "stranger", "term-c");
        store
            .begin_attempt(mine.operation_id, mine.incarnation_id, "mine-req")
            .expect("attempt");
        store
            .accept_start_submission(mine.operation_id, mine.incarnation_id, "w1:p1", "term-a")
            .expect("submitted");
        let observed = crate::herdr::AgentObservation {
            terminal_id: "term-a".into(),
            pane_id: "w1:p1".into(),
            name: Some("mine".into()),
            agent: Some("codex".into()),
            interactive_ready: true,
            launch_pending: false,
            agent_session: None,
        };
        // Demoting another agent's incarnation would retire a stranger.
        let error = store
            .accept_start_ready(
                mine.operation_id,
                mine.incarnation_id,
                &observed,
                Some(stranger.incarnation_id),
            )
            .expect_err("refuses a foreign predecessor");
        assert!(matches!(error, StoreError::Conflict(_)), "{error:?}");
        // And the refusal is atomic: the successor is not left Ready either.
        assert_ne!(
            store
                .incarnation_state(mine.incarnation_id)
                .expect("successor"),
            crate::domain::IncarnationState::Ready
        );
    }

    #[test]
    fn a_scheduled_ask_answered_early_never_fires_later() {
        let mut store = Store::in_memory().expect("store");
        let waiting = store
            .declare_start(&intent("waiting", "term-a", "settled-waiting"))
            .expect("waiting");
        let owing = store
            .declare_start(&intent("owing", "term-b", "settled-owing"))
            .expect("owing");
        mark_ready(&mut store, owing, "owing", "term-b");
        let due_at = store_clock_ms().expect("clock") + 3_600_000;
        let ask = store
            .create_ask_with_due(
                waiting.logical_agent_id,
                owing.logical_agent_id,
                owing.incarnation_id,
                "review the freeze",
                "settled-due-ask",
                Some(due_at),
            )
            .expect("queued ask");

        // The recipient learns of the ask by other means — a peer naming its ID
        // — and answers before the scheduled delivery is due.
        store
            .cancel_obligation(waiting.logical_agent_id, ask.message_id, "answered already")
            .expect("obligation settles");

        // At the scheduled moment the delivery must not present settled work as
        // a fresh demand: the recipient cannot tell a late envelope from a new
        // request, and would redo work it already delivered.
        assert_eq!(
            store.supersede_settled_queued_asks(due_at).expect("sweep"),
            1
        );
        assert!(
            store
                .due_deliveries(due_at)
                .expect("due")
                .iter()
                .all(|due| due.message_id != ask.message_id),
            "a settled ask must never be delivered"
        );
        assert_eq!(
            store.delivery_outcome(ask.operation_id).expect("outcome"),
            DeliveryOutcome::Superseded
        );
    }

    #[test]
    fn scheduled_ask_cancel_closes_obligation_only_before_submit() {
        let mut store = Store::in_memory().expect("store");
        let waiting = store
            .declare_start(&intent("waiting", "term-a", "due-ask-waiting"))
            .expect("waiting");
        let owing = store
            .declare_start(&intent("owing", "term-b", "due-ask-owing"))
            .expect("owing");
        mark_ready(&mut store, owing, "owing", "term-b");
        let due_at = store_clock_ms().expect("clock") + 1_000;
        let ask = store
            .create_ask_with_due(
                waiting.logical_agent_id,
                owing.logical_agent_id,
                owing.incarnation_id,
                "later?",
                "due-ask-1",
                Some(due_at),
            )
            .expect("queued ask");
        assert_eq!(
            store.obligation_state(ask.message_id).expect("open"),
            ObligationState::Open
        );
        store
            .begin_attempt(ask.operation_id, owing.incarnation_id, "due-ask-req")
            .expect("prepared");
        store
            .submit_queued_delivery(ask.operation_id, 1, "due-ask-req", due_at)
            .expect("submitted");
        let late =
            store.cancel_queued_delivery(waiting.logical_agent_id, ask.message_id, "too late");
        assert!(matches!(late, Ok(false)));
        store
            .cancel_obligation(waiting.logical_agent_id, ask.message_id, "drop obligation")
            .expect("obligation still cancellable");
        assert_eq!(
            store.delivery_outcome(ask.operation_id).expect("submitted"),
            DeliveryOutcome::Submitted
        );
        assert_eq!(
            store.obligation_state(ask.message_id).expect("cancelled"),
            ObligationState::Cancelled
        );
    }

    #[test]
    fn recover_marks_overdue_queued_unknown_and_leaves_future_queued() {
        let mut store = Store::in_memory().expect("store");
        let recipient = store
            .declare_start(&intent("worker", "term-1", "missed-due-recipient"))
            .expect("recipient");
        mark_ready(&mut store, recipient, "worker", "term-1");
        let now = store_clock_ms().expect("clock");
        let overdue = store
            .create_tell_with_due(
                recipient.logical_agent_id,
                recipient.logical_agent_id,
                recipient.incarnation_id,
                "missed",
                "missed-due",
                Some(now - 5_000),
            )
            .expect("overdue");
        let future = store
            .create_tell_with_due(
                recipient.logical_agent_id,
                recipient.logical_agent_id,
                recipient.incarnation_id,
                "later",
                "future-due",
                Some(now + 60_000),
            )
            .expect("future");
        let report = store
            .reconcile(&Snapshot {
                protocol: 20,
                panes: vec![],
                agents: vec![observed_agent("term-1")],
            })
            .expect("reconcile");
        assert_eq!(report.outcomes_marked_unknown, 1);
        assert_eq!(
            store
                .delivery_outcome(overdue.operation_id)
                .expect("missed"),
            DeliveryOutcome::Unknown
        );
        assert_eq!(
            store
                .delivery_outcome(future.operation_id)
                .expect("still queued"),
            DeliveryOutcome::Queued
        );
        assert!(
            store
                .due_deliveries(now)
                .expect("no silent fire")
                .is_empty()
        );
    }

    #[test]
    fn open_ask_survives_store_restart() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("kelpie.sqlite3");
        let ask_id = {
            let mut store = Store::open(&path).expect("store");
            let waiting = store
                .declare_start(&intent("waiting", "term-a", "restart-start-a"))
                .expect("waiting");
            let owing = store
                .declare_start(&intent("owing", "term-b", "restart-start-b"))
                .expect("owing");
            store
                .create_ask(
                    waiting.logical_agent_id,
                    owing.logical_agent_id,
                    owing.incarnation_id,
                    "survive",
                    "restart-ask",
                )
                .expect("ask")
                .message_id
        };
        let reopened = Store::open(&path).expect("reopen store");
        assert_eq!(
            reopened.obligation_state(ask_id).expect("obligation"),
            ObligationState::Open
        );
    }

    #[test]
    fn pending_order_survives_same_millisecond_and_restart() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("pending-order.sqlite3");
        let (owing_id, first_id, second_id) = {
            let mut store = Store::open(&path).expect("store");
            let waiting = store
                .declare_start(&intent("waiting", "term-a", "order-start-a"))
                .expect("waiting");
            let owing = store
                .declare_start(&intent("owing", "term-b", "order-start-b"))
                .expect("owing");
            let first = store
                .create_ask(
                    waiting.logical_agent_id,
                    owing.logical_agent_id,
                    owing.incarnation_id,
                    "first",
                    "order-ask-1",
                )
                .expect("first ask");
            let second = store
                .create_ask(
                    waiting.logical_agent_id,
                    owing.logical_agent_id,
                    owing.incarnation_id,
                    "second",
                    "order-ask-2",
                )
                .expect("second ask");
            store
                .connection
                .execute("UPDATE obligations SET created_at_ms = 1", [])
                .expect("same millisecond");
            (owing.logical_agent_id, first.message_id, second.message_id)
        };

        let reopened = Store::open(&path).expect("reopen");
        let ordered: Vec<MessageId> = reopened
            .pending_obligations(owing_id)
            .expect("pending")
            .into_iter()
            .map(|obligation| obligation.ask_message_id)
            .collect();
        assert_eq!(ordered, vec![first_id, second_id]);
        let sequences: Vec<i64> = {
            let mut statement = reopened
                .connection
                .prepare("SELECT creation_sequence FROM obligations ORDER BY creation_sequence")
                .expect("statement");
            statement
                .query_map([], |row| row.get(0))
                .expect("sequences")
                .collect::<Result<_, _>>()
                .expect("sequence rows")
        };
        assert_eq!(sequences, vec![1, 2]);
    }

    #[test]
    fn cancellation_enforces_owner_reason_and_terminal_states() {
        let mut store = Store::in_memory().expect("store");
        let waiting = store
            .declare_start(&intent("waiting", "term-a", "cancel-waiting"))
            .expect("waiting");
        let owing = store
            .declare_start(&intent("owing", "term-b", "cancel-owing"))
            .expect("owing");
        let spoof = store
            .declare_start(&intent("spoof", "term-c", "cancel-spoof"))
            .expect("spoof");
        let ask = store
            .create_ask(
                waiting.logical_agent_id,
                owing.logical_agent_id,
                owing.incarnation_id,
                "question",
                "cancel-ask",
            )
            .expect("ask");
        for conflict in [
            store.cancel_obligation(waiting.logical_agent_id, ask.message_id, "  "),
            store.cancel_obligation(spoof.logical_agent_id, ask.message_id, "spoofed"),
            store.cancel_obligation(waiting.logical_agent_id, MessageId::new(), "absent"),
        ] {
            assert!(matches!(conflict, Err(StoreError::Conflict(_))));
        }
        assert_eq!(
            store.obligation_state(ask.message_id).expect("unchanged"),
            ObligationState::Open
        );
        store
            .cancel_obligation(waiting.logical_agent_id, ask.message_id, "no longer needed")
            .expect("cancel");
        assert_eq!(
            store.obligation_state(ask.message_id).expect("cancelled"),
            ObligationState::Cancelled
        );
        let repeat =
            store.cancel_obligation(waiting.logical_agent_id, ask.message_id, "replace reason");
        assert!(matches!(repeat, Err(StoreError::Conflict(_))));
        let (requester, reason): (String, String) = store
            .connection
            .query_row(
                "SELECT cancellation_requester_agent_id, cancellation_reason
                 FROM obligations WHERE ask_message_id = ?1",
                [ask.message_id.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("cancellation evidence");
        assert_eq!(requester, waiting.logical_agent_id.to_string());
        assert_eq!(reason, "no longer needed");

        mark_ready(&mut store, waiting, "waiting", "term-a");
        let resolved = store
            .create_ask(
                waiting.logical_agent_id,
                owing.logical_agent_id,
                owing.incarnation_id,
                "resolved question",
                "resolved-before-cancel",
            )
            .expect("resolved ask");
        deliver_reply(
            &mut store,
            resolved.message_id,
            owing.logical_agent_id,
            "done",
            ReplyDisposition::Final,
            "resolve-before-cancel",
            "term-a",
        );
        let terminal =
            store.cancel_obligation(waiting.logical_agent_id, resolved.message_id, "too late");
        assert!(matches!(terminal, Err(StoreError::Conflict(_))));
        assert_eq!(
            store
                .obligation_state(resolved.message_id)
                .expect("resolved unchanged"),
            ObligationState::Resolved
        );
    }

    #[test]
    fn cancellation_claim_and_reason_survive_restart() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("cancellation.sqlite3");
        let (waiting_id, ask_id) = {
            let mut store = Store::open(&path).expect("store");
            let waiting = store
                .declare_start(&intent("waiting", "term-a", "restart-cancel-waiting"))
                .expect("waiting");
            let owing = store
                .declare_start(&intent("owing", "term-b", "restart-cancel-owing"))
                .expect("owing");
            let ask = store
                .create_ask(
                    waiting.logical_agent_id,
                    owing.logical_agent_id,
                    owing.incarnation_id,
                    "question",
                    "restart-cancel-ask",
                )
                .expect("ask");
            store
                .cancel_obligation(waiting.logical_agent_id, ask.message_id, "durable reason")
                .expect("cancel");
            (waiting.logical_agent_id, ask.message_id)
        };
        let reopened = Store::open(&path).expect("reopen");
        assert_eq!(
            reopened.obligation_state(ask_id).expect("state"),
            ObligationState::Cancelled
        );
        let (requester, reason): (String, String) = reopened
            .connection
            .query_row(
                "SELECT cancellation_requester_agent_id, cancellation_reason
                 FROM obligations WHERE ask_message_id = ?1",
                [ask_id.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("evidence");
        assert_eq!(requester, waiting_id.to_string());
        assert_eq!(reason, "durable reason");
    }

    #[test]
    fn in_progress_obligation_can_be_cancelled() {
        let mut store = Store::in_memory().expect("store");
        let waiting = store
            .declare_start(&intent("waiting", "term-a", "progress-cancel-waiting"))
            .expect("waiting");
        mark_ready(&mut store, waiting, "waiting", "term-a");
        let owing = store
            .declare_start(&intent("owing", "term-b", "progress-cancel-owing"))
            .expect("owing");
        let ask = store
            .create_ask(
                waiting.logical_agent_id,
                owing.logical_agent_id,
                owing.incarnation_id,
                "question",
                "progress-cancel-ask",
            )
            .expect("ask");
        deliver_reply(
            &mut store,
            ask.message_id,
            owing.logical_agent_id,
            "working",
            ReplyDisposition::Progress,
            "progress-cancel-progress",
            "term-a",
        );
        store
            .cancel_obligation(
                waiting.logical_agent_id,
                ask.message_id,
                "superseded request",
            )
            .expect("cancel in progress");
        assert_eq!(
            store.obligation_state(ask.message_id).expect("state"),
            ObligationState::Cancelled
        );
        assert!(
            store
                .pending_obligations(owing.logical_agent_id)
                .expect("pending")
                .is_empty()
        );
    }

    #[test]
    fn operator_notice_survives_without_ephemeral_notification() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("notices.sqlite3");
        let notice_id = {
            let mut store = Store::open(&path).expect("store");
            store
                .create_operator_notice("runtime needs operator attention")
                .expect("notice")
        };
        let reopened = Store::open(&path).expect("reopen");
        let notices = reopened.operator_notices().expect("notices");
        assert_eq!(notices.len(), 1);
        assert_eq!(notices[0].id, notice_id);
        assert_eq!(notices[0].body, "runtime needs operator attention");
        assert!(!notices[0].acknowledged);
    }

    #[test]
    fn a_retirement_whose_close_failed_stays_resolvable() {
        // Retirement records durable intent before it writes to Herdr, so a
        // rejected close leaves a `retiring` incarnation. Recovery finishes that
        // only once the runtime is absent; while the pane is still live the
        // close has to be re-sent, and that needs the binding and the original
        // intent to still be resolvable by id.
        let mut store = Store::in_memory().expect("store");
        let declared = store
            .declare_start(&intent("worker", "term-1", "resume-start"))
            .expect("intent");
        store
            .begin_attempt(declared.operation_id, declared.incarnation_id, "request")
            .expect("attempt");
        store
            .accept_start_ready(
                declared.operation_id,
                declared.incarnation_id,
                &observed_agent("term-1"),
                None,
            )
            .expect("ready");

        let (binding, state) = store
            .retirable_binding(declared.incarnation_id)
            .expect("ready incarnation is retirable");
        assert_eq!(state, IncarnationState::Ready);
        assert_eq!(binding.terminal_id, "term-1");

        let retirement = store
            .request_retirement(declared.incarnation_id, "resume-retire")
            .expect("retirement intent");

        let (binding, state) = store
            .retirable_binding(declared.incarnation_id)
            .expect("a retiring incarnation stays retirable");
        assert_eq!(state, IncarnationState::Retiring);
        assert_eq!(binding.terminal_id, "term-1");
        assert_eq!(
            store
                .open_retirement(declared.incarnation_id)
                .expect("the recorded intent is reusable"),
            retirement,
            "resuming must reuse the original intent instead of recording a second one"
        );

        // The runtime is gone: absence is what completes it.
        let absent = Snapshot {
            protocol: 20,
            panes: vec![],
            agents: vec![],
        };
        assert!(
            store
                .complete_retirement_if_absent(
                    retirement,
                    declared.incarnation_id,
                    &binding.pane_id,
                    &binding.terminal_id,
                    &absent,
                )
                .expect("completion")
        );
        assert_eq!(
            store
                .incarnation_state(declared.incarnation_id)
                .expect("state"),
            IncarnationState::Retired
        );
        assert!(
            store.retirable_binding(declared.incarnation_id).is_err(),
            "a retired incarnation is no longer retirable"
        );
    }

    #[test]
    fn a_reused_binding_names_the_newer_incarnation_holding_it() {
        // Panes, terminals, backend kinds, and public names are all reusable, so
        // an older incarnation's recorded binding can point at a runtime a newer
        // incarnation now owns. Retiring the old one must not end the new one.
        let mut store = Store::in_memory().expect("store");
        let older = store
            .declare_start(&intent("worker", "term-1", "reused-first"))
            .expect("first intent");
        store
            .begin_attempt(older.operation_id, older.incarnation_id, "request")
            .expect("first attempt");
        store
            .accept_start_ready(
                older.operation_id,
                older.incarnation_id,
                &observed_agent("term-1"),
                None,
            )
            .expect("first ready");
        store
            .request_retirement(older.incarnation_id, "reused-retire")
            .expect("retirement intent");

        let (binding, _) = store
            .retirable_binding(older.incarnation_id)
            .expect("retiring binding");
        assert_eq!(
            store
                .ready_incarnation_other_than(
                    older.incarnation_id,
                    &binding.pane_id,
                    &binding.terminal_id,
                )
                .expect("holder lookup"),
            None,
            "nothing else is ready on this binding yet"
        );

        // The same pane and terminal come back under a newer incarnation.
        let newer = store
            .declare_start(&intent("worker", "term-1", "reused-second"))
            .expect("second intent");
        store
            .begin_attempt(newer.operation_id, newer.incarnation_id, "request")
            .expect("second attempt");
        store
            .accept_start_ready(
                newer.operation_id,
                newer.incarnation_id,
                &observed_agent("term-1"),
                None,
            )
            .expect("second ready");

        assert_eq!(
            store
                .ready_incarnation_other_than(
                    older.incarnation_id,
                    &binding.pane_id,
                    &binding.terminal_id,
                )
                .expect("holder lookup"),
            Some(newer.incarnation_id),
            "the newer incarnation must be named so the older retirement refuses"
        );
        assert_eq!(
            store
                .ready_incarnation_other_than(
                    newer.incarnation_id,
                    &binding.pane_id,
                    &binding.terminal_id,
                )
                .expect("holder lookup"),
            None,
            "an incarnation never blocks itself"
        );
    }

    #[test]
    fn retirement_requires_exact_absence_and_preserves_artifacts() {
        let artifact_root = tempfile::tempdir().expect("artifact root");
        let artifact = artifact_root.path().join("work.txt");
        std::fs::write(&artifact, "preserve me").expect("artifact");
        let mut start_intent = intent("worker", "term-1", "retire-start");
        start_intent.working_directory = artifact_root.path().display().to_string();
        let mut store = Store::in_memory().expect("store");
        let declared = store.declare_start(&start_intent).expect("intent");
        store
            .begin_attempt(declared.operation_id, declared.incarnation_id, "request")
            .expect("attempt");
        store
            .accept_start_ready(
                declared.operation_id,
                declared.incarnation_id,
                &observed_agent("term-1"),
                None,
            )
            .expect("ready");
        let ask = store
            .create_ask(
                declared.logical_agent_id,
                declared.logical_agent_id,
                declared.incarnation_id,
                "preserved question",
                "retire-ask",
            )
            .expect("ask");
        let retirement = store
            .request_retirement(declared.incarnation_id, "retire-operation")
            .expect("retirement intent");

        let live = Snapshot {
            protocol: 20,
            panes: vec![],
            agents: vec![observed_agent("term-1")],
        };
        assert_eq!(
            store.reconcile(&live).expect("live reconciliation"),
            RecoveryReport {
                untouched_pending_intents: 1,
                retirements_still_live: 1,
                ..RecoveryReport::default()
            }
        );
        assert_eq!(
            store.operation_outcome(retirement).expect("retirement"),
            OperationOutcome::Accepted
        );

        let replacement = Snapshot {
            protocol: 20,
            panes: vec![],
            agents: vec![observed_agent("term-replacement")],
        };
        assert_eq!(
            store
                .reconcile(&replacement)
                .expect("absence reconciliation"),
            RecoveryReport {
                untouched_pending_intents: 1,
                retirements_completed: 1,
                ..RecoveryReport::default()
            }
        );
        let (state, working_directory): (String, String) = store
            .connection
            .query_row(
                "SELECT state, working_directory FROM incarnations WHERE id = ?1",
                [declared.incarnation_id.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("incarnation");
        assert_eq!(state, "retired");
        assert_eq!(
            working_directory,
            artifact_root.path().display().to_string()
        );
        assert!(artifact.exists());
        assert_eq!(
            store.obligation_state(ask.message_id).expect("obligation"),
            ObligationState::Open
        );
        let message_count: i64 = store
            .connection
            .query_row("SELECT COUNT(*) FROM messages", [], |row| row.get(0))
            .expect("messages");
        assert_eq!(message_count, 1);
    }

    #[test]
    fn recovery_marks_replaced_ready_binding_lost_and_preserves_history() {
        let mut store = Store::in_memory().expect("store");
        let declared = store
            .declare_start(&intent("worker", "term-1", "lost-start"))
            .expect("intent");
        store
            .begin_attempt(declared.operation_id, declared.incarnation_id, "request")
            .expect("attempt");
        store
            .accept_start_ready(
                declared.operation_id,
                declared.incarnation_id,
                &observed_agent("term-1"),
                None,
            )
            .expect("ready");
        let ask = store
            .create_ask(
                declared.logical_agent_id,
                declared.logical_agent_id,
                declared.incarnation_id,
                "preserve this question",
                "lost-ask",
            )
            .expect("ask");
        let mut replacement = observed_agent("term-1");
        replacement.name = Some("replacement".into());
        let report = store
            .reconcile(&Snapshot {
                protocol: 20,
                panes: vec![],
                agents: vec![replacement],
            })
            .expect("reconcile");
        assert_eq!(report.incarnations_marked_lost, 1);
        assert_eq!(
            store
                .incarnation_state(declared.incarnation_id)
                .expect("state"),
            crate::domain::IncarnationState::Lost
        );
        assert_eq!(
            store
                .agent_address(declared.logical_agent_id)
                .expect("identity"),
            "worker"
        );
        assert_eq!(
            store.obligation_state(ask.message_id).expect("obligation"),
            ObligationState::Open
        );
    }

    #[test]
    fn recovery_keeps_exact_ready_binding_despite_readiness_hint_change() {
        let mut store = Store::in_memory().expect("store");
        let declared = store
            .declare_start(&intent("worker", "term-1", "still-live-start"))
            .expect("intent");
        store
            .begin_attempt(declared.operation_id, declared.incarnation_id, "request")
            .expect("attempt");
        store
            .accept_start_ready(
                declared.operation_id,
                declared.incarnation_id,
                &observed_agent("term-1"),
                None,
            )
            .expect("ready");
        let mut exact = observed_agent("term-1");
        exact.interactive_ready = false;
        let report = store
            .reconcile(&Snapshot {
                protocol: 20,
                panes: vec![],
                agents: vec![exact],
            })
            .expect("reconcile");
        assert_eq!(report.incarnations_marked_lost, 0);
        assert_eq!(
            store
                .incarnation_state(declared.incarnation_id)
                .expect("state"),
            crate::domain::IncarnationState::Ready
        );
    }

    /// A Ready incarnation bound to `session`, for the rotation tests.
    fn ready_with_session(store: &mut Store, key: &str, session: Option<&str>) -> DeclaredStart {
        let declared = store
            .declare_start(&intent("worker", "term-1", key))
            .expect("intent");
        store
            .begin_attempt(declared.operation_id, declared.incarnation_id, key)
            .expect("attempt");
        let mut agent = observed_agent("term-1");
        agent.agent_session = session.map(|value| serde_json::json!({"value": value}));
        store
            .accept_start_ready(declared.operation_id, declared.incarnation_id, &agent, None)
            .expect("ready");
        declared
    }

    fn rotated_to(session: &str) -> crate::herdr::AgentObservation {
        let mut agent = observed_agent("term-1");
        agent.agent_session = Some(serde_json::json!({"value": session}));
        agent
    }

    fn snapshot_of(agent: crate::herdr::AgentObservation) -> Snapshot {
        Snapshot {
            protocol: 20,
            panes: vec![],
            agents: vec![agent],
        }
    }

    #[test]
    fn a_rotated_conversation_keeps_the_binding_and_refreshes_the_record() {
        let mut store = Store::in_memory().expect("store");
        let declared = ready_with_session(&mut store, "rotation-start", Some("first"));
        let report = store
            .reconcile(&snapshot_of(rotated_to("second")))
            .expect("reconcile");
        // Clearing, resuming, compacting, or forking a live agent rotates this
        // reference without replacing the runtime, so the binding survives and
        // the stale value is replaced.
        assert_eq!(report.incarnations_marked_lost, 0);
        assert_eq!(report.native_sessions_refreshed, 1);
        assert_eq!(
            store
                .incarnation_state(declared.incarnation_id)
                .expect("state"),
            crate::domain::IncarnationState::Ready
        );
        assert_eq!(
            store
                .observed_native_session(declared.incarnation_id)
                .expect("session"),
            Some(serde_json::json!({"value": "second"}))
        );
    }

    /// The conversation-age stamp for one incarnation, as the report carries it.
    fn conversation_started_at(store: &Store, incarnation_id: IncarnationId) -> Option<i64> {
        store
            .report()
            .expect("report")
            .agents
            .iter()
            .flat_map(|agent| &agent.incarnations)
            .find(|incarnation| incarnation.id == incarnation_id)
            .expect("incarnation in report")
            .native_session_rotated_at_ms
    }

    #[test]
    fn a_conversation_that_kelpie_never_saw_start_has_an_unknown_age() {
        let mut store = Store::in_memory().expect("store");
        let declared = ready_with_session(&mut store, "never-rotated", Some("first"));
        // The trap this measurement exists to avoid: created_at_ms is when the
        // incarnation was bound, and it is not the conversation start the moment
        // the conversation rotates. An unobserved start is unknown, and unknown
        // is a real answer rather than a reason to substitute a wrong one.
        assert_eq!(
            conversation_started_at(&store, declared.incarnation_id),
            None
        );
    }

    #[test]
    fn a_rotation_stamps_when_the_new_conversation_was_observed() {
        let mut store = Store::in_memory().expect("store");
        let declared = ready_with_session(&mut store, "stamp-start", Some("first"));
        let before = now_millis().expect("clock");
        store
            .reconcile(&snapshot_of(rotated_to("second")))
            .expect("reconcile");
        let stamped = conversation_started_at(&store, declared.incarnation_id)
            .expect("a rotation makes the conversation start known");
        assert!(
            stamped >= before,
            "the stamp records when the boundary was observed, not the incarnation's age"
        );
    }

    #[test]
    fn a_conversation_that_did_not_rotate_keeps_its_original_stamp() {
        let mut store = Store::in_memory().expect("store");
        let declared = ready_with_session(&mut store, "steady-stamp", Some("first"));
        store
            .reconcile(&snapshot_of(rotated_to("second")))
            .expect("first rotation");
        let first = conversation_started_at(&store, declared.incarnation_id).expect("stamped");
        // Reconciliation runs continuously. If an unchanged conversation
        // restamped, every agent would report an age near zero forever and the
        // measurement would silently read as "nothing ever gets old".
        store
            .reconcile(&snapshot_of(rotated_to("second")))
            .expect("no rotation");
        assert_eq!(
            conversation_started_at(&store, declared.incarnation_id),
            Some(first)
        );
    }

    #[test]
    fn an_unchanged_conversation_is_not_counted_as_a_refresh() {
        let mut store = Store::in_memory().expect("store");
        ready_with_session(&mut store, "steady-start", Some("first"));
        let report = store
            .reconcile(&snapshot_of(rotated_to("first")))
            .expect("reconcile");
        assert_eq!(report.native_sessions_refreshed, 0);
        assert_eq!(report.incarnations_marked_lost, 0);
    }

    #[test]
    fn an_incarnation_without_a_recorded_conversation_still_binds() {
        let mut store = Store::in_memory().expect("store");
        let declared = ready_with_session(&mut store, "unrecorded-start", None);
        let report = store
            .reconcile(&snapshot_of(rotated_to("appeared")))
            .expect("reconcile");
        assert_eq!(report.incarnations_marked_lost, 0);
        assert_eq!(
            store
                .incarnation_state(declared.incarnation_id)
                .expect("state"),
            crate::domain::IncarnationState::Ready
        );
    }

    #[test]
    fn a_rotated_conversation_never_detaches_identity_or_obligations() {
        let mut store = Store::in_memory().expect("store");
        let declared = ready_with_session(&mut store, "identity-start", Some("first"));
        let ask = store
            .create_ask(
                declared.logical_agent_id,
                declared.logical_agent_id,
                declared.incarnation_id,
                "still owed across two rotations",
                "identity-ask",
            )
            .expect("ask");
        for session in ["second", "third"] {
            store
                .reconcile(&snapshot_of(rotated_to(session)))
                .expect("reconcile");
        }
        // The production failure this fixes: a live agent kept its pane and its
        // name, rotated its conversation, and lost its logical identity and
        // every debt attached to it.
        assert_eq!(
            store
                .incarnation_state(declared.incarnation_id)
                .expect("state"),
            crate::domain::IncarnationState::Ready
        );
        assert_eq!(
            store
                .agent_address(declared.logical_agent_id)
                .expect("identity"),
            "worker"
        );
        assert_eq!(
            store.obligation_state(ask.message_id).expect("obligation"),
            ObligationState::Open
        );
    }

    #[test]
    fn a_moved_pane_terminal_or_backend_is_still_lost() {
        for (label, mutate) in [
            (
                "pane",
                Box::new(|agent: &mut crate::herdr::AgentObservation| {
                    agent.pane_id = "w9:p9".into();
                }) as Box<dyn Fn(&mut crate::herdr::AgentObservation)>,
            ),
            (
                "terminal",
                Box::new(|agent: &mut crate::herdr::AgentObservation| {
                    agent.terminal_id = "term-9".into();
                }),
            ),
            (
                "backend",
                Box::new(|agent: &mut crate::herdr::AgentObservation| {
                    agent.agent = Some("claude".into());
                }),
            ),
        ] {
            let mut store = Store::in_memory().expect("store");
            let declared = ready_with_session(&mut store, "moved-start", Some("first"));
            let mut agent = rotated_to("first");
            mutate(&mut agent);
            let report = store.reconcile(&snapshot_of(agent)).expect("reconcile");
            assert_eq!(report.incarnations_marked_lost, 1, "{label} must be exact");
            assert_eq!(
                store
                    .incarnation_state(declared.incarnation_id)
                    .expect("state"),
                crate::domain::IncarnationState::Lost,
                "{label} must be exact"
            );
        }
    }

    #[test]
    fn recovery_uses_exact_snapshot_and_never_retries() {
        let mut store = Store::in_memory().expect("store");
        let declared = store
            .declare_start(&intent("worker", "term-1", "recover-start"))
            .expect("intent");
        store
            .begin_attempt(
                declared.operation_id,
                declared.incarnation_id,
                "recover-request",
            )
            .expect("attempt");
        store
            .mark_submitted(declared.operation_id, 1, "recover-request")
            .expect("submitted");

        let snapshot = Snapshot {
            protocol: 20,
            panes: vec![],
            agents: vec![crate::herdr::AgentObservation {
                terminal_id: "term-1".into(),
                pane_id: "w1:p1".into(),
                name: Some("worker".into()),
                agent: Some("codex".into()),
                interactive_ready: true,
                launch_pending: false,
                agent_session: None,
            }],
        };
        assert_eq!(
            store.reconcile(&snapshot).expect("reconcile"),
            RecoveryReport {
                starts_recovered: 1,
                outcomes_marked_unknown: 0,
                untouched_pending_intents: 0,
                unattempted_clears_failed: 0,
                retirements_completed: 0,
                retirements_still_live: 0,
                incarnations_marked_lost: 0,
                native_sessions_refreshed: 0,
            }
        );
        assert_eq!(
            store
                .operation_outcome(declared.operation_id)
                .expect("outcome"),
            OperationOutcome::Succeeded
        );
        assert_eq!(
            store.reconcile(&snapshot).expect("idempotent reconcile"),
            RecoveryReport::default()
        );
    }

    #[test]
    fn crash_after_request_boundary_recovers_to_unknown() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("crash.sqlite3");
        let declared = {
            let mut store = Store::open(&path).expect("store");
            let declared = store
                .declare_start(&intent("worker", "term-1", "crash-start"))
                .expect("intent");
            store
                .begin_attempt(
                    declared.operation_id,
                    declared.incarnation_id,
                    "crash-request",
                )
                .expect("attempt");
            store
                .mark_submitted(declared.operation_id, 1, "crash-request")
                .expect("write boundary");
            declared
        };
        let mut recovered = Store::open(&path).expect("reopen");
        let report = recovered
            .reconcile(&Snapshot {
                protocol: 20,
                panes: vec![],
                agents: vec![],
            })
            .expect("recover");
        assert_eq!(report.outcomes_marked_unknown, 1);
        assert_eq!(
            recovered
                .operation_outcome(declared.operation_id)
                .expect("outcome"),
            OperationOutcome::Unknown
        );
    }

    #[test]
    fn crash_before_request_boundary_remains_proven_pending() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("prepared-crash.sqlite3");
        let declared = {
            let mut store = Store::open(&path).expect("store");
            let declared = store
                .declare_start(&intent("worker", "term-1", "prepared-crash"))
                .expect("intent");
            store
                .begin_attempt(
                    declared.operation_id,
                    declared.incarnation_id,
                    "prepared-request",
                )
                .expect("prepared attempt");
            declared
        };
        let mut recovered = Store::open(&path).expect("reopen");
        assert_eq!(
            recovered
                .reconcile(&Snapshot {
                    protocol: 20,
                    panes: vec![],
                    agents: vec![],
                })
                .expect("recover"),
            RecoveryReport {
                untouched_pending_intents: 1,
                ..RecoveryReport::default()
            }
        );
        assert_eq!(
            recovered
                .operation_outcome(declared.operation_id)
                .expect("outcome"),
            OperationOutcome::Pending
        );
    }

    #[test]
    fn crash_after_raw_start_acceptance_without_readiness_is_unknown() {
        let mut store = Store::in_memory().expect("store");
        let declared = store
            .declare_start(&intent("worker", "term-1", "accepted-crash"))
            .expect("intent");
        store
            .begin_attempt(declared.operation_id, declared.incarnation_id, "request")
            .expect("attempt");
        store
            .mark_submitted(declared.operation_id, 1, "request")
            .expect("submitted");
        store
            .accept_start_submission(
                declared.operation_id,
                declared.incarnation_id,
                "w1:p1",
                "term-1",
            )
            .expect("accepted");
        let mut pending = observed_agent("term-1");
        pending.interactive_ready = false;
        pending.launch_pending = true;
        assert_eq!(
            store
                .reconcile(&Snapshot {
                    protocol: 20,
                    panes: vec![],
                    agents: vec![pending],
                })
                .expect("recover")
                .outcomes_marked_unknown,
            1
        );
        assert_eq!(
            store
                .operation_outcome(declared.operation_id)
                .expect("outcome"),
            OperationOutcome::Unknown
        );
    }

    #[test]
    fn submitted_prompt_is_not_resent_during_recovery() {
        let mut store = Store::in_memory().expect("store");
        let declared = store
            .declare_start(&intent("worker", "term-1", "delivery-start"))
            .expect("intent");
        store
            .begin_attempt(
                declared.operation_id,
                declared.incarnation_id,
                "start-request",
            )
            .expect("start attempt");
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
                "question",
                "delivery-ask",
            )
            .expect("ask");
        store
            .begin_attempt(ask.operation_id, declared.incarnation_id, "prompt-request")
            .expect("prompt attempt");
        store
            .mark_submitted(ask.operation_id, 1, "prompt-request")
            .expect("submitted");
        let report = store
            .reconcile(&Snapshot {
                protocol: 20,
                panes: vec![],
                agents: vec![crate::herdr::AgentObservation {
                    terminal_id: "term-1".into(),
                    pane_id: "w1:p1".into(),
                    name: Some("worker".into()),
                    agent: Some("codex".into()),
                    interactive_ready: true,
                    launch_pending: false,
                    agent_session: None,
                }],
            })
            .expect("recover");
        assert_eq!(report.outcomes_marked_unknown, 1);
        assert_eq!(
            store.operation_outcome(ask.operation_id).expect("outcome"),
            OperationOutcome::Unknown
        );
        assert_eq!(
            store.obligation_state(ask.message_id).expect("obligation"),
            ObligationState::Open
        );
    }

    #[test]
    fn alias_resolution_binds_exact_ids_and_reuse_does_not_retarget() {
        let mut store = Store::in_memory().expect("store");
        let first = store
            .declare_start(&intent("alice", "term-1", "alice-first"))
            .expect("first");
        mark_ready(&mut store, first, "alice", "term-1");
        let (resolved_agent, resolved_incarnation) =
            store.resolve_ready_alias("alice").expect("resolve");
        assert_eq!(resolved_agent, first.logical_agent_id);
        assert_eq!(resolved_incarnation, first.incarnation_id);

        let waiting = store
            .declare_start(&intent("waiting", "term-w", "waiting-start"))
            .expect("waiting");
        let ask = store
            .create_ask(
                waiting.logical_agent_id,
                resolved_agent,
                resolved_incarnation,
                "question for first alice",
                "ask-first-alice",
            )
            .expect("ask");

        store
            .request_retirement(first.incarnation_id, "retire-first-alice")
            .expect("retire");
        // Force terminal retirement without Herdr so the alias can be reclaimed.
        store
            .connection
            .execute(
                "UPDATE incarnations SET state = 'retired' WHERE id = ?1",
                [first.incarnation_id.to_string()],
            )
            .expect("force retire");

        let second = store
            .declare_start(&intent("alice", "term-2", "alice-second"))
            .expect("second");
        assert_ne!(second.logical_agent_id, first.logical_agent_id);
        mark_ready(&mut store, second, "alice", "term-2");
        let (new_agent, new_incarnation) = store.resolve_ready_alias("alice").expect("new alice");
        assert_eq!(new_agent, second.logical_agent_id);
        assert_eq!(new_incarnation, second.incarnation_id);

        let (sender, recipient) = store.message_parties(ask.message_id).expect("parties");
        assert_eq!(sender, waiting.logical_agent_id);
        assert_eq!(recipient, first.logical_agent_id);
        assert_eq!(
            store
                .delivery_recipient_incarnation(ask.message_id)
                .expect("delivery"),
            first.incarnation_id
        );
        assert_eq!(
            store
                .pending_obligations(first.logical_agent_id)
                .expect("first pending")[0]
                .ask_message_id,
            ask.message_id
        );
        assert!(
            store
                .pending_obligations(second.logical_agent_id)
                .expect("second pending")
                .is_empty()
        );
    }

    #[test]
    fn continuing_logical_agent_preserves_obligations_new_name_owner_does_not() {
        let mut store = Store::in_memory().expect("store");
        let waiting = store
            .declare_start(&intent("waiting", "term-w", "waiting"))
            .expect("waiting");
        let original = store
            .declare_start(&intent("worker", "term-1", "worker-v1"))
            .expect("worker");
        mark_ready(&mut store, original, "worker", "term-1");
        let ask = store
            .create_ask(
                waiting.logical_agent_id,
                original.logical_agent_id,
                original.incarnation_id,
                "still owed after restart",
                "ask-preserve",
            )
            .expect("ask");

        let mut continued_intent = intent("worker", "term-2", "worker-v2");
        continued_intent.logical_agent_id = Some(original.logical_agent_id);
        continued_intent.pane_id = "w1:p2".into();
        let continued = store.declare_start(&continued_intent).expect("continue");
        assert_eq!(continued.logical_agent_id, original.logical_agent_id);
        assert_ne!(continued.incarnation_id, original.incarnation_id);
        store
            .begin_attempt(
                continued.operation_id,
                continued.incarnation_id,
                "continue-request",
            )
            .expect("attempt");
        store
            .accept_start_submission(
                continued.operation_id,
                continued.incarnation_id,
                "w1:p2",
                "term-2",
            )
            .expect("submission");
        store
            .accept_start_ready(
                continued.operation_id,
                continued.incarnation_id,
                &crate::herdr::AgentObservation {
                    terminal_id: "term-2".into(),
                    pane_id: "w1:p2".into(),
                    name: Some("worker".into()),
                    agent: Some("codex".into()),
                    interactive_ready: true,
                    launch_pending: false,
                    agent_session: None,
                },
                None,
            )
            .expect("ready");

        assert_eq!(
            store
                .pending_obligations(original.logical_agent_id)
                .expect("preserved")[0]
                .ask_message_id,
            ask.message_id
        );

        let mut new_owner = intent("worker", "term-3", "worker-new-owner");
        new_owner.pane_id = "w1:p3".into();
        // Retire continued so alias is unique among ready agents when needed.
        store
            .connection
            .execute(
                "UPDATE incarnations SET state = 'retired' WHERE id IN (?1, ?2)",
                params![
                    original.incarnation_id.to_string(),
                    continued.incarnation_id.to_string()
                ],
            )
            .expect("retire prior");
        let replacement = store.declare_start(&new_owner).expect("new owner");
        assert_ne!(replacement.logical_agent_id, original.logical_agent_id);
        assert!(
            store
                .pending_obligations(replacement.logical_agent_id)
                .expect("no inheritance")
                .is_empty()
        );
        assert_eq!(
            store
                .pending_obligations(original.logical_agent_id)
                .expect("original still owns")[0]
                .ask_message_id,
            ask.message_id
        );
    }

    #[test]
    fn simplified_reply_fails_closed_for_stale_correlation() {
        let mut store = Store::in_memory().expect("store");
        let waiting = store
            .declare_start(&intent("waiting", "term-a", "stale-waiting"))
            .expect("waiting");
        mark_ready(&mut store, waiting, "waiting", "term-a");
        let owing = store
            .declare_start(&intent("owing", "term-b", "stale-owing"))
            .expect("owing");
        let ask = store
            .create_ask(
                waiting.logical_agent_id,
                owing.logical_agent_id,
                owing.incarnation_id,
                "question",
                "stale-ask",
            )
            .expect("ask");
        deliver_reply(
            &mut store,
            ask.message_id,
            owing.logical_agent_id,
            "done",
            ReplyDisposition::Final,
            "stale-final",
            "term-a",
        );
        let stale = store.create_reply(
            ask.message_id,
            owing.logical_agent_id,
            "again",
            ReplyDisposition::Final,
            "stale-again",
        );
        assert!(matches!(stale, Err(StoreError::Conflict(_))));
        let missing = store.create_reply(
            MessageId::new(),
            owing.logical_agent_id,
            "nope",
            ReplyDisposition::Progress,
            "missing-ask",
        );
        assert!(matches!(missing, Err(StoreError::Conflict(_))));
    }

    #[test]
    fn final_rejects_do_not_resolve_and_unknown_does_not_resend() {
        let mut store = Store::in_memory().expect("store");
        let waiting = store
            .declare_start(&intent("waiting", "term-a", "reject-waiting"))
            .expect("waiting");
        mark_ready(&mut store, waiting, "waiting", "term-a");
        let owing = store
            .declare_start(&intent("owing", "term-b", "reject-owing"))
            .expect("owing");
        let ask = store
            .create_ask(
                waiting.logical_agent_id,
                owing.logical_agent_id,
                owing.incarnation_id,
                "question",
                "reject-ask",
            )
            .expect("ask");
        let rejected = store
            .create_reply(
                ask.message_id,
                owing.logical_agent_id,
                "done",
                ReplyDisposition::Final,
                "final-reject",
            )
            .expect("create final");
        let rejected_operation = rejected.operation_id.expect("pane reply operation");
        let rejected_incarnation = rejected
            .recipient_incarnation
            .expect("pane reply incarnation");
        store
            .begin_attempt(rejected_operation, rejected_incarnation, "final-reject-req")
            .expect("attempt");
        store
            .mark_submitted(rejected_operation, 1, "final-reject-req")
            .expect("submitted");
        store
            .mark_rejected(
                rejected_operation,
                rejected_incarnation,
                "target missing",
                DeliveryOutcome::TargetUnavailable,
            )
            .expect("rejected");
        assert_eq!(
            store.obligation_state(ask.message_id).expect("open"),
            ObligationState::Open
        );
        assert_eq!(
            store
                .delivery_outcome(rejected_operation)
                .expect("delivery"),
            DeliveryOutcome::TargetUnavailable
        );

        let unknown = store
            .create_reply(
                ask.message_id,
                owing.logical_agent_id,
                "done again",
                ReplyDisposition::Final,
                "final-unknown",
            )
            .expect("second final");
        let unknown_operation = unknown.operation_id.expect("pane reply operation");
        let unknown_incarnation = unknown
            .recipient_incarnation
            .expect("pane reply incarnation");
        store
            .begin_attempt(unknown_operation, unknown_incarnation, "final-unknown-req")
            .expect("attempt");
        store
            .mark_submitted(unknown_operation, 1, "final-unknown-req")
            .expect("submitted");
        store
            .mark_unknown(unknown_operation, unknown_incarnation, "disconnect")
            .expect("unknown");
        assert_eq!(
            store.obligation_state(ask.message_id).expect("still open"),
            ObligationState::Open
        );
        assert_eq!(
            store
                .delivery_outcome(unknown_operation)
                .expect("unknown delivery"),
            DeliveryOutcome::Unknown
        );
        // Recovery must not create a second attempt for the ambiguous delivery.
        let attempts: i64 = store
            .connection
            .query_row(
                "SELECT COUNT(*) FROM operation_attempts WHERE operation_id = ?1",
                [unknown_operation.to_string()],
                |row| row.get(0),
            )
            .expect("attempt count");
        assert_eq!(attempts, 1);
    }

    #[test]
    fn reply_requires_unique_ready_waiting_incarnation() {
        let mut store = Store::in_memory().expect("store");
        let waiting = store
            .declare_start(&intent("waiting", "term-a", "no-ready-waiting"))
            .expect("waiting");
        let owing = store
            .declare_start(&intent("owing", "term-b", "no-ready-owing"))
            .expect("owing");
        let ask = store
            .create_ask(
                waiting.logical_agent_id,
                owing.logical_agent_id,
                owing.incarnation_id,
                "question",
                "no-ready-ask",
            )
            .expect("ask");
        let missing = store.create_reply(
            ask.message_id,
            owing.logical_agent_id,
            "body",
            ReplyDisposition::Progress,
            "no-ready-progress",
        );
        assert!(matches!(missing, Err(StoreError::Conflict(_))));
    }

    fn adopt_intent(key: &str) -> crate::domain::AdoptIntent {
        crate::domain::AdoptIntent {
            pane_id: "w1:p9".into(),
            expected_terminal_id: "term-live".into(),
            public_name: Some("preexisting".into()),
            logical_agent_id: None,
            parent: Parent::Parentless,
            herdr_session: "test".into(),
            backend_kind: Some("grok".into()),
            backend_args: Vec::new(),
            requested_model: None,
            requested_provider: None,
            requested_effort: None,
            idempotency_key: key.into(),
        }
    }

    fn ready_evidence() -> AdoptEvidence {
        AdoptEvidence {
            pane_id: "w1:p9".into(),
            terminal_id: "term-live".into(),
            public_name: "preexisting".into(),
            backend_kind: "grok".into(),
            working_directory: "/tmp/work".into(),
            interactive_ready: true,
            launch_pending: false,
            native_agent_session: Some(serde_json::json!({
                "agent":"grok","kind":"id","value":"sess-1"
            })),
        }
    }

    #[test]
    fn adopt_creates_ready_binding_and_is_idempotent() {
        let mut store = Store::in_memory().expect("store");
        let first = store
            .declare_adopt(&adopt_intent("adopt-1"), &ready_evidence())
            .expect("adopt");
        assert_eq!(
            store
                .incarnation_state(first.incarnation_id)
                .expect("state"),
            crate::domain::IncarnationState::Ready
        );
        let binding = store.ready_binding(first.incarnation_id).expect("binding");
        assert_eq!(binding.pane_id, "w1:p9");
        assert_eq!(binding.terminal_id, "term-live");
        let again = store
            .declared_by_idempotency_key("adopt-1")
            .expect("lookup")
            .expect("present");
        assert_eq!(again, first);
        let duplicate_key = store.declare_adopt(&adopt_intent("adopt-1"), &ready_evidence());
        assert!(matches!(duplicate_key, Err(StoreError::Conflict(_))));
    }

    #[test]
    fn adopt_fails_closed_for_launch_pending_name_kind_and_duplicate_binding() {
        let mut store = Store::in_memory().expect("store");
        let mut pending = ready_evidence();
        pending.launch_pending = true;
        assert!(matches!(
            store.declare_adopt(&adopt_intent("a-pending"), &pending),
            Err(StoreError::Conflict(_))
        ));
        let mut unmanaged_idle = ready_evidence();
        unmanaged_idle.interactive_ready = false;
        store
            .declare_adopt(&adopt_intent("a-idle-unmanaged"), &unmanaged_idle)
            .expect("idle occupant without managed interactive_ready is adoptable");
        store
            .connection
            .execute(
                "UPDATE incarnations SET state = 'lost' WHERE observed_pane_id = 'w1:p9'",
                [],
            )
            .expect("clear binding");
        let mut wrong_name = ready_evidence();
        wrong_name.public_name = "other".into();
        assert!(matches!(
            store.declare_adopt(&adopt_intent("a-name"), &wrong_name),
            Err(StoreError::Conflict(_))
        ));
        let mut wrong_kind = ready_evidence();
        wrong_kind.backend_kind = "codex".into();
        assert!(matches!(
            store.declare_adopt(&adopt_intent("a-kind"), &wrong_kind),
            Err(StoreError::Conflict(_))
        ));
        store
            .declare_adopt(&adopt_intent("a-first"), &ready_evidence())
            .expect("first");
        assert!(matches!(
            store.declare_adopt(&adopt_intent("a-second"), &ready_evidence()),
            Err(StoreError::Conflict(_))
        ));
    }

    /// A closed pane must not make its alias permanently unadoptable.
    ///
    /// On 2026-08-22 a Buzz occupant's grok pane was closed and replaced by an
    /// opencode one. Kelpie kept the old incarnation Ready, so every adopt of
    /// that name was refused against a runtime Herdr no longer had, and the
    /// channel stayed silent until `kelpie recover` was run by hand.
    #[test]
    fn an_alias_bound_to_a_dead_pane_is_released_by_the_snapshot() {
        let mut store = Store::in_memory().expect("store");
        let first = store
            .declare_adopt(&adopt_intent("adopt-first"), &ready_evidence())
            .expect("adopt");

        // The pane is gone: the snapshot has no agent at w1:p9 / term-live.
        let snapshot = Snapshot {
            protocol: 20,
            panes: vec![],
            agents: vec![crate::herdr::AgentObservation {
                terminal_id: "term-new".into(),
                pane_id: "w2:p1".into(),
                name: Some("preexisting".into()),
                agent: Some("opencode".into()),
                interactive_ready: true,
                launch_pending: false,
                agent_session: None,
            }],
        };
        assert_eq!(
            store
                .release_absent_alias_binding("preexisting", &snapshot)
                .expect("release"),
            Some(first.incarnation_id),
            "the binding Herdr cannot see is the one that yields"
        );
        assert_eq!(
            store
                .incarnation_state(first.incarnation_id)
                .expect("state"),
            crate::domain::IncarnationState::Lost
        );

        // And the name is adoptable again, which is the whole point.
        store
            .declare_adopt(&adopt_intent("adopt-second"), &ready_evidence())
            .expect("the alias is free once its pane is gone");
    }

    /// A live binding still wins. This is not adoption guessing.
    #[test]
    fn a_live_alias_binding_is_never_released() {
        let mut store = Store::in_memory().expect("store");
        let first = store
            .declare_adopt(&adopt_intent("adopt-first"), &ready_evidence())
            .expect("adopt");
        let snapshot = Snapshot {
            protocol: 20,
            panes: vec![],
            agents: vec![crate::herdr::AgentObservation {
                terminal_id: "term-live".into(),
                pane_id: "w1:p9".into(),
                name: Some("preexisting".into()),
                agent: Some("grok".into()),
                interactive_ready: true,
                launch_pending: false,
                agent_session: None,
            }],
        };
        assert_eq!(
            store
                .release_absent_alias_binding("preexisting", &snapshot)
                .expect("release"),
            None
        );
        assert_eq!(
            store
                .incarnation_state(first.incarnation_id)
                .expect("state"),
            crate::domain::IncarnationState::Ready
        );
        store
            .declare_adopt(&adopt_intent("adopt-second"), &ready_evidence())
            .expect_err("a live holder still refuses the name");
    }

    #[test]
    fn adopt_continue_preserves_history_name_reuse_does_not() {
        let mut store = Store::in_memory().expect("store");
        let waiting = store
            .declare_start(&intent("waiting", "term-w", "wait-adopt"))
            .expect("waiting");
        let first = store
            .declare_adopt(&adopt_intent("adopt-orig"), &ready_evidence())
            .expect("adopt");
        let ask = store
            .create_ask(
                waiting.logical_agent_id,
                first.logical_agent_id,
                first.incarnation_id,
                "owed",
                "ask-adopt",
            )
            .expect("ask");
        // Force prior incarnation off Ready so continue can rebind the live slot.
        store
            .connection
            .execute(
                "UPDATE incarnations SET state = 'lost' WHERE id = ?1",
                [first.incarnation_id.to_string()],
            )
            .expect("lose first");
        let mut cont = adopt_intent("adopt-continue");
        cont.logical_agent_id = Some(first.logical_agent_id);
        let continued = store
            .declare_adopt(&cont, &ready_evidence())
            .expect("continue");
        assert_eq!(continued.logical_agent_id, first.logical_agent_id);
        assert_ne!(continued.incarnation_id, first.incarnation_id);
        assert_eq!(
            store
                .pending_obligations(first.logical_agent_id)
                .expect("pending")[0]
                .ask_message_id,
            ask.message_id
        );
        store
            .connection
            .execute(
                "UPDATE incarnations SET state = 'lost' WHERE id = ?1",
                [continued.incarnation_id.to_string()],
            )
            .expect("lose continued");
        // The obligation is still unresolved, so a create-new adopt under the
        // same name is refused rather than forking an identity someone waits on.
        let forked = store.declare_adopt(&adopt_intent("adopt-fork"), &ready_evidence());
        let Err(StoreError::Conflict(message)) = forked else {
            panic!("create-new adopt must fail closed while an obligation is unresolved");
        };
        assert!(
            message.contains(&first.logical_agent_id.to_string()),
            "the refusal names the logical agent to continue: {message}"
        );
        store
            .cancel_obligation(waiting.logical_agent_id, ask.message_id, "test teardown")
            .expect("cancel");
        let replacement = store
            .declare_adopt(&adopt_intent("adopt-new-owner"), &ready_evidence())
            .expect("new owner same name");
        assert_ne!(replacement.logical_agent_id, first.logical_agent_id);
        assert!(
            store
                .pending_obligations(replacement.logical_agent_id)
                .expect("empty")
                .is_empty()
        );
    }

    fn mark_lost(store: &Store, incarnation_id: IncarnationId) {
        store
            .connection
            .execute(
                "UPDATE incarnations SET state = 'lost' WHERE id = ?1",
                [incarnation_id.to_string()],
            )
            .expect("lose");
    }

    #[test]
    fn continuable_binding_returns_the_unique_lost_agent() {
        let mut store = Store::in_memory().expect("store");
        let first = store
            .declare_adopt(&adopt_intent("cont-first"), &ready_evidence())
            .expect("adopt");
        assert_eq!(
            store
                .continuable_logical_agent_for_binding("w1:p9", "term-live", "grok")
                .expect("ready is not continuable"),
            None
        );
        mark_lost(&store, first.incarnation_id);
        assert_eq!(
            store
                .continuable_logical_agent_for_binding("w1:p9", "term-live", "grok")
                .expect("lost"),
            Some(first.logical_agent_id)
        );
        let backend_mismatch = store
            .continuable_logical_agent_for_binding("w1:p9", "term-live", "claude")
            .expect_err("wrong backend");
        assert!(
            backend_mismatch.to_string().contains("adopt --logical-id"),
            "{backend_mismatch}"
        );
        store
            .connection
            .execute(
                "UPDATE incarnations SET state = 'retired' WHERE id = ?1",
                [first.incarnation_id.to_string()],
            )
            .expect("retire");
        assert_eq!(
            store
                .continuable_logical_agent_for_binding("w1:p9", "term-live", "grok")
                .expect("retired is not continuable"),
            None
        );
    }

    #[test]
    fn continuable_binding_fails_closed_on_two_lost_agents() {
        let mut store = Store::in_memory().expect("store");
        let first = store
            .declare_adopt(&adopt_intent("amb-first"), &ready_evidence())
            .expect("first");
        mark_lost(&store, first.incarnation_id);
        let second = store
            .declare_adopt(&adopt_intent("amb-second"), &ready_evidence())
            .expect("second");
        mark_lost(&store, second.incarnation_id);
        let error = store
            .continuable_logical_agent_for_binding("w1:p9", "term-live", "grok")
            .expect_err("ambiguous");
        let message = error.to_string();
        assert!(
            message.contains("2 continuable logical agents"),
            "{message}"
        );
        assert!(message.contains("adopt --logical-id"), "{message}");
        assert!(
            message.contains(&first.logical_agent_id.to_string()),
            "{message}"
        );
        assert!(
            message.contains(&second.logical_agent_id.to_string()),
            "{message}"
        );
    }

    #[test]
    fn continuable_binding_retries_a_declared_occupant() {
        let mut store = Store::in_memory().expect("store");
        let pending = store
            .declare_adopt_pending(&adopt_intent("pend-only"), &ready_evidence())
            .expect("pending");
        assert_eq!(
            store
                .continuable_logical_agent_for_binding("w1:p9", "term-live", "grok")
                .expect("declared retries"),
            Some(pending.logical_agent_id)
        );
        store
            .connection
            .execute(
                "UPDATE incarnations SET state = 'failed' WHERE id = ?1",
                [pending.incarnation_id.to_string()],
            )
            .expect("fail");
        assert_eq!(
            store
                .continuable_logical_agent_for_binding("w1:p9", "term-live", "grok")
                .expect("failed retries"),
            Some(pending.logical_agent_id)
        );
    }

    #[test]
    fn continue_adopt_refuses_a_name_someone_else_still_owes_on() {
        let mut store = Store::in_memory().expect("store");
        let waiting = store
            .declare_start(&intent("waiting", "term-w", "wait-foreign"))
            .expect("waiting");
        let holder = store
            .declare_adopt(&adopt_intent("holder"), &ready_evidence())
            .expect("holder");
        store
            .create_ask(
                waiting.logical_agent_id,
                holder.logical_agent_id,
                holder.incarnation_id,
                "owed",
                "foreign-ask",
            )
            .expect("ask");
        mark_lost(&store, holder.incarnation_id);
        let other = store
            .declare_start(&intent("other", "term-o", "other-start"))
            .expect("other");
        let mut cont = adopt_intent("steal-name");
        cont.logical_agent_id = Some(other.logical_agent_id);
        let error = store
            .declare_adopt(&cont, &ready_evidence())
            .expect_err("foreign debts");
        assert!(
            error
                .to_string()
                .contains(&holder.logical_agent_id.to_string()),
            "{error}"
        );
    }

    fn unnamed_evidence() -> AdoptEvidence {
        AdoptEvidence {
            pane_id: "w7:p1H".into(),
            terminal_id: "term-coord".into(),
            public_name: String::new(),
            backend_kind: "codex".into(),
            working_directory: "/tmp/quorum".into(),
            interactive_ready: false,
            launch_pending: false,
            native_agent_session: Some(serde_json::json!({
                "agent":"codex","kind":"id","value":"sess-coord"
            })),
        }
    }

    /// A name claimed by two logical agents — one Ready, one lost — with one
    /// open ask in each direction, so `name_info` has to report claimant
    /// liveness, per-claimant counts, and both parties of each ask.
    fn claimed_name_fixture(store: &Store) -> (String, String, String) {
        let connection = &store.connection;
        let asker = LogicalAgentId::new();
        let second_claimant = LogicalAgentId::new();
        let responder = LogicalAgentId::new();
        for (agent_id, name, created_at_ms) in [
            (&asker, "worker-x", 1),
            (&second_claimant, "worker-x", 2),
            (&responder, "helper", 3),
        ] {
            connection
                .execute(
                    "INSERT INTO logical_agents
                     (id, public_name, explicitly_parentless, created_at_ms)
                     VALUES (?1, ?2, 1, ?3)",
                    params![agent_id.to_string(), name, created_at_ms],
                )
                .expect("logical agent");
            let state = if agent_id == &asker { "lost" } else { "ready" };
            connection
                .execute(
                    "INSERT INTO incarnations (
                        id, logical_agent_id, herdr_session, intended_pane_id,
                        expected_terminal_id, backend_kind, backend_args_json,
                        working_directory, created_at_ms, state
                     ) VALUES (?1, ?2, 's', 'w1:p1', 't1', 'claude', '[]', '/tmp', 1, ?3)",
                    params![
                        IncarnationId::new().to_string(),
                        agent_id.to_string(),
                        state
                    ],
                )
                .expect("incarnation");
        }
        let open_ask = MessageId::new();
        let progress_ask = MessageId::new();
        for (sequence, ask, owing, waiting, state) in [
            (1, &open_ask, &responder, &asker, "open"),
            (
                2,
                &progress_ask,
                &second_claimant,
                &responder,
                "in_progress",
            ),
        ] {
            connection
                .execute(
                    "INSERT INTO messages
                     (id, sender_agent_id, recipient_agent_id, kind, body,
                      created_at_ms, creates_obligation)
                     VALUES (?1, ?2, ?3, 'ask', 'status?', 1, 1)",
                    params![ask.to_string(), waiting.to_string(), owing.to_string()],
                )
                .expect("ask message");
            connection
                .execute(
                    "INSERT INTO obligations
                     (ask_message_id, owing_agent_id, waiting_agent_id,
                      creation_sequence, created_at_ms, last_activity_at_ms, state)
                     VALUES (?1, ?2, ?3, ?4, 1, 1, ?5)",
                    params![
                        ask.to_string(),
                        owing.to_string(),
                        waiting.to_string(),
                        sequence,
                        state
                    ],
                )
                .expect("obligation");
        }
        (
            asker.to_string(),
            second_claimant.to_string(),
            open_ask.to_string(),
        )
    }

    #[test]
    fn name_info_reports_every_claimant_and_both_parties() {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = Store::open(directory.path().join("kelpie.sqlite3")).expect("store");
        let (asker, second_claimant, open_ask) = claimed_name_fixture(&store);

        let info = store.name_info("worker-x").expect("name info");
        assert_eq!(info.claimants.len(), 2);
        let lost = info
            .claimants
            .iter()
            .find(|claimant| claimant.logical_agent_id == asker)
            .expect("the lost claimant");
        assert!(!lost.has_ready_incarnation);
        assert_eq!(lost.unresolved_count, 1);
        let ready = info
            .claimants
            .iter()
            .find(|claimant| claimant.logical_agent_id == second_claimant)
            .expect("the ready claimant");
        assert!(ready.has_ready_incarnation);
        assert_eq!(ready.unresolved_count, 1);

        assert_eq!(info.unresolved.len(), 2);
        let ask = info
            .unresolved
            .iter()
            .find(|obligation| obligation.ask_message_id == open_ask)
            .expect("the open ask");
        assert_eq!(ask.asker_name, "worker-x");
        assert!(!ask.asker_live);
        assert_eq!(ask.responder_name, "helper");
        assert!(ask.responder_live);
        let progress = info
            .unresolved
            .iter()
            .find(|obligation| obligation.ask_message_id != open_ask)
            .expect("the in-progress ask");
        assert_eq!(progress.asker_name, "helper");
        assert!(progress.asker_live);
        assert_eq!(progress.responder_name, "worker-x");
        assert!(progress.responder_live);
    }

    #[test]
    fn create_new_refusal_names_the_asks_both_parties_and_three_remedies() {
        let info = NameInfo {
            public_name: "divine-work".into(),
            claimants: vec![NameClaimant {
                logical_agent_id: "01a00fd4-prior".into(),
                created_at_ms: 1,
                has_ready_incarnation: false,
                unresolved_count: 1,
            }],
            unresolved: vec![NameObligation {
                ask_message_id: "01a008ed-ask".into(),
                state: "in_progress".into(),
                asker_agent_id: "01a00352-asker".into(),
                asker_name: "divine-work".into(),
                asker_live: false,
                responder_agent_id: "01a008ed-responder".into(),
                responder_name: "nudge-design".into(),
                responder_live: false,
                created_at_ms: 1,
                last_activity_at_ms: 2,
            }],
        };
        let message = Store::name_conflict_message(&info);
        assert!(message.contains("1 unresolved obligation(s)"), "{message}");
        assert!(
            message.contains("ask 01a008ed-ask (in_progress)"),
            "{message}"
        );
        assert!(message.contains("responder nudge-design"), "{message}");
        assert!(message.contains("not live"), "{message}");
        assert!(message.contains("--logical-id 01a00fd4-prior"), "{message}");
        assert!(message.contains("kelpie cancel 01a008ed-ask"), "{message}");
        assert!(message.contains("--sender-id 01a00352-asker"), "{message}");
        assert!(message.contains("different name"), "{message}");
    }

    #[test]
    fn unready_alias_error_discloses_claimants_when_the_name_is_taken() {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = Store::open(directory.path().join("kelpie.sqlite3")).expect("store");
        claimed_name_fixture(&store);
        // One more name whose only holder is lost and owes an ask.
        let gone = LogicalAgentId::new();
        let gone_ask = MessageId::new();
        store
            .connection
            .execute(
                "INSERT INTO logical_agents
                 (id, public_name, explicitly_parentless, created_at_ms)
                 VALUES (?1, 'dead-name', 1, 9)",
                params![gone.to_string()],
            )
            .expect("logical agent");
        store
            .connection
            .execute(
                "INSERT INTO messages
                 (id, sender_agent_id, recipient_agent_id, kind, body,
                  created_at_ms, creates_obligation)
                 VALUES (?1, ?2, ?2, 'ask', 'status?', 1, 1)",
                params![gone_ask.to_string(), gone.to_string()],
            )
            .expect("ask message");
        store
            .connection
            .execute(
                "INSERT INTO obligations
                 (ask_message_id, owing_agent_id, waiting_agent_id,
                  creation_sequence, created_at_ms, last_activity_at_ms, state)
                 VALUES (?1, ?2, ?2, 3, 1, 1, 'open')",
                params![gone_ask.to_string(), gone.to_string()],
            )
            .expect("obligation");

        let error = store
            .resolve_ready_alias("dead-name")
            .expect_err("no Ready incarnation holds the name");
        let message = error.to_string();
        assert!(
            message.contains("1 logical agent(s) already hold"),
            "{message}"
        );
        assert!(message.contains("1 unresolved obligation(s)"), "{message}");
        assert!(message.contains("kelpie name-info dead-name"), "{message}");

        // A name with no claimants keeps the live-but-unadopted hint.
        let fresh = store
            .resolve_ready_alias("never-held")
            .expect_err("nothing holds this name");
        assert!(
            fresh.to_string().contains("a live Herdr agent may hold"),
            "{fresh}"
        );
    }

    #[test]
    fn reply_is_refused_from_anyone_but_the_owing_agent() {
        let directory = tempfile::tempdir().expect("tempdir");
        let mut store = Store::open(directory.path().join("kelpie.sqlite3")).expect("store");
        let waiting = store
            .declare_start(&intent("waiting", "term-waiting", "reply-owner-waiting"))
            .expect("declare waiting");
        let owing = store
            .declare_start(&intent("owing", "term-owing", "reply-owner-owing"))
            .expect("declare owing");
        mark_ready(&mut store, waiting, "waiting", "term-waiting");
        mark_ready(&mut store, owing, "owing", "term-owing");
        let ask = store
            .create_ask(
                waiting.logical_agent_id,
                owing.logical_agent_id,
                owing.incarnation_id,
                "question",
                "reply-owner-ask",
            )
            .expect("ask");

        // The asker replying to its own ask would have its words attributed to
        // the owing agent and delivered back to itself — forged provenance.
        let self_reply = store.create_reply_with_due(
            ask.message_id,
            waiting.logical_agent_id,
            "my own words as if the responder sent them",
            ReplyDisposition::Progress,
            "self-reply",
            None,
        );
        assert!(matches!(self_reply, Err(StoreError::Conflict(_))));

        // A third party is refused identically.
        let third_reply = store.create_reply_with_due(
            ask.message_id,
            LogicalAgentId::new(),
            "unrelated commentary",
            ReplyDisposition::Progress,
            "third-reply",
            None,
        );
        assert!(matches!(third_reply, Err(StoreError::Conflict(_))));

        // The obligation is untouched by both refusals, and the owing agent
        // can still reply.
        assert_eq!(
            store.obligation_state(ask.message_id).expect("state"),
            ObligationState::Open
        );
        assert!(
            store
                .create_reply_with_due(
                    ask.message_id,
                    owing.logical_agent_id,
                    "the actual answer",
                    ReplyDisposition::Final,
                    "owed-reply",
                    None,
                )
                .is_ok()
        );
    }

    #[test]
    fn cancellation_delivers_response_to_ready_asker() -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir().expect("tempdir");
        let mut store = Store::open(directory.path().join("kelpie.sqlite3")).expect("store");
        let waiting = store
            .declare_start(&intent("waiting", "term-waiting", "cancel-waiting"))
            .expect("declare waiting");
        let owing = store
            .declare_start(&intent("owing", "term-owing", "cancel-owing"))
            .expect("declare owing");
        mark_ready(&mut store, waiting, "waiting", "term-waiting");
        mark_ready(&mut store, owing, "owing", "term-owing");
        let ask = store
            .create_ask(
                waiting.logical_agent_id,
                owing.logical_agent_id,
                owing.incarnation_id,
                "status?",
                "cancel-delivered-ask",
            )
            .expect("ask");

        let created = store
            .cancel_with_response(
                waiting.logical_agent_id,
                ask.message_id,
                "obsolete question",
                "Your ask was cancelled. Reason: obsolete question.",
                "Stop. Ask was cancelled. Reason: obsolete question.",
                None,
                None,
            )
            .expect("cancel");
        let (operation_id, incarnation) = created.delivery.expect("Ready asker gets a delivery");
        assert_eq!(incarnation, waiting.incarnation_id);
        let (owing_operation_id, owing_incarnation) = created
            .owing_delivery
            .expect("Ready owing agent gets a stop-notice");
        assert_eq!(owing_incarnation, owing.incarnation_id);

        // The response is Kelpie's own message: kind cancellation, no sender.
        let (kind, sender): (String, Option<String>) = store
            .connection
            .query_row(
                "SELECT kind, sender_agent_id FROM messages WHERE id = ?1",
                [created.message_id.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("response message");
        assert_eq!(kind, "cancellation");
        assert_eq!(sender, None, "the response is attributed to nobody");
        let delivery_outcome: String = store
            .connection
            .query_row(
                "SELECT outcome FROM deliveries WHERE operation_id = ?1",
                [operation_id.to_string()],
                |row| row.get(0),
            )
            .expect("delivery");
        assert_eq!(delivery_outcome, "pending");
        // The deferred fire path renders from this intent, so it must produce
        // exactly the immediate path's envelope inputs: the original ask id and
        // the reason — never the response's own message id or its body text.
        let (render_ask, render_reason, audience) = store
            .cancellation_rendering_for_operation(operation_id)
            .expect("cancellation rendering from intent");
        assert_eq!(render_ask, ask.message_id);
        assert_eq!(render_reason, "obsolete question");
        assert_eq!(audience, CancellationAudience::Waiting);
        let (_, _, owing_audience) = store
            .cancellation_rendering_for_operation(owing_operation_id)
            .expect("owing cancellation rendering");
        assert_eq!(owing_audience, CancellationAudience::Owing);
        assert_eq!(
            store.obligation_state(ask.message_id).expect("state"),
            ObligationState::Cancelled
        );

        // While the response is undelivered the cancellation is surfaced; once
        // a pane has accepted it, revival surfacing stands down — the asker
        // already has the reason.
        //
        // This cancellation happened inside the current Ready binding, so the
        // while-away window — the span between the previous binding and this
        // one — does not contain it either way: the delivery itself is the
        // surface, and the window is for cancellations suffered while away.
        let away = store
            .cancelled_while_away(waiting.logical_agent_id)
            .expect("while away before acceptance");
        assert!(away.is_empty(), "current-binding cancel is not while-away");
        store.accept_delivery(operation_id, incarnation, "w1:p1", "term-waiting")?;
        let away = store
            .cancelled_while_away(waiting.logical_agent_id)
            .expect("while away after acceptance");
        assert!(away.is_empty(), "a delivered response must not re-surface");

        // A second cancel is refused: the obligation is terminal, so no
        // duplicate response can ever be composed, let alone written.
        assert!(
            store
                .cancel_with_response(
                    waiting.logical_agent_id,
                    ask.message_id,
                    "again",
                    "again",
                    "again",
                    None,
                    None,
                )
                .is_err()
        );
        Ok(())
    }

    #[test]
    fn cancellation_without_ready_asker_is_recorded_for_revival() {
        let directory = tempfile::tempdir().expect("tempdir");
        let mut store = Store::open(directory.path().join("kelpie.sqlite3")).expect("store");
        let waiting = store
            .declare_start(&intent("waiting", "term-waiting", "away-waiting"))
            .expect("declare waiting");
        let owing = store
            .declare_start(&intent("owing", "term-owing", "away-owing"))
            .expect("declare owing");
        mark_ready(&mut store, owing, "owing", "term-owing");
        let ask = store
            .create_ask(
                waiting.logical_agent_id,
                owing.logical_agent_id,
                owing.incarnation_id,
                "status?",
                "away-ask",
            )
            .expect("ask");

        let created = store
            .cancel_with_response(
                waiting.logical_agent_id,
                ask.message_id,
                "answered elsewhere",
                "Your ask was cancelled. Reason: answered elsewhere.",
                "Stop. Ask was cancelled. Reason: answered elsewhere.",
                None,
                None,
            )
            .expect("cancel");
        assert!(created.delivery.is_none(), "no Ready asker to deliver to");
        assert!(
            created.owing_delivery.is_some(),
            "Ready owing agent still gets a stop-notice"
        );

        // No incarnation at all: every cancellation is visible.
        let away = store
            .cancelled_while_away(waiting.logical_agent_id)
            .expect("while away");
        assert_eq!(away.len(), 1);
        assert_eq!(away[0].reason, "answered elsewhere");

        // Revived onto an incarnation created after the cancellation, the
        // cancellation — which happened while the agent was away — is still the
        // first thing seen.
        let cancelled_at: i64 = store
            .connection
            .query_row(
                "SELECT last_activity_at_ms FROM obligations WHERE ask_message_id = ?1",
                [ask.message_id.to_string()],
                |row| row.get::<_, i64>(0),
            )
            .expect("cancelled obligation");
        store
            .connection
            .execute(
                "INSERT INTO incarnations (
                    id, logical_agent_id, herdr_session, intended_pane_id,
                    expected_terminal_id, backend_kind, backend_args_json,
                    working_directory, created_at_ms, state
                 ) VALUES (?1, ?2, 's', 'w1:p1', 'term-waiting', 'codex', '[]', '/tmp', ?3, 'ready')",
                params![
                    IncarnationId::new().to_string(),
                    waiting.logical_agent_id.to_string(),
                    cancelled_at + 1
                ],
            )
            .expect("revival incarnation");
        let revived = store
            .cancelled_while_away(waiting.logical_agent_id)
            .expect("while away after revival");
        assert_eq!(revived.len(), 1);
        assert_eq!(revived[0].ask_message_id, ask.message_id.to_string());
    }

    #[test]
    fn cancellation_without_ready_owing_is_recorded_for_revival() {
        let directory = tempfile::tempdir().expect("tempdir");
        let mut store = Store::open(directory.path().join("kelpie.sqlite3")).expect("store");
        let waiting = store
            .declare_start(&intent("waiting", "term-waiting", "owing-away-waiting"))
            .expect("declare waiting");
        let owing = store
            .declare_start(&intent("owing", "term-owing", "owing-away-owing"))
            .expect("declare owing");
        mark_ready(&mut store, waiting, "waiting", "term-waiting");
        let ask = store
            .create_ask(
                waiting.logical_agent_id,
                owing.logical_agent_id,
                owing.incarnation_id,
                "status?",
                "owing-away-ask",
            )
            .expect("ask");

        let created = store
            .cancel_with_response(
                waiting.logical_agent_id,
                ask.message_id,
                "stand down",
                "Your ask was cancelled. Reason: stand down.",
                "Stop. Ask was cancelled. Reason: stand down.",
                None,
                None,
            )
            .expect("cancel");
        assert!(
            created.owing_delivery.is_none(),
            "no Ready owing to deliver to"
        );
        assert!(
            created.delivery.is_some(),
            "Ready asker still gets a response"
        );

        let away = store
            .cancelled_owing_while_away(owing.logical_agent_id)
            .expect("owing while away");
        assert_eq!(away.len(), 1);
        assert_eq!(away[0].reason, "stand down");
        assert_eq!(away[0].ask_message_id, ask.message_id.to_string());

        let cancelled_at: i64 = store
            .connection
            .query_row(
                "SELECT last_activity_at_ms FROM obligations WHERE ask_message_id = ?1",
                [ask.message_id.to_string()],
                |row| row.get::<_, i64>(0),
            )
            .expect("cancelled obligation");
        store
            .connection
            .execute(
                "INSERT INTO incarnations (
                    id, logical_agent_id, herdr_session, intended_pane_id,
                    expected_terminal_id, backend_kind, backend_args_json,
                    working_directory, created_at_ms, state
                 ) VALUES (?1, ?2, 's', 'w1:p2', 'term-owing', 'codex', '[]', '/tmp', ?3, 'ready')",
                params![
                    IncarnationId::new().to_string(),
                    owing.logical_agent_id.to_string(),
                    cancelled_at + 1
                ],
            )
            .expect("owing revival incarnation");
        let revived = store
            .cancelled_owing_while_away(owing.logical_agent_id)
            .expect("owing while away after revival");
        assert_eq!(revived.len(), 1);
        assert_eq!(revived[0].ask_message_id, ask.message_id.to_string());
    }

    #[test]
    fn accepted_or_absent_owing_notice_does_not_resurface() {
        let directory = tempfile::tempdir().expect("tempdir");
        let mut store = Store::open(directory.path().join("kelpie.sqlite3")).expect("store");
        let waiting = store
            .declare_start(&intent("waiting", "term-waiting", "owing-excl-waiting"))
            .expect("declare waiting");
        let owing = store
            .declare_start(&intent("owing", "term-owing", "owing-excl-owing"))
            .expect("declare owing");
        mark_ready(&mut store, waiting, "waiting", "term-waiting");
        let ask = store
            .create_ask(
                waiting.logical_agent_id,
                owing.logical_agent_id,
                owing.incarnation_id,
                "status?",
                "owing-excl-ask",
            )
            .expect("ask");
        let created = store
            .cancel_with_response(
                waiting.logical_agent_id,
                ask.message_id,
                "stand down",
                "Your ask was cancelled. Reason: stand down.",
                "Stop. Ask was cancelled. Reason: stand down.",
                None,
                None,
            )
            .expect("cancel");
        let cancelled_at: i64 = store
            .connection
            .query_row(
                "SELECT last_activity_at_ms FROM obligations WHERE ask_message_id = ?1",
                [ask.message_id.to_string()],
                |row| row.get::<_, i64>(0),
            )
            .expect("cancelled obligation");
        store
            .connection
            .execute(
                "INSERT INTO incarnations (
                    id, logical_agent_id, herdr_session, intended_pane_id,
                    expected_terminal_id, backend_kind, backend_args_json,
                    working_directory, created_at_ms, state
                 ) VALUES (?1, ?2, 's', 'w1:p2', 'term-owing', 'codex', '[]', '/tmp', ?3, 'ready')",
                params![
                    IncarnationId::new().to_string(),
                    owing.logical_agent_id.to_string(),
                    cancelled_at + 1
                ],
            )
            .expect("owing revival incarnation");
        store
            .connection
            .execute(
                "INSERT INTO deliveries
                 (message_id, delivery_transport, recipient_incarnation_id,
                  recipient_agent_id, attempt_number, scheduled_at_ms, outcome)
                 VALUES (?1, 'socket_inbox', NULL, ?2, 1, ?3, 'accepted')",
                params![
                    created.owing_message_id.to_string(),
                    owing.logical_agent_id.to_string(),
                    cancelled_at
                ],
            )
            .expect("accepted owing notice");
        let after_accept = store
            .cancelled_owing_while_away(owing.logical_agent_id)
            .expect("owing after accepted notice");
        assert!(
            after_accept.is_empty(),
            "an accepted owing notice must not re-surface"
        );

        store
            .connection
            .execute(
                "UPDATE obligations SET cancellation_owing_message_id = NULL
                 WHERE ask_message_id = ?1",
                [ask.message_id.to_string()],
            )
            .expect("pre-upgrade null owing notice");
        store
            .connection
            .execute(
                "DELETE FROM deliveries WHERE message_id = ?1",
                [created.owing_message_id.to_string()],
            )
            .expect("drop synthetic delivery");
        let pre_upgrade = store
            .cancelled_owing_while_away(owing.logical_agent_id)
            .expect("owing with null notice id");
        assert!(
            pre_upgrade.is_empty(),
            "a cancel from before owing notices existed must not surface as a stop-notice"
        );
    }

    fn unnamed_intent(key: &str) -> crate::domain::AdoptIntent {
        crate::domain::AdoptIntent {
            pane_id: "w7:p1H".into(),
            expected_terminal_id: "term-coord".into(),
            public_name: None,
            logical_agent_id: None,
            parent: Parent::Parentless,
            herdr_session: "default".into(),
            backend_kind: Some("codex".into()),
            backend_args: Vec::new(),
            requested_model: None,
            requested_provider: None,
            requested_effort: None,
            idempotency_key: key.into(),
        }
    }

    fn unnamed_live_agent() -> crate::herdr::AgentObservation {
        crate::herdr::AgentObservation {
            terminal_id: "term-coord".into(),
            pane_id: "w7:p1H".into(),
            name: None,
            agent: Some("codex".into()),
            interactive_ready: false,
            launch_pending: false,
            agent_session: Some(serde_json::json!({
                "agent":"codex","kind":"id","value":"sess-coord"
            })),
        }
    }

    #[test]
    fn declare_adopt_rejects_empty_live_name() {
        let mut store = Store::in_memory().expect("store");
        assert!(matches!(
            store.declare_adopt(&unnamed_intent("adopt-empty"), &unnamed_evidence()),
            Err(StoreError::Conflict(_))
        ));
    }

    #[test]
    fn pending_name_claim_becomes_ready_only_after_named_snapshot() {
        let mut store = Store::in_memory().expect("store");
        let mut evidence = unnamed_evidence();
        evidence.public_name = "quorum".into();
        let declared = store
            .declare_adopt_pending(&unnamed_intent("adopt-claim"), &evidence)
            .expect("pending");
        assert_eq!(
            store
                .incarnation_state(declared.incarnation_id)
                .expect("state"),
            crate::domain::IncarnationState::Declared
        );
        store
            .begin_attempt(
                declared.operation_id,
                declared.incarnation_id,
                "kelpie:adopt-rename:test",
            )
            .expect("attempt");
        let mut named = unnamed_live_agent();
        named.name = Some("quorum".into());
        store
            .accept_adopt_ready(declared.operation_id, declared.incarnation_id, &named)
            .expect("ready after claim");
        assert_eq!(
            store
                .incarnation_state(declared.incarnation_id)
                .expect("state"),
            crate::domain::IncarnationState::Ready
        );
        let again = store
            .declared_by_idempotency_key("adopt-claim")
            .expect("lookup")
            .expect("present");
        assert_eq!(again, declared);
        assert!(matches!(
            store.declare_adopt_pending(&unnamed_intent("adopt-claim"), &evidence),
            Err(StoreError::Conflict(_))
        ));
    }

    #[test]
    fn pending_name_claim_unknown_when_snapshot_still_unnamed() {
        let mut store = Store::in_memory().expect("store");
        let mut evidence = unnamed_evidence();
        evidence.public_name = "quorum".into();
        let declared = store
            .declare_adopt_pending(&unnamed_intent("adopt-unknown"), &evidence)
            .expect("pending");
        store
            .begin_attempt(
                declared.operation_id,
                declared.incarnation_id,
                "kelpie:adopt-rename:unknown",
            )
            .expect("attempt");
        store
            .mark_submitted(declared.operation_id, 1, "kelpie:adopt-rename:unknown")
            .expect("submitted");
        let report = store
            .reconcile(&Snapshot {
                protocol: 20,
                panes: vec![],
                agents: vec![unnamed_live_agent()],
            })
            .expect("recover unnamed after attempted claim");
        assert_eq!(report.outcomes_marked_unknown, 1);
        assert_eq!(
            store
                .incarnation_state(declared.incarnation_id)
                .expect("state"),
            crate::domain::IncarnationState::Unknown
        );
    }

    #[test]
    fn pending_name_claim_recovers_when_snapshot_shows_claimed_name() {
        let mut store = Store::in_memory().expect("store");
        let mut evidence = unnamed_evidence();
        evidence.public_name = "quorum".into();
        let declared = store
            .declare_adopt_pending(&unnamed_intent("adopt-recover"), &evidence)
            .expect("pending");
        store
            .begin_attempt(
                declared.operation_id,
                declared.incarnation_id,
                "kelpie:adopt-rename:recover",
            )
            .expect("attempt");
        store
            .mark_submitted(declared.operation_id, 1, "kelpie:adopt-rename:recover")
            .expect("submitted");
        let mut named = unnamed_live_agent();
        named.name = Some("quorum".into());
        store
            .reconcile(&Snapshot {
                protocol: 20,
                panes: vec![],
                agents: vec![named],
            })
            .expect("recover claimed name");
        assert_eq!(
            store
                .incarnation_state(declared.incarnation_id)
                .expect("state"),
            crate::domain::IncarnationState::Ready
        );
    }

    #[test]
    fn accept_adopt_ready_rejects_name_mismatch() {
        let mut store = Store::in_memory().expect("store");
        let mut evidence = unnamed_evidence();
        evidence.public_name = "quorum".into();
        let declared = store
            .declare_adopt_pending(&unnamed_intent("adopt-mismatch"), &evidence)
            .expect("pending");
        store
            .begin_attempt(
                declared.operation_id,
                declared.incarnation_id,
                "kelpie:adopt-rename:mismatch",
            )
            .expect("attempt");
        let mut other = unnamed_live_agent();
        other.name = Some("other".into());
        assert!(matches!(
            store.accept_adopt_ready(declared.operation_id, declared.incarnation_id, &other),
            Err(StoreError::Conflict(_))
        ));
    }

    #[test]
    fn adopt_named_occupant_is_lost_on_terminal_or_backend_replacement() {
        fn live() -> crate::herdr::AgentObservation {
            crate::herdr::AgentObservation {
                terminal_id: "term-live".into(),
                pane_id: "w1:p9".into(),
                name: Some("preexisting".into()),
                agent: Some("grok".into()),
                interactive_ready: true,
                launch_pending: false,
                agent_session: ready_evidence().native_agent_session,
            }
        }
        let mut store = Store::in_memory().expect("store");
        let terminal = store
            .declare_adopt(&adopt_intent("rep-term"), &ready_evidence())
            .expect("adopt");
        let mut replaced_terminal = live();
        replaced_terminal.terminal_id = "term-other".into();
        assert_eq!(
            store
                .reconcile(&Snapshot {
                    protocol: 20,
                    panes: vec![],
                    agents: vec![replaced_terminal],
                })
                .expect("terminal")
                .incarnations_marked_lost,
            1
        );
        assert_eq!(
            store
                .incarnation_state(terminal.incarnation_id)
                .expect("state"),
            crate::domain::IncarnationState::Lost
        );

        let mut store = Store::in_memory().expect("store");
        let backend = store
            .declare_adopt(&adopt_intent("rep-backend"), &ready_evidence())
            .expect("adopt");
        let mut replaced_backend = live();
        replaced_backend.agent = Some("codex".into());
        assert_eq!(
            store
                .reconcile(&Snapshot {
                    protocol: 20,
                    panes: vec![],
                    agents: vec![replaced_backend],
                })
                .expect("backend")
                .incarnations_marked_lost,
            1
        );
        assert_eq!(
            store
                .incarnation_state(backend.incarnation_id)
                .expect("state"),
            crate::domain::IncarnationState::Lost
        );

        // A rotated conversation used to belong in this list. It does not:
        // an adopted agent that clears, resumes, compacts, or forks reports a
        // different session while occupying the same pane and terminal under
        // the same name, and losing it there de-addressed live agents by the
        // dozen. The reference is refreshed instead.
        let mut store = Store::in_memory().expect("store");
        let session = store
            .declare_adopt(&adopt_intent("rep-session"), &ready_evidence())
            .expect("adopt");
        let mut rotated_session = live();
        rotated_session.agent_session = Some(serde_json::json!({
            "agent":"grok","kind":"id","value":"sess-other"
        }));
        let report = store
            .reconcile(&Snapshot {
                protocol: 20,
                panes: vec![],
                agents: vec![rotated_session],
            })
            .expect("session");
        assert_eq!(report.incarnations_marked_lost, 0);
        assert_eq!(report.native_sessions_refreshed, 1);
        assert_eq!(
            store
                .incarnation_state(session.incarnation_id)
                .expect("state"),
            crate::domain::IncarnationState::Ready
        );
    }

    #[test]
    fn adopt_named_occupant_still_requires_live_name() {
        let mut store = Store::in_memory().expect("store");
        let adopted = store
            .declare_adopt(&adopt_intent("adopt-named"), &ready_evidence())
            .expect("adopt named");
        let mut unnamed = crate::herdr::AgentObservation {
            terminal_id: "term-live".into(),
            pane_id: "w1:p9".into(),
            name: None,
            agent: Some("grok".into()),
            interactive_ready: true,
            launch_pending: false,
            agent_session: ready_evidence().native_agent_session,
        };
        let report = store
            .reconcile(&Snapshot {
                protocol: 20,
                panes: vec![],
                agents: vec![unnamed.clone()],
            })
            .expect("recover missing name");
        assert_eq!(report.incarnations_marked_lost, 1);
        assert_eq!(
            store
                .incarnation_state(adopted.incarnation_id)
                .expect("state"),
            crate::domain::IncarnationState::Lost
        );

        let mut store = Store::in_memory().expect("store");
        let adopted = store
            .declare_adopt(&adopt_intent("adopt-named-keep"), &ready_evidence())
            .expect("adopt named");
        unnamed.name = Some("preexisting".into());
        let report = store
            .reconcile(&Snapshot {
                protocol: 20,
                panes: vec![],
                agents: vec![unnamed],
            })
            .expect("recover exact name");
        assert_eq!(report.incarnations_marked_lost, 0);
        assert_eq!(
            store
                .incarnation_state(adopted.incarnation_id)
                .expect("state"),
            crate::domain::IncarnationState::Ready
        );
    }

    #[test]
    fn version_six_store_migrates_to_observed_name_authority() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("v6.sqlite3");
        let agent_id = LogicalAgentId::new();
        let incarnation_id = IncarnationId::new();
        let operation_id = OperationId::new();
        let session = serde_json::json!({"agent":"codex","kind":"id","value":"sess-coord"});
        {
            let connection = Connection::open(&path).expect("db");
            for migration in [
                include_str!("../migrations/001_initial.sql"),
                include_str!("../migrations/002_operator_notices.sql"),
                include_str!("../migrations/003_operator_message_sender.sql"),
                include_str!("../migrations/004_obligation_creation_sequence.sql"),
                include_str!("../migrations/005_obligation_cancellation.sql"),
                include_str!("../migrations/006_adopt_operation.sql"),
            ] {
                connection.execute_batch(migration).expect("migrate step");
            }
            connection
                .execute(
                    "INSERT INTO logical_agents
                     (id, public_name, explicitly_parentless, created_at_ms)
                     VALUES (?1, 'adopted-w7-p1H', 1, 1)",
                    [agent_id.to_string()],
                )
                .expect("agent");
            connection
                .execute(
                    "INSERT INTO incarnations (
                        id, logical_agent_id, herdr_session, intended_pane_id,
                        expected_terminal_id, observed_pane_id, observed_terminal_id,
                        backend_kind, backend_args_json, working_directory,
                        created_at_ms, state
                     ) VALUES (?1, ?2, 'default', 'w7:p1H', 'term-coord',
                               'w7:p1H', 'term-coord', 'codex', '[]', '/tmp', 1, 'ready')",
                    params![incarnation_id.to_string(), agent_id.to_string()],
                )
                .expect("incarnation");
            let intent = serde_json::json!({
                "adopt": {
                    "pane_id": "w7:p1H",
                    "expected_terminal_id": "term-coord",
                    "parent": {"kind":"parentless"},
                    "herdr_session": "default",
                    "backend_kind": "codex",
                    "idempotency_key": "legacy-unnamed"
                },
                "evidence": {
                    "pane_id": "w7:p1H",
                    "terminal_id": "term-coord",
                    "public_name": "adopted-w7-p1H",
                    "backend_kind": "codex",
                    "working_directory": "/tmp",
                    "herdr_session": "default",
                    "native_agent_session": session
                }
            });
            connection
                .execute(
                    "INSERT INTO operations (
                        id, idempotency_key, kind, target_incarnation_id, intent_json,
                        created_at_ms, resolved_at_ms, outcome
                     ) VALUES (?1, 'legacy-unnamed', 'adopt', ?2, ?3, 1, 1, 'succeeded')",
                    params![
                        operation_id.to_string(),
                        incarnation_id.to_string(),
                        intent.to_string()
                    ],
                )
                .expect("operation");
        }
        let mut store = Store::open(&path).expect("open migrates to v9");
        let version: i64 = store
            .connection
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .expect("version");
        assert_eq!(version, SCHEMA_VERSION);
        let authority: String = store
            .connection
            .query_row(
                "SELECT name_authority FROM incarnations WHERE id = ?1",
                [incarnation_id.to_string()],
                |row| row.get(0),
            )
            .expect("authority");
        assert_eq!(authority, "observed");
        let report = store
            .reconcile(&Snapshot {
                protocol: 20,
                panes: vec![],
                agents: vec![unnamed_live_agent()],
            })
            .expect("recover requires live name");
        assert_eq!(report.incarnations_marked_lost, 1);
        assert_eq!(
            store.incarnation_state(incarnation_id).expect("state"),
            crate::domain::IncarnationState::Lost
        );
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn version_sixteen_store_settles_prepare_asks_of_ended_cycles() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("v16.sqlite3");
        let agent_id = LogicalAgentId::new();
        let incarnation_id = IncarnationId::new();
        let ended_ask = MessageId::new();
        let live_ask = MessageId::new();
        {
            let connection = Connection::open(&path).expect("db");
            for migration in [
                include_str!("../migrations/001_initial.sql"),
                include_str!("../migrations/002_operator_notices.sql"),
                include_str!("../migrations/003_operator_message_sender.sql"),
                include_str!("../migrations/004_obligation_creation_sequence.sql"),
                include_str!("../migrations/005_obligation_cancellation.sql"),
                include_str!("../migrations/006_adopt_operation.sql"),
                include_str!("../migrations/007_name_authority.sql"),
                include_str!("../migrations/008_scheduled_delivery.sql"),
                include_str!("../migrations/009_observed_attribution.sql"),
                include_str!("../migrations/010_obligation_reminders.sql"),
                include_str!("../migrations/011_pending_rename.sql"),
                include_str!("../migrations/012_renew.sql"),
                include_str!("../migrations/013_conversation_age.sql"),
                include_str!("../migrations/014_renew_clear_stall.sql"),
                include_str!("../migrations/015_lazy_rotation.sql"),
                include_str!("../migrations/016_clear_operation.sql"),
            ] {
                connection.execute_batch(migration).expect("migrate step");
            }
            connection
                .execute(
                    "INSERT INTO logical_agents
                     (id, public_name, explicitly_parentless, created_at_ms)
                     VALUES (?1, 'worker', 1, 1)",
                    [agent_id.to_string()],
                )
                .expect("agent");
            connection
                .execute(
                    "INSERT INTO incarnations (
                        id, logical_agent_id, herdr_session, intended_pane_id,
                        expected_terminal_id, backend_kind, backend_args_json,
                        working_directory, created_at_ms, state
                     ) VALUES (?1, ?2, 's', 'w1:p1', 't1', 'claude', '[]', '/tmp', 1, 'ready')",
                    params![incarnation_id.to_string(), agent_id.to_string()],
                )
                .expect("incarnation");
            // Two cycles of one policy: the first ended without an answer, the
            // second is still waiting for one.
            for (sequence, (ask, cycle, phase, resolved)) in [
                (ended_ask, 90, "aborted", Some(2)),
                (live_ask, 91, "preparing", None),
            ]
            .into_iter()
            .enumerate()
            {
                connection
                    .execute(
                        "INSERT INTO messages
                         (id, sender_agent_id, recipient_agent_id, kind, body,
                          created_at_ms, creates_obligation)
                         VALUES (?1, ?2, ?2, 'ask', 'save your progress', 1, 1)",
                        params![ask.to_string(), agent_id.to_string()],
                    )
                    .expect("prepare ask");
                connection
                    .execute(
                        "INSERT INTO obligations
                         (ask_message_id, owing_agent_id, waiting_agent_id,
                          creation_sequence, created_at_ms, last_activity_at_ms, state)
                         VALUES (?1, ?2, ?2, ?3, 1, 1, 'open')",
                        params![
                            ask.to_string(),
                            agent_id.to_string(),
                            i64::try_from(sequence).expect("sequence") + 1
                        ],
                    )
                    .expect("obligation");
                connection
                    .execute(
                        "INSERT INTO renews
                         (id, logical_agent_id, incarnation_id, requester_agent_id,
                          prepare_prompt, resume_prompt, on_timeout, prepare_timeout_ms,
                          every_ms, cycle, scheduled_at_ms, phase, ask_message_id,
                          created_at_ms, resolved_at_ms)
                         VALUES (?1, ?2, ?3, ?2, 'save', 'resume', 'abort', 1000,
                                 3600000, ?4, 1, ?5, ?6, 1, ?7)",
                        params![
                            RenewId::new().to_string(),
                            agent_id.to_string(),
                            incarnation_id.to_string(),
                            cycle,
                            phase,
                            ask.to_string(),
                            resolved
                        ],
                    )
                    .expect("renew");
            }
        }
        let store = Store::open(&path).expect("open migrates to v17");
        let version: i64 = store
            .connection
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .expect("version");
        assert_eq!(version, SCHEMA_VERSION);

        // The ended cycle's ask is settled, with the reason saying why rather
        // than claiming a reply arrived.
        assert_eq!(
            store.obligation_state(ended_ask).expect("state"),
            ObligationState::Cancelled
        );
        let (requester, reason): (String, String) = store
            .connection
            .query_row(
                "SELECT cancellation_requester_agent_id, cancellation_reason
                 FROM obligations WHERE ask_message_id = ?1",
                [ended_ask.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("cancellation");
        assert_eq!(requester, agent_id.to_string());
        assert!(
            reason.contains("renew cycle 90 ended in aborted"),
            "{reason}"
        );

        // The cycle still running keeps its obligation: the gate that lets an
        // agent authorise its own clear is exactly this ask staying open.
        assert_eq!(
            store.obligation_state(live_ask).expect("state"),
            ObligationState::Open
        );
    }

    #[test]
    fn socket_waiter_is_pane_less_and_can_wait_on_an_ask() {
        let mut store = Store::in_memory().expect("store");
        let waiter = store
            .register_socket_waiter("inbox", Parent::Parentless, "waiter-1")
            .expect("register");
        assert_eq!(
            store
                .delivery_transport(waiter.logical_agent_id)
                .expect("transport"),
            DeliveryTransport::SocketInbox
        );
        let incarnations: i64 = store
            .connection
            .query_row(
                "SELECT COUNT(*) FROM incarnations WHERE logical_agent_id = ?1",
                [waiter.logical_agent_id.to_string()],
                |row| row.get(0),
            )
            .expect("count");
        assert_eq!(incarnations, 0);

        let owing = store
            .declare_start(&intent("owing", "term-b", "owing-start"))
            .expect("owing");
        mark_ready(&mut store, owing, "owing", "term-b");
        let ask = store
            .create_ask_with_schedule(
                waiter.logical_agent_id,
                owing.logical_agent_id,
                owing.incarnation_id,
                "question",
                "waiter-ask",
                None,
                None,
                true,
            )
            .expect("ask");
        let waiting: String = store
            .connection
            .query_row(
                "SELECT waiting_agent_id FROM obligations WHERE ask_message_id = ?1",
                [ask.message_id.to_string()],
                |row| row.get(0),
            )
            .expect("waiting");
        assert_eq!(waiting, waiter.logical_agent_id.to_string());
        let sender: Option<String> = store
            .connection
            .query_row(
                "SELECT sender_agent_id FROM messages WHERE id = ?1",
                [ask.message_id.to_string()],
                |row| row.get(0),
            )
            .expect("sender");
        assert_eq!(sender, None);
        assert_eq!(
            store.agent_address(waiter.logical_agent_id).expect("from="),
            "inbox"
        );

        store
            .record_socket_inbox_delivery(
                ask.message_id,
                waiter.logical_agent_id,
                DeliveryOutcome::Queued,
            )
            .expect("socket delivery");
        let (transport, incarnation, agent): (String, Option<String>, String) = store
            .connection
            .query_row(
                "SELECT delivery_transport, recipient_incarnation_id, recipient_agent_id
                 FROM deliveries
                 WHERE message_id = ?1 AND delivery_transport = 'socket_inbox'",
                [ask.message_id.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .expect("delivery");
        assert_eq!(transport, "socket_inbox");
        assert_eq!(incarnation, None);
        assert_eq!(agent, waiter.logical_agent_id.to_string());
    }

    #[test]
    fn operator_cannot_be_the_waiting_agent_and_absent_ids_cannot_wait() {
        let mut store = Store::in_memory().expect("store");
        let owing = store
            .declare_start(&intent("owing", "term-b", "owing-op"))
            .expect("owing");
        mark_ready(&mut store, owing, "owing", "term-b");
        let error = store
            .create_ask_with_schedule(
                owing.logical_agent_id,
                owing.logical_agent_id,
                owing.incarnation_id,
                "q",
                "operator-as-waiter",
                None,
                None,
                true,
            )
            .expect_err("pane agent is not a waiter");
        assert!(
            error
                .to_string()
                .contains("operator attribution does not make operator the waiter"),
            "{error}"
        );
        let missing = LogicalAgentId::new();
        let absent = store
            .create_ask(
                missing,
                owing.logical_agent_id,
                owing.incarnation_id,
                "q",
                "connection-id",
            )
            .expect_err("unknown id");
        assert!(absent.to_string().contains("waiting agent"), "{absent}");
    }

    #[test]
    fn start_and_rename_refuse_a_socket_waiter_name_and_identity() {
        let mut store = Store::in_memory().expect("store");
        let waiter = store
            .register_socket_waiter("held", Parent::Parentless, "held-waiter")
            .expect("register");
        let start = store
            .declare_start(&intent("held", "term-1", "taken-name"))
            .expect_err("name");
        assert!(start.to_string().contains("socket waiter"), "{start}");
        let continue_err = {
            let mut continued = intent("other", "term-2", "continue-socket");
            continued.logical_agent_id = Some(waiter.logical_agent_id);
            store.declare_start(&continued).expect_err("continue")
        };
        assert!(
            continue_err.to_string().contains("socket-inbox"),
            "{continue_err}"
        );
        store
            .end_socket_waiter(waiter.logical_agent_id)
            .expect("end");
        store
            .declare_start(&intent("held", "term-3", "name-free"))
            .expect("name released");
    }

    #[test]
    fn queued_operator_ask_keeps_waiter_as_envelope_sender_and_cancels() {
        let mut store = Store::in_memory().expect("store");
        let waiter = store
            .register_socket_waiter("inbox", Parent::Parentless, "queued-waiter")
            .expect("register");
        let owing = store
            .declare_start(&intent("owing", "term-b", "queued-owing"))
            .expect("owing");
        mark_ready(&mut store, owing, "owing", "term-b");
        let due_at = store_clock_ms().expect("clock") + 1_000;
        let ask = store
            .create_ask_with_schedule(
                waiter.logical_agent_id,
                owing.logical_agent_id,
                owing.incarnation_id,
                "later?",
                "queued-from-operator",
                Some(due_at),
                None,
                true,
            )
            .expect("queued ask");
        let due = store.due_deliveries(due_at).expect("due");
        let item = due
            .iter()
            .find(|item| item.message_id == ask.message_id)
            .expect("queued ask is due");
        assert_eq!(item.sender, Some(waiter.logical_agent_id));
        assert!(
            store
                .cancel_queued_delivery(waiter.logical_agent_id, ask.message_id, "host down")
                .expect("cancel")
        );
    }

    #[test]
    fn register_refuses_a_ready_alias_and_mismatched_or_retired_replay() {
        let mut store = Store::in_memory().expect("store");
        store
            .declare_start(&intent("pending", "term-1", "pending-start"))
            .expect("declared");
        store
            .register_socket_waiter("pending", Parent::Parentless, "against-declared")
            .expect("declared is not a live alias");
        let ready = store
            .declare_start(&intent("live", "term-2", "live-start"))
            .expect("live");
        mark_ready(&mut store, ready, "live", "term-2");
        let taken = store
            .register_socket_waiter("live", Parent::Parentless, "against-ready")
            .expect_err("ready");
        assert!(taken.to_string().contains("live"), "{taken}");
        let first = store
            .register_socket_waiter("inbox", Parent::Parentless, "same-key")
            .expect("first");
        let mismatch = store
            .register_socket_waiter("other", Parent::Parentless, "same-key")
            .expect_err("mismatch");
        assert!(mismatch.to_string().contains("idempotency"), "{mismatch}");
        store
            .end_socket_waiter(first.logical_agent_id)
            .expect("end");
        store
            .register_socket_waiter("inbox", Parent::Parentless, "new-key")
            .expect("name released");
        let retired = store
            .register_socket_waiter("inbox", Parent::Parentless, "same-key")
            .expect_err("retired replay");
        assert!(retired.to_string().contains("ended waiter"), "{retired}");
        let named = store
            .declare_start(&intent("old", "term-3", "rename-then-lost"))
            .expect("named");
        mark_ready(&mut store, named, "old", "term-3");
        store
            .declare_rename(named.incarnation_id, "target")
            .expect("pending rename");
        store
            .reconcile(&Snapshot {
                protocol: 20,
                panes: vec![],
                agents: vec![],
            })
            .expect("lost");
        store
            .register_socket_waiter("target", Parent::Parentless, "after-lost-rename")
            .expect("lost pending rename does not hold the name");
    }

    #[test]
    fn socket_inbox_queues_drain_and_ack_by_waiter_id() {
        let mut store = Store::in_memory().expect("store");
        let waiter = store
            .register_socket_waiter("inbox", Parent::Parentless, "drain-waiter")
            .expect("register");
        let other = store
            .register_socket_waiter("other", Parent::Parentless, "other-waiter")
            .expect("other");
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
        store
            .claim_socket_waiter(waiter.logical_agent_id)
            .expect("claim");
        let queued = store
            .queued_socket_inbox_deliveries(waiter.logical_agent_id)
            .expect("drain");
        assert_eq!(queued.len(), 1);
        assert_eq!(queued[0].message_id, reply);
        assert_eq!(queued[0].body, "later reply body");
        assert_eq!(queued[0].kind, MessageKind::Reply);
        assert_eq!(queued[0].reply_to, Some(ask));
        assert_eq!(queued[0].disposition, Some(ReplyDisposition::Final));
        assert_eq!(
            store
                .ack_socket_inbox_delivery(waiter.logical_agent_id, reply)
                .expect("ack"),
            DeliveryOutcome::Accepted
        );
        assert!(
            store
                .queued_socket_inbox_deliveries(waiter.logical_agent_id)
                .expect("empty")
                .is_empty()
        );
        assert_eq!(
            store
                .ack_socket_inbox_delivery(waiter.logical_agent_id, reply)
                .expect("idempotent"),
            DeliveryOutcome::Accepted
        );
        let missing = LogicalAgentId::new();
        assert!(
            store
                .claim_socket_waiter(missing)
                .expect_err("absent")
                .to_string()
                .contains("absent")
        );
        let pane = store
            .declare_start(&intent("owing", "term-b", "pane-not-waiter"))
            .expect("pane");
        mark_ready(&mut store, pane, "owing", "term-b");
        assert!(
            store
                .claim_socket_waiter(pane.logical_agent_id)
                .expect_err("pane")
                .to_string()
                .contains("not an active socket waiter")
        );
        let stolen = store
            .ack_socket_inbox_delivery(other.logical_agent_id, reply)
            .expect_err("other waiter");
        assert!(
            stolen.to_string().contains("no socket-inbox delivery"),
            "{stolen}"
        );
        store
            .end_socket_waiter(waiter.logical_agent_id)
            .expect("end");
        assert!(
            store
                .claim_socket_waiter(waiter.logical_agent_id)
                .expect_err("ended")
                .to_string()
                .contains("not an active socket waiter")
        );
    }

    #[test]
    fn socket_inbox_final_resolves_only_on_ack() {
        let mut store = Store::in_memory().expect("store");
        let waiter = store
            .register_socket_waiter("inbox", Parent::Parentless, "ack-waiter")
            .expect("register");
        let owing = store
            .declare_start(&intent("owing", "term-b", "ack-owing"))
            .expect("owing");
        mark_ready(&mut store, owing, "owing", "term-b");
        let ask = store
            .create_ask_with_schedule(
                waiter.logical_agent_id,
                owing.logical_agent_id,
                owing.incarnation_id,
                "question",
                "ack-ask",
                None,
                None,
                true,
            )
            .expect("ask");
        let progress = store
            .create_reply(
                ask.message_id,
                owing.logical_agent_id,
                "working",
                ReplyDisposition::Progress,
                "ack-progress",
            )
            .expect("progress");
        assert!(progress.operation_id.is_none());
        assert!(progress.recipient_incarnation.is_none());
        assert_eq!(
            store.obligation_state(ask.message_id).expect("progress"),
            ObligationState::InProgress
        );
        assert_eq!(
            store
                .delivery_outcome_for_message(progress.message_id)
                .expect("queued"),
            DeliveryOutcome::Queued
        );
        let herdr: i64 = store
            .connection
            .query_row(
                "SELECT COUNT(*) FROM deliveries
                 WHERE message_id = ?1 AND delivery_transport = 'herdr_prompt'",
                [progress.message_id.to_string()],
                |row| row.get(0),
            )
            .expect("herdr");
        assert_eq!(herdr, 0);
        store
            .ack_socket_inbox_delivery(waiter.logical_agent_id, progress.message_id)
            .expect("ack progress");
        assert_eq!(
            store.obligation_state(ask.message_id).expect("still open"),
            ObligationState::InProgress
        );

        let final_reply = store
            .create_reply(
                ask.message_id,
                owing.logical_agent_id,
                "done",
                ReplyDisposition::Final,
                "ack-final",
            )
            .expect("final");
        assert_eq!(
            store.obligation_state(ask.message_id).expect("persist"),
            ObligationState::InProgress
        );
        assert_eq!(
            store
                .delivery_outcome_for_message(final_reply.message_id)
                .expect("queued final"),
            DeliveryOutcome::Queued
        );
        store
            .ack_socket_inbox_delivery(waiter.logical_agent_id, final_reply.message_id)
            .expect("ack final");
        assert_eq!(
            store.obligation_state(ask.message_id).expect("resolved"),
            ObligationState::Resolved
        );
        assert_eq!(
            store
                .ack_socket_inbox_delivery(waiter.logical_agent_id, final_reply.message_id)
                .expect("idempotent"),
            DeliveryOutcome::Accepted
        );
        let second = store.create_reply(
            ask.message_id,
            owing.logical_agent_id,
            "again",
            ReplyDisposition::Final,
            "ack-second",
        );
        assert!(matches!(second, Err(StoreError::Conflict(_))));
    }

    #[test]
    fn socket_inbox_cancel_queues_cancellation_not_resolved() {
        let mut store = Store::in_memory().expect("store");
        let waiter = store
            .register_socket_waiter("inbox", Parent::Parentless, "cancel-waiter")
            .expect("register");
        let owing = store
            .declare_start(&intent("owing", "term-b", "cancel-owing"))
            .expect("owing");
        mark_ready(&mut store, owing, "owing", "term-b");
        let ask = store
            .create_ask_with_schedule(
                waiter.logical_agent_id,
                owing.logical_agent_id,
                owing.incarnation_id,
                "question",
                "cancel-ask",
                None,
                None,
                true,
            )
            .expect("ask");
        let created = store
            .cancel_with_response(
                waiter.logical_agent_id,
                ask.message_id,
                "obsolete",
                "Your ask was cancelled. Reason: obsolete.",
                "Stop. Ask was cancelled. Reason: obsolete.",
                None,
                None,
            )
            .expect("cancel");
        assert!(created.delivery.is_none());
        assert!(
            created.owing_delivery.is_some(),
            "Ready owing pane still gets a stop-notice"
        );
        assert_eq!(
            store.obligation_state(ask.message_id).expect("state"),
            ObligationState::Cancelled
        );
        let (kind, sender): (String, Option<String>) = store
            .connection
            .query_row(
                "SELECT kind, sender_agent_id FROM messages WHERE id = ?1",
                [created.message_id.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("message");
        assert_eq!(kind, "cancellation");
        assert_eq!(sender, None);
        let queued = store
            .queued_socket_inbox_deliveries(waiter.logical_agent_id)
            .expect("inbox");
        assert_eq!(queued.len(), 1);
        assert_eq!(queued[0].message_id, created.message_id);
        assert_eq!(queued[0].kind, MessageKind::Cancellation);
        assert_eq!(queued[0].body, "Your ask was cancelled. Reason: obsolete.");
    }

    fn waiter_ask(
        store: &mut Store,
        waiter: LogicalAgentId,
        owing: DeclaredStart,
        body: &str,
        key: &str,
    ) -> CreatedAsk {
        store
            .create_ask_with_schedule(
                waiter,
                owing.logical_agent_id,
                owing.incarnation_id,
                body,
                key,
                None,
                None,
                true,
            )
            .expect("ask")
    }

    #[test]
    fn waiter_retire_cancels_open_asks_and_queued_finals() {
        let mut store = Store::in_memory().expect("store");
        let waiter = store
            .register_socket_waiter("inbox", Parent::Parentless, "retire-waiter")
            .expect("register");
        let owing = store
            .declare_start(&intent("owing", "term-b", "retire-owing"))
            .expect("owing");
        mark_ready(&mut store, owing, "owing", "term-b");
        let open_ask = waiter_ask(&mut store, waiter.logical_agent_id, owing, "open", "open");
        let queued_ask = waiter_ask(
            &mut store,
            waiter.logical_agent_id,
            owing,
            "queued",
            "queued",
        );
        let final_reply = store
            .create_reply(
                queued_ask.message_id,
                owing.logical_agent_id,
                "done",
                ReplyDisposition::Final,
                "retire-final",
            )
            .expect("queued final");
        assert_eq!(
            store
                .delivery_outcome_for_message(final_reply.message_id)
                .expect("queued"),
            DeliveryOutcome::Queued
        );
        let ended = store
            .end_socket_waiter(waiter.logical_agent_id)
            .expect("retire");
        assert_eq!(ended.cancelled_ask_ids.len(), 2);
        assert_eq!(
            store.obligation_state(open_ask.message_id).expect("open"),
            ObligationState::Cancelled
        );
        assert_eq!(
            store
                .obligation_state(queued_ask.message_id)
                .expect("queued"),
            ObligationState::Cancelled
        );
        let reason: String = store
            .connection
            .query_row(
                "SELECT cancellation_reason FROM obligations WHERE ask_message_id = ?1",
                [open_ask.message_id.to_string()],
                |row| row.get(0),
            )
            .expect("reason");
        assert_eq!(reason, "waiter retired");
        assert!(
            store
                .pending_obligations(owing.logical_agent_id)
                .expect("pending")
                .is_empty()
        );
        assert_eq!(
            store
                .delivery_outcome_for_message(final_reply.message_id)
                .expect("undeliverable"),
            DeliveryOutcome::TargetUnavailable
        );
        let remaining_queued: i64 = store
            .connection
            .query_row(
                "SELECT COUNT(*) FROM deliveries
                 WHERE recipient_agent_id = ?1 AND outcome = 'queued'",
                [waiter.logical_agent_id.to_string()],
                |row| row.get(0),
            )
            .expect("queued count");
        assert_eq!(remaining_queued, 0);
        let later = store
            .reply_receive_path(open_ask.message_id, owing.logical_agent_id)
            .expect_err("closed")
            .to_string();
        assert!(
            later.contains("does not name an open obligation"),
            "later final must fail as not open, not as an ended waiter: {later}"
        );
        store
            .declare_start(&intent("inbox", "term-c", "name-free-after-retire"))
            .expect("name released");
    }

    #[test]
    fn waiter_retire_without_open_asks_only_ends_targeting() {
        let mut store = Store::in_memory().expect("store");
        let waiter = store
            .register_socket_waiter("inbox", Parent::Parentless, "empty-retire")
            .expect("register");
        let ended = store
            .end_socket_waiter(waiter.logical_agent_id)
            .expect("retire");
        assert!(ended.cancelled_ask_ids.is_empty());
        assert!(ended.owing_notices.is_empty());
        let again = store.end_socket_waiter(waiter.logical_agent_id);
        assert!(
            again
                .expect_err("already ended")
                .to_string()
                .contains("not an active socket waiter")
        );
    }
}
