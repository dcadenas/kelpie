//! Strong domain types for durable coordination state.

use std::fmt;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

macro_rules! id_type {
    ($name:ident) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(Uuid);

        impl $name {
            /// Create a time-ordered opaque identifier.
            #[must_use]
            pub fn new() -> Self {
                Self(Uuid::now_v7())
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(f)
            }
        }
    };
}

id_type!(LogicalAgentId);
id_type!(IncarnationId);
id_type!(OperationId);
id_type!(MessageId);
id_type!(OperatorNoticeId);
id_type!(RenewId);

impl IncarnationId {
    pub(crate) fn parse(value: &str) -> Option<Self> {
        Uuid::parse_str(value).ok().map(Self)
    }
}

impl OperationId {
    pub(crate) fn parse(value: &str) -> Option<Self> {
        Uuid::parse_str(value).ok().map(Self)
    }
}

impl MessageId {
    pub(crate) fn parse(value: &str) -> Option<Self> {
        Uuid::parse_str(value).ok().map(Self)
    }
}

impl LogicalAgentId {
    pub(crate) fn parse(value: &str) -> Option<Self> {
        Uuid::parse_str(value).ok().map(Self)
    }
}

impl OperatorNoticeId {
    pub(crate) fn parse(value: &str) -> Option<Self> {
        Uuid::parse_str(value).ok().map(Self)
    }
}

impl RenewId {
    pub(crate) fn parse(value: &str) -> Option<Self> {
        Uuid::parse_str(value).ok().map(Self)
    }
}

/// A logical agent's durable parent relationship.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "agent_id")]
pub enum Parent {
    Parentless,
    Agent(LogicalAgentId),
}

/// Lifecycle state for one exact runtime binding attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IncarnationState {
    Declared,
    Starting,
    Ready,
    Failed,
    Unknown,
    Retiring,
    Retired,
    Lost,
    Superseded,
}

/// Durable outcome for an external operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationOutcome {
    Pending,
    Accepted,
    Succeeded,
    Failed,
    Superseded,
    Unknown,
}

/// Semantic kind of a stored message.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageKind {
    Tell,
    Ask,
    Reply,
    Cancellation,
}

/// Explicit semantic kind for a launch's initial message.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InitialMessageKind {
    Tell,
    Ask,
}

/// Initial message stored in launch intent before runtime effects begin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InitialMessageIntent {
    /// `None` denotes same-user operator attribution, not authentication.
    pub sender: Option<LogicalAgentId>,
    pub kind: InitialMessageKind,
    pub body: String,
}

/// Disposition of a reply message.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplyDisposition {
    Progress,
    Final,
}

/// State of the final-reply obligation created by an ask.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObligationState {
    Open,
    InProgress,
    Resolved,
    Cancelled,
    Orphaned,
}

/// Outcome of one terminal-input delivery attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryOutcome {
    Pending,
    Submitted,
    Accepted,
    Queued,
    Unknown,
    Rejected,
    TargetUnavailable,
    Superseded,
}

/// Phase of one renew of an incarnation's backend-native context.
///
/// `Clearing` is the only phase in which the incarnation has lost its context
/// without yet having been re-seeded. A renew interrupted there is completed by
/// recovery, never restarted, because the clear cannot be undone or repeated.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RenewPhase {
    Scheduled,
    Preparing,
    Ready,
    Clearing,
    Injected,
    Done,
    TimedOut,
    Aborted,
    Terminated,
}

/// What to do when the prepare deadline elapses with no final reply.
///
/// There is no default. Aborting protects work the agent never saved;
/// proceeding bounds the context and destroys it. Which is correct depends on
/// what the agent is doing, so the caller states it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RenewTimeout {
    Abort,
    Proceed,
}

/// Which of a renew's two external effects an attempt record describes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RenewStep {
    Clear,
    Inject,
}

/// Complete durable intent for one renew.
///
/// Both prompts are persisted before the first Herdr write. That ordering is
/// what makes an interrupted renew recoverable: the resume prompt is already
/// durable when the clear is submitted, so no crash can leave an incarnation
/// cleared with nothing to re-seed it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RenewIntent {
    pub logical_agent_id: LogicalAgentId,
    pub incarnation_id: IncarnationId,
    pub requester_agent_id: LogicalAgentId,
    pub prepare_prompt: String,
    pub resume_prompt: String,
    pub on_timeout: RenewTimeout,
    pub prepare_timeout_ms: i64,
    /// `None` is one-shot. `Some` re-arms after each injection and ends only
    /// when the incarnation stops being Ready.
    pub every_ms: Option<i64>,
    pub scheduled_at_ms: i64,
}

/// Complete durable intent for starting one incarnation.
///
/// When [`Self::logical_agent_id`] is `None`, Kelpie creates a new logical
/// agent. When set, Kelpie continues that exact logical agent in a new
/// incarnation and preserves its obligations and history. The public name is a
/// reusable live alias, never a primary key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StartIntent {
    pub public_name: String,
    /// Exact logical agent to continue, or `None` for create-new intent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub logical_agent_id: Option<LogicalAgentId>,
    pub parent: Parent,
    pub herdr_session: String,
    pub pane_id: String,
    pub expected_terminal_id: String,
    pub backend_kind: String,
    pub backend_args: Vec<String>,
    pub initial_message: InitialMessageIntent,
    pub working_directory: String,
    pub idempotency_key: String,
    pub readiness_timeout_ms: u64,
    pub keep_open: bool,
    /// Requested model. Never treated as observed execution metadata.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requested_model: Option<String>,
    /// Requested provider. Never treated as observed execution metadata.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requested_provider: Option<String>,
    /// Requested reasoning effort. Never treated as observed execution metadata.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requested_effort: Option<String>,
    /// Incarnation this start replaces, demoted in the same transaction that
    /// proves the successor Ready.
    ///
    /// A handoff moves one logical agent to a new runtime. Doing the demotion
    /// separately would leave a moment with two Ready incarnations of one agent,
    /// which makes both alias resolution and reply correlation ambiguous, or a
    /// moment with none, which makes the agent unaddressable. Both are visible
    /// to every child at once, so the two writes are one write.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub supersedes: Option<IncarnationId>,
}

/// Explicit adoption of an already-running Herdr agent without `agent.start`.
///
/// Fail-closed exact selector is `pane_id` + `expected_terminal_id`. Optional
/// fields further constrain the snapshot match. Create-new vs continue mirrors
/// [`StartIntent`]: absent [`Self::logical_agent_id`] allocates a new logical
/// agent; a set id continues that exact identity and preserves its history.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdoptIntent {
    pub pane_id: String,
    pub expected_terminal_id: String,
    /// When set, must equal the live Herdr name or the derived claim name.
    /// When absent, adopt uses the observed Herdr name or a cwd-derived claim.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub public_name: Option<String>,
    /// Exact logical agent to continue, or `None` for create-new intent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub logical_agent_id: Option<LogicalAgentId>,
    /// Used only for create-new logical agents.
    pub parent: Parent,
    pub herdr_session: String,
    /// When set, must equal the live backend kind (`agent` field).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backend_kind: Option<String>,
    /// Backend arguments the caller believes this runtime was launched with.
    ///
    /// Adoption observes a runtime Kelpie did not start, so this is a claim
    /// about intent, never evidence of what the process is running. It is
    /// recorded as requested configuration and is never reported as observed.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub backend_args: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requested_model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requested_provider: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requested_effort: Option<String>,
    pub idempotency_key: String,
}

/// Stored message data used by the initial slice.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message {
    pub id: MessageId,
    pub sender: LogicalAgentId,
    pub recipient: LogicalAgentId,
    pub kind: MessageKind,
    pub body: String,
    pub reply_to: Option<MessageId>,
    pub disposition: Option<ReplyDisposition>,
    pub creates_obligation: bool,
}

/// Human-readable duration, coarsest two units, never negative.
///
/// Shared because an operator notice and the report render the same intervals;
/// a raw millisecond count in one of them is a worse answer than in the other.
pub(crate) fn format_duration_ms(ms: i64) -> String {
    if ms < 0 {
        return "0s".into();
    }
    let (seconds, minutes, hours, days) = (ms / 1000, ms / 60_000, ms / 3_600_000, ms / 86_400_000);
    if days > 0 {
        format!("{days}d{}h", hours % 24)
    } else if hours > 0 {
        format!("{hours}h{}m", minutes % 60)
    } else if minutes > 0 {
        format!("{minutes}m{}s", seconds % 60)
    } else {
        format!("{seconds}s")
    }
}
