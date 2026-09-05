# Kelpie Specification

Status: Draft normative contract

Kelpie is a durable inter-agent coordination layer for agent runtimes managed by
Herdr. Herdr owns terminals, processes, topology, and observed agent state.
Kelpie owns logical identity, message semantics, reply obligations, lifecycle
intent, and recovery.

This document defines the product boundary and the invariants an implementation
MUST preserve.

## Normative language

The terms **MUST**, **MUST NOT**, **SHOULD**, **SHOULD NOT**, and **MAY** are used
as described by RFC 2119 and RFC 8174.

Statements marked `Open question` are not implementation authority. Work that
depends on one of them MUST stop at the boundary or make the decision explicit
in a later revision of this specification.

## Problem

Herdr is a runtime for terminals and coding agents. Its prompt operation is
terminal input, its waits observe agent lifecycle, and its event stream is
volatile. It does not provide attributed messages, durable logical identity,
parentage, reply correlation, pending obligations, durable operation receipts,
or recovery of coordination state.

Agent coordination applications need those semantics, but application policy
MUST NOT be embedded in Herdr. Kelpie supplies the missing general-purpose
coordination layer while using Herdr as the singular authority for live runtime
facts.

## Goals

Kelpie MUST provide:

1. Stable logical agent identities distinct from Herdr panes, names, processes,
   and backend-native conversations.
2. Exact, immutable agent incarnations for correlating asynchronous effects.
3. Explicit parentage or explicit parentlessness.
4. Structured `tell`, `ask`, progress, and final-reply messaging.
5. Durable message, delivery, operation, and reply-obligation records.
6. Honest delivery and lifecycle outcomes, including ambiguity.
7. Crash recovery through persisted intent and reconciliation with Herdr.
8. Direct integration with Herdr's socket protocol without invoking its CLI.
9. A transport-neutral domain model with two local delivery transports: Herdr
   prompt delivery, and a local socket inbox.
10. Optional operator notification backed by a durable local inbox.
11. A small API and command surface usable by agents and other automation.
12. Fault-injection tests that prove the invariants across process failure,
    Herdr restart, disconnect, delayed events, and runtime identity reuse.

## Non-goals

Kelpie MUST NOT:

- replace Herdr's terminal, process, pane, workspace, worktree, rendering, or
  agent-detection responsibilities;
- infer task completion from `idle`, `done`, `blocked`, terminal output, or a
  successful prompt submission;
- encode application policy such as frozen revisions, judge counts, review
  verdicts, finding reconciliation, repository-hosting state, or model rules;
- become a durable workflow-phase registry or automatically choose the next
  workflow step;
- require tmux;
- use Herdr's private rendering/client protocol or deserialize Herdr's internal
  persistence files;
- treat Herdr's event stream as a durable event ledger;
- promise exactly-once terminal delivery;
- add remote networking or peer discovery to the supported local architecture;
- provide a dashboard, LLM router, workflow-driving cron system, automatic
  loop-stall recovery, or backend-launch monolith. A repeating schedule only
  delivers an opaque tell to one existing logical agent. It MUST NOT interpret
  the body, choose a next action, or start, revive, or restart an agent;
- place coordination records in a repository being worked on by an agent.

## Authority boundaries

Authority MUST be singular for each concern:

| Concern | Authority |
| --- | --- |
| Live terminals, processes, topology, cwd, and observed agent state | Herdr |
| Logical agent identity, desired public name, and parentage | Kelpie |
| Agent incarnation and coordination operation identity | Kelpie |
| Backend-native conversation reference as currently observed | Herdr |
| Association of that reference with a logical incarnation | Kelpie |
| Message bodies, correlation, deliveries, and reply obligations | Kelpie |
| Assignment outcome and application-specific workflow state | Calling application |
| Source code, branches, commits, and worktrees | Git and the target repository |
| Human-visible ephemeral notification | Herdr when available |
| Durable operator inbox | Kelpie |

Kelpie MUST query or subscribe to Herdr for present runtime facts. It MUST NOT
maintain an independent opinion about whether a terminal or process is alive.
The Herdr agent name is a projection of Kelpie's desired public name. A missing
name MUST NOT by itself invalidate a Ready binding; Kelpie MUST repair that
projection only when the recorded pane and terminal still identify the live
seat, the current incarnation's backend still matches, and no different live
name claims it. A backend change ends the current incarnation but MUST NOT
prevent automatic continuation of its logical agent on that recorded seat.
Continuation MUST fail closed when more than one logical agent matches the seat.

## System boundary

The architecture consists of:

1. A durable Kelpie service or process.
2. A Kelpie client/helper used by coordinators and agents.
3. A typed adapter speaking Herdr's documented newline-delimited JSON socket
   protocol directly.
4. An optional Herdr plugin package for installation, actions, configuration,
   and operator UI. The plugin MUST NOT be the only durable execution mechanism
   unless Herdr gains a supervised service interface.

Kelpie MUST negotiate Herdr protocol compatibility through `ping` and MUST fail
with an explicit incompatibility result when the running protocol is outside
its supported range.

Kelpie MUST bootstrap live state from `session.snapshot`. Subscriptions MAY
reduce latency, but after reconnect, Herdr restart, suspected event loss, or
live handoff, Kelpie MUST discard cached present-state assumptions and obtain a
new authoritative snapshot.

The local daemon MUST NOT delay one client on another client's Herdr request,
readiness poll, rotation poll, retry delay, or incomplete request. The daemon
MUST represent each such wait as parked state and advance it from an event or a
store-clock deadline.

Herdr mutations for one pane MUST preserve the order in which Kelpie committed
their intents. Mutations for different panes MAY run concurrently.

A client disconnect before its response MUST NOT change the durable operation
outcome. A `start` response MUST still require observed readiness; acceptance of
`agent.start` alone MUST NOT prove readiness.

## Local delivery transports

Kelpie's domain is transport-neutral. Locally it MUST support exactly two
delivery clients of one seam:

1. A Herdr pane, addressed by a Ready incarnation, delivered by Herdr prompt.
2. A long-lived local socket client, addressed by a pane-less LogicalAgent,
   delivered by draining that waiter's inbox.

The host process of a socket waiter is a delivery client. It is not a Herdr
pane.

The verbs are `tell`, `ask`, and `reply`. An occupant answers an ask with
`reply` and a final disposition. Kelpie MUST NOT add a new message verb or a
new obligation kind for socket waiters. An alias-addressed `tell` MUST resolve
first to one active LogicalAgent and then dispatch by that agent's fixed
`delivery_transport`: a `herdr_prompt` target requires its exact Ready
incarnation, while a `socket_inbox` target queues to its logical-agent ID with
no incarnation. Alias resolution MUST fail closed if more than one active
target holds the name.

Occupant envelopes are `<kelpie from=… msg=… reply-to=…>`: `from` is the
waiter's public name, `msg` and `reply-to` are the ask message ID. Pane callers
MUST default sender attribution to the Ready binding of the calling pane. A
pane-less host MAY attribute a send as the operator; that attribution MUST NOT
make the operator the waiting agent.

The operator-notice inbox in Goal 10 is a human-facing durable record. It is
not the socket-waiter receive path.

A LogicalAgent created as a socket-inbox recipient MUST record
`delivery_transport` as `socket_inbox`. A LogicalAgent created as a Herdr pane
recipient MUST record `delivery_transport` as `herdr_prompt`. Those are the
only two values. `delivery_transport` is fixed at creation. It MUST NOT change
when the agent later gains or loses a Herdr binding. Start-continue and adopt
MUST refuse a logical agent whose `delivery_transport` is `socket_inbox`.

## Domain model

The names below describe semantic records, not a required database schema.

### LogicalAgent

A `LogicalAgent` is a durable Kelpie identity.

It MUST contain:

- an immutable opaque ID;
- a human-readable address or name;
- either a parent logical-agent ID or an explicit parentless marker;
- `delivery_transport`: `herdr_prompt` or `socket_inbox`;
- creation time;
- application-owned optional metadata that does not become Kelpie policy.

A public name MUST NOT be the primary key. Reuse of a name MUST NOT cause old
messages, operations, or obligations to refer to the new owner.

A LogicalAgent MAY exist with no Herdr pane. That pane-less agent is a legal
delivery target. Creating it MUST NOT mint a fake pane occupant or a fake
incarnation. The operator identity MUST NOT be a LogicalAgent id and MUST NOT
be an obligation's waiting agent. A TCP or Unix connection MUST NOT be a
LogicalAgent id.

Public names are one namespace across both transports. A socket waiter's public
name MUST NOT equal a Ready Herdr alias or another socket waiter's public name.
Start, adopt, and rename MUST refuse a name a socket waiter holds. Alias
resolution MUST fail closed when more than one agent could match. Snapshot
absence MUST NOT release a socket waiter's name. That name is released only by
`waiter.retire`, which ends that LogicalAgent as a delivery target. `delivery_transport`
MUST remain `socket_inbox` after that end. Start-continue and adopt MUST still
refuse the agent. `waiter.retire` with no `open` or `in_progress` asks waiting
on that agent MUST only end targeting. With such asks, it MUST cancel them in
the same transaction, with reason `waiter retired`, and MUST notify each owing
agent when that agent is addressable, as `cancel` does — including skipping the
owing stop-notice when the ask's own delivery is still an unsubmitted `queued`
row. It MUST NOT deliver those cancellation notices to the retiring waiter's
inbox. Queued socket-inbox
deliveries for that waiter MUST NOT remain `queued`. After that, occupant
`pending` MUST NOT list those asks, and a later final MUST be refused because
the obligation is not open, not because the waiter is no longer a delivery
target.

`waiter.register` MUST create that pane-less LogicalAgent. The request MUST
contain a public name, explicit parent or parentless marker, and an idempotency
key. It MUST NOT contain a pane, terminal, backend, or incarnation. Replay of
the same idempotency key MUST return the same logical-agent id. The result is
the logical-agent id. Creating it MUST NOT insert an incarnation.

On `ask`, `from_operator` is message-sender attribution only. The obligation's
`waiting_agent_id` MUST be the waiter LogicalAgent named as `sender`. Occupant
envelopes MUST use `from=` equal to that waiter's public name, never `operator`
and never a relay pubkey. `from_operator` MUST be refused unless that waiting
agent's `delivery_transport` is `socket_inbox`.

### Incarnation

An `Incarnation` represents one attempt to bind a logical agent to a live Herdr
runtime and backend conversation.

It MUST contain:

- an immutable opaque incarnation ID;
- its logical-agent ID;
- intended Herdr session and launch configuration;
- observed Herdr terminal and pane identity when available;
- observed backend-native conversation reference when available;
- creation time;
- terminal time and terminal reason when known;
- a lifecycle state from the state machine below.

Every delayed response or observation that mutates lifecycle state MUST name the
exact incarnation. A result for an older incarnation MUST NOT mutate a newer
incarnation even when the public name, pane, or backend reference was reused.

### Operation

An `Operation` records intended interaction with Herdr, such as start, prompt,
resume, retire, or notification.

It MUST contain:

- a caller-supplied or Kelpie-generated idempotency key;
- operation kind and target incarnation;
- the complete parsed intent;
- creation time;
- zero or more attempt records;
- a current outcome.

The outcome MUST be one of:

- `pending`: intent is durable but no attempt has been accepted;
- `accepted`: Herdr accepted an attempt but completion is not known;
- `succeeded`: the operation's defined terminal condition was observed;
- `failed`: a terminal failure was observed;
- `superseded`: a newer exact-owner operation made this result inapplicable;
- `unknown`: available evidence cannot distinguish success from failure.

`unknown` MUST NOT be coerced to either success or failure. Retrying an unknown
operation MUST follow operation-specific idempotency rules.

A repeated caller idempotency key for a prompt operation MUST be resolved from
the prior operation's outcome, not from an undifferentiated uniqueness
violation. `succeeded` MUST return the recorded result without creating another
effect only when the message kind, sender, and reply correlation match the
recorded request; a mismatch MUST refuse replay. `failed` MAY create a fresh
operation under the same caller key because
terminal failure proves no effect landed; the failed operation and its evidence
MUST remain intact. `pending`, `accepted`, `superseded`, and `unknown` MUST
refuse a fresh operation. Every refusal MUST name the prior outcome so the caller
can reconcile it.

Non-prompt operations retain their key for every outcome. Operation-specific
replay MAY return a recorded result, but reuse MUST NOT mint a second logical
identity or repeat another non-prompt effect. When reuse is refused, the error
MUST name the prior operation and outcome.

### Message

A `Message` MUST contain:

- an immutable message ID;
- sender logical-agent ID or the operator identity;
- recipient logical-agent ID or the operator identity;
- kind: `tell`, `ask`, or `reply`;
- body or a typed payload reference;
- creation time;
- optional `reply_to` message ID;
- for replies, disposition: `progress` or `final`;
- whether the message itself creates a new reply obligation.

The stored structured message is authoritative. Any XML, Markdown, JSON, or
plain-text rendering injected into an agent is a transport representation.

### Delivery

A `Delivery` represents one attempt to convey a message to one recipient.

It MUST contain:

- message ID;
- `delivery_transport`: `herdr_prompt` or `socket_inbox`;
- for `herdr_prompt`, the exact recipient incarnation ID;
- for `socket_inbox`, the recipient logical-agent ID, and MUST NOT require an
  incarnation ID;
- attempt number;
- scheduled, attempted, and resolved times where applicable;
- Herdr request correlation when the transport is `herdr_prompt` and the
  correlation is available;
- an outcome.

Herdr request correlation MUST NOT be required for `socket_inbox`.

Delivery outcome MUST distinguish at least:

- `pending`;
- `submitted`;
- `accepted`;
- `queued`;
- `unknown`;
- `rejected`;
- `target_unavailable`;
- `superseded`.

The exact mapping from transport observations to these outcomes MUST be
documented and tested. For `socket_inbox`, including an unsolicited `tell`
resolved from the waiter's public alias, a client acknowledgement is
`accepted`, a disconnected host leaves the delivery `queued`, and an absent
waiter identity is `target_unavailable`. A write that may have reached the
client in part stays `queued`: persist precedes every inbox byte, a torn line
has no newline so the client discards it, and reconnect drains that same row.
That drain is not a resend. This transport MUST NOT record `unknown` for an
inbox write. Persist of the delivery record is not acceptance. Success of any
publish outside Kelpie is not acceptance.

`submitted`, `accepted`, `queued`, and `unknown` MUST NOT be blindly
resent because the recipient may already have received the message. Draining a
still-queued `socket_inbox` delivery on reconnect is that same attempt
completing. It is not a resend.

### Obligation

An `Obligation` records that one logical agent owes a final reply to a specific
`ask`.

It MUST contain:

- the source ask message ID;
- the owing and waiting logical-agent IDs;
- creation and last-activity times;
- state: `open`, `in_progress`, `resolved`, `cancelled`, or `orphaned`;
- the resolving final-reply message ID when resolved;
- the cancellation requester and reason when cancelled.

A progress reply MUST refresh activity and set `in_progress`; it MUST NOT resolve
the obligation. A final reply MUST resolve only the exact obligation named by
`reply_to`. A reply MUST be accepted only from the obligation's owing agent; a
reply from the asker or any third party MUST be refused without mutating the
obligation or delivering anything. A reply from the wrong sender or to the wrong
message MUST NOT clear another obligation.

Progress and final replies are durable messages with their own delivery attempts
to the waiting logical agent. Kelpie MUST resolve the owing and waiting agents
from the obligation named by `reply_to`. When send intent is recorded, Kelpie
MUST bind the waiter's receive path:

- `herdr_prompt`: the unique Ready incarnation of the waiting agent, then Herdr
  prompt delivery;
- `socket_inbox`: the waiting agent's socket inbox, with no Herdr prompt.

Outcomes are the same accepted / rejected / target-unavailable / unknown set as
`tell` and `ask`. Submitted and unknown reply deliveries MUST NOT be blindly
resent.

A final reply MUST resolve the obligation only when its delivery is accepted.
For `socket_inbox`, accepted means the socket client acknowledged that
delivery. Rejected, target-unavailable, or unknown final deliveries leave the
obligation open or in progress so the waiter is not treated as answered without
an accepted delivery. Progress MAY set `in_progress` when the progress message
is durably recorded, independent of that progress delivery's terminal outcome.

### OperatorNotice

An `OperatorNotice` is a durable local record addressed to the human operator.
Kelpie SHOULD also request a best-effort Herdr notification. Failure to display
the ephemeral notification MUST NOT remove or invalidate the durable record.

### ScheduledDelivery

A future delivery MAY have a due time and cancellation state. A repeating
schedule MAY materialize an ordinary tell on an interval. Scheduling MUST be
limited to delayed message delivery, repeating opaque tells, reminders, and
renew policies. It MUST NOT encode workflow phases, application verdicts, or
automatic next actions.

A delivery due time is one-shot. A repeating tell schedule is bound to a logical
agent, advances on the host wall clock, survives incarnation replacement, and is
cancellable by its requester or target. Each firing MUST resolve the target's
current unique receive path. When none exists, the firing MUST record and report
`target_unavailable`, MUST create no message, delivery, operation, pane,
worktree, or runtime, and MUST continue to its next interval. Missed intervals
while kelpied is down are coalesced into one due firing; the next interval starts
when that firing is recorded rather than producing a restart burst.
If an earlier firing still has a `pending`, `queued`, or `submitted`
delivery, a later due firing MUST be recorded as skipped and MUST NOT materialize
a second message beside it. An `unknown` delivery MUST NOT be resent, but it MUST
NOT stop later intervals from materializing distinct messages.
Kelpie MUST raise an operator notice when a schedule enters an unavailable run,
but MUST NOT repeat that notice on every interval until a firing succeeds.
Every ask creates a reply-reminder policy by default.
The caller MAY explicitly disable reminders for one ask. The policy is armed
only after the ask delivery is accepted. Reminder injection is `herdr_prompt`
only. It MUST be injected only when a fresh Herdr snapshot proves the owing
logical agent's exact Ready incarnation is `idle` or `done`. A `socket_inbox`
owing agent has no Herdr pane; Kelpie MUST NOT require reminder injection for
that owing agent. A first observed working-to-idle/done boundary MAY trigger the
initial reminder before the interval when no reply activity has occurred.
Progress and final-reply activity reset its interval. A recorded final reply
whose delivery is `queued`, `submitted`, `accepted`, or `unknown` MUST NOT
receive reminder injection until that delivery terminals. Persist is not
resolve: waiter ACK or Herdr accept remains the only resolve. Rejected and
`target_unavailable` finals leave the obligation open; reminders MAY resume.
An unknown reminder delivery MUST suspend automatic retries. Snoozing or disabling
a reminder MUST NOT resolve its obligation. Reminders are not cron and MUST NOT
require receiver acknowledgement. Every reminder MUST carry the original ask
body: the obligation is durable while the owing agent's context may have been
replaced by a renew, a restart, or a clear, so the reminder is the amnesia
protocol — the agent must be able to answer what it was asked without asking
the sender to repeat itself. Cancel of a scheduled delivery is legal only
before the first Herdr write. After submit, existing no-resend and unknown
rules apply. The due clock is Unix epoch milliseconds from the host
`SystemTime`. A delivery is due when `now_ms >= scheduled_at_ms`. A due time
that elapses while kelpied is not running is `unknown`; restart MUST NOT fire
that delivery without a new attempt record. Due work MUST run with no client
connected.

### Clear

A `Clear` replaces one Ready incarnation's backend-native conversation without
replacing its runtime, pane, logical identity, parentage, obligations, or
message history. It MUST carry durable intent before the Herdr write. It MUST
NOT send a prepare ask or inject a resume prompt.

Clear MUST use a command defined for the incarnation's backend kind. An
undefined backend kind MUST be rejected as incompatible before any durable
intent. Kelpie MUST NOT infer a clear command.

For a backend whose clear rotates its session reference, clear MUST wait for a
reference differing from the pre-clear observation before succeeding. It MUST
NOT infer completion from elapsed time, `idle`, or prompt acceptance. For a
backend that allocates its replacement conversation on the next prompt, clear
MUST return after the clear command is accepted. Waiting for rotation in that
case cannot terminate because the caller's next prompt creates the replacement.

An absent session reference MUST NOT prove rotation. The daemon MUST keep
serving unrelated clients while a clear waits for prompt spacing or session
rotation. A clear MUST NOT follow another submitted prompt, including a
reminder or one with an unknown outcome, into the same pane without the backend
settle gap, and that gap MUST NOT be treated as evidence. Prompt deliveries and
reminders MUST remain queued while an on-clear operation is awaiting rotation;
this includes progress and final replies, whose obligations MUST retain their
normal accepted-delivery semantics. The first prompt after a submitted
standalone clear MUST also wait out the backend settle gap when that clear
succeeds or has an unknown outcome, including when the prompt is what allocates
the replacement conversation. This gate applies when a prompt fires, not only
when it is created, so previously scheduled deliveries, reminders, and renew
prepare prompts MUST wait too. A queued delivery withheld by this gate MUST
carry a durable due time at or after the gate, so recovery during the gap does
not classify the delivery as a missed wake. The clear intent MUST record the
settle duration so recovery of a submitted clear applies the same deadline even
if the daemon's current configuration differs.
Standalone clear and an in-flight renew cycle MUST be mutually exclusive for
one incarnation; a future scheduled renew policy MAY coexist with clear and
MUST NOT advance until clear finishes.

An ambiguous clear submission MUST NOT be resent automatically, and MUST NOT be
resent on request either: while an incarnation's latest clear is `unknown` and
no backend-native session rotation has been observed since it resolved, a
further clear MUST be refused with a stable error code. A retry destroys a real
context to re-ask a question the observation channel has already failed to
answer. The refusal MUST lift on evidence rather than on elapsed time: an
observed rotation after that clear resolves the ambiguity and makes clearing
available again, with no operator step.

Renew MUST use
the same verified command submission and session-rotation proof as standalone
clear. Recovery MUST fail a standalone clear intent that has no submitted
attempt, because no Herdr write occurred; it MUST NOT leave that intent blocking
the incarnation. Recovery that changes a submitted clear to `unknown` MUST
create a durable operator notice because the original caller no longer exists.

### Renew

A `Renew` bounds one incarnation's backend-native context by clearing it and
re-seeding it from a prompt. It MUST NOT replace the runtime, the pane, the
logical agent, its parentage, its obligations, or its message history.

A renew MUST carry a prepare prompt, a resume prompt, and an explicit timeout
disposition. Both prompts MUST be durable before the first Herdr write, so an
interrupted renew can be completed from stored state alone.

A renew MUST identify its target by exact logical agent and incarnation. It MUST
NOT resolve a public name. A public name is a reusable live alias that MAY be
held by a different agent than the caller meant, and the cost of that mistake is
not one misdelivered message: a policy clears its target's context once per
cycle, and only the target or the requester can undo it. An interface that
offers no alias for a renew MUST NOT be worked around by resolving one first
purely to satisfy the exact form.

A renew request that names no target MUST arm on the calling incarnation. Kelpie
MUST refuse a second non-terminal policy on one incarnation, and that refusal is
scoped per incarnation — so it protects a caller arming on itself and does not
protect one arming on anybody else.

Clearing MUST use a clear command defined for the incarnation's backend kind. An
undefined backend kind MUST be rejected as incompatible before any durable
intent. Kelpie MUST NOT infer a clear command.

The prepare obligation gates a destructive local operation, so it MUST NOT
depend on any agent other than the one being cleared. The prepare ask MUST be
owed by the incarnation being renewed. It MUST NOT be owed by the policy's
requester, whose liveness is unrelated to whether that context should be
bounded; a requester-owed obligation violates this property whether or not the
requester happens to be alive. The requester remains recorded on the policy for
attribution and for the cancel permission, and remains the prepare envelope's
sender.

A renew MUST disclose itself. The prepare prompt MUST be rendered with the
resume prompt quoted verbatim, and with what survives and what does not. The
resume prompt MUST be rendered as a continuation and MUST carry its cycle
number. A renew MUST NOT clear an incarnation that was not told it would be
cleared.

Clear completion MUST be proven by observing that the backend-native session
reference differs from the reference observed before the clear. It MUST NOT be
inferred from elapsed time, from `idle`, or from a successful prompt
submission.

For a backend whose clear rotates that reference, the resume prompt MUST NOT be
submitted before that observation.

A backend MAY instead allocate its replacement conversation on its next prompt,
so that the injection is what produces the rotation. For such a backend the
resume prompt MUST be submitted before the observation, because waiting first
cannot terminate, and the renew MUST NOT be completed until the rotation is
observed afterwards. Such an injection MUST NOT be submitted back to back with
the clear, and the delay that separates them MUST NOT be treated as evidence
that the clear landed.

No two prompts a renew submits into one pane may be submitted back to back. A
requester that is also the recipient receives the authorising final reply into
the pane about to be cleared, so the clear MUST NOT follow that delivery
immediately either. As above, an elapsed gap is never evidence about the clear.

A clear that remains unproven past a bounded deadline MUST raise an operator
notice, and MUST raise at most one for that renew. It MUST NOT abandon the
injection, resend the clear, or complete the renew. The deadline bounds the
silence, never the recovery.

A clear still unproven long past that deadline MUST end its cycle rather than
wait indefinitely, and MUST NOT complete it. Ending is legal only once the
resume prompt has been injected: before that the context may be gone with
nothing to re-seed it, and the injection is never abandoned. Ending MUST record
an operator notice and MUST arm a policy's next cycle, because a cycle that
could not be proven is the last reason to stop bounding that context.

A renew MUST replace the recorded observed backend-native session reference on
completion, because the clear is what makes the prior reference false. A renew
needs no reconciliation exemption for that change: a backend-native session
reference is not a binding component, and a changed one is never evidence that a
runtime was replaced (see Adoption).

While a renew is awaiting or performing its clear, deliveries addressed to that
incarnation MUST NOT be submitted. They MUST remain queued and become due after
the resume prompt is delivered. A message delivered into a context that is about
to be cleared MUST NOT be recorded as accepted.

A renew policy uses the same durable recurrence and per-firing ledger as a
repeating tell, with an active-occupancy clock instead of a wall clock. Its
overlap guard MUST prevent a firing from starting another cycle while one is in
flight. It re-arms after each completed injection and MUST terminate when
its incarnation is no longer Ready, and MUST NOT terminate for any other reason:
a cycle that is skipped, aborted, or abandoned unproven MUST arm the next one. A
prepare timeout MUST be recorded as an operator notice and MUST NOT by itself
suspend the policy.

Termination MUST be recorded as an operator notice, in the same durable
transaction as the termination itself, naming the logical agent, the exact
incarnation, the renew, and the reason. A policy is the only thing bounding its
agent's context, adoption restores addressing but not the policy, and nothing
re-raises the event afterwards because a terminated renew is never selected
again — so a termination that is not announced is a context that silently stops
being bounded. Announcing MUST NOT re-arm, retry, or otherwise change when a
policy terminates.

A policy MUST be cancellable before its incarnation ends, by its requester or by
its target and by no one else. A cancel any agent could issue would be a way to
silently disarm another agent's supervision, which is the failure an announced
termination exists to prevent. A cancel MUST be refused while the cycle is
clearing: the context is already gone at that point and only the resume prompt
restores it, so ending there would abandon an injection. That refusal MUST state
that the cancel becomes possible once the cycle finishes. A cancel MUST settle
the cycle's unanswered prepare obligation and MUST be recorded as an operator
notice, in the same durable transaction, naming the policy, the target, the
canceller, and the stated reason.

`report` MUST expose whether an incarnation has a non-terminal renew, and when
it does MUST include its id, phase, cycle, interval, and the due time of that
cycle. A caller MUST be able to tell an armed agent from an unarmed one without
inferring it from anything else. That due time is the cycle's own and MUST NOT
be presented as the next fire for a cycle already in flight.

A policy armed with no explicit due time MUST schedule its first cycle one
interval of observed active occupancy away rather than immediately. Arming
states an interval, not a request to clear now.

The interval MUST accumulate only while a fresh Herdr snapshot observes the
target incarnation as `working` or `blocked`. Time spent `idle` or `done` MUST
NOT advance that cycle's due time. Time with no occupancy observation MUST NOT
exhaust the interval; a sample MAY credit at most a bounded sampling allowance
of previously unobserved time.
This clock MUST NOT infer that idle means the work is done. A cycle already in
`Preparing`, `Ready`, `Clearing`, `Injected`, or `TimedOut` MUST complete its
clear and resume; occupancy MUST NOT abort it. A `socket_inbox` waiter has no
Herdr occupancy and is not this clock. Token counts MUST NOT be the trigger.

## State machines

### Incarnation lifecycle

An incarnation has these states:

```text
Declared -> Starting -> Ready
                    \-> Failed
          \----------> Unknown
Ready -> Retiring -> Retired
Ready -------------> Lost
Starting/Ready -----> Superseded
```

Rules:

- Intent MUST be durable before the first external launch side effect.
- `Ready` means only that the configured runtime readiness condition was
  observed. It does not mean the assignment completed.
- Herdr `idle`, `done`, `blocked`, and `unknown` are observations associated
  with a ready incarnation, not incarnation terminal states.
- A timeout without decisive evidence MUST produce `Unknown`, not `Failed`.
- Retirement MUST preserve worktrees, transcripts, messages, and artifacts by
  default. Destructive cleanup is a separate explicit operation.
- A lost Herdr binding MUST NOT delete the logical agent or its history.

### Ask and reply

```text
Ask persisted
  -> obligation opened
  -> delivery attempted
  -> zero or more progress replies (each delivered to the waiting agent)
  -> one final reply with accepted delivery resolves the obligation
```

The sender MUST return after durable acceptance of the ask unless the caller
explicitly requests a bounded wait. A wait MUST be implemented against the
Kelpie obligation/message state, not against Herdr idle state.

`reply(message_id, payload, progress|final)` MUST not require the caller to
supply owing or waiting logical-agent IDs; those parties are the obligation
owners for `message_id`.

### Renew

```text
Scheduled -> Preparing -> Ready -> Clearing -> Injected -> Done
                     \-> TimedOut -> Aborted        (on-timeout=abort)
                                 \-> Ready          (on-timeout=proceed)
                                              \-> Terminated (clear never proven)
any non-terminal phase ----------> Terminated (incarnation no longer Ready)
```

Rules:

- `Preparing` is entered by delivering the prepare prompt as an ask. The
  obligation and its reminders are the ask's own; renew MUST NOT invent a second
  reminder mechanism.
- A cycle that leaves `Preparing` or `TimedOut` without a final reply MUST
  settle that ask's obligation as cancelled, with a reason, in the same
  transaction. It MUST NOT be recorded as resolved, because no reply was
  delivered. This applies to abort, to termination, and to the `proceed`
  disposition, which stops waiting for the answer at the moment it promotes to
  `Ready`. An obligation outlives every runtime, so a cycle that ends is the
  only thing that can end the question it asked.
- `Ready` is entered only by an accepted final reply to that ask. An agent that
  has issued a final reply has ended its turn, so the clear acts on a settled
  incarnation. `Ready` is left no sooner than a settling gap after that reply,
  which for a self-addressed renew was itself a prompt into the same pane.
- `TimedOut` is entered when the prepare deadline elapses without a final reply.
  The recorded timeout disposition decides whether it proceeds or aborts. There
  is no default disposition.
- `Clearing` is left only on observing a changed backend-native session
  reference, except for a backend that rotates on its next prompt, where it is
  left by submitting the resume prompt and the changed reference is required to
  leave `Injected` instead. In both cases the renew is completed only after a
  changed reference has been observed.
- A renew interrupted in `Clearing` MUST be completed by recovery, not retried
  from the beginning. The incarnation has already lost its context and MUST NOT
  be cleared twice.
- `Injected` is terminal in one of two ways: `Done` when the changed reference
  is observed, or `Terminated` when it is still not observed long past the
  deadline that reported the silence. It MUST NOT be a phase a renew can occupy
  indefinitely.

### Recovery

On startup Kelpie MUST:

1. Open and validate its durable state before issuing Herdr mutations.
2. Identify operations left in non-terminal states.
3. Connect to the selected Herdr session and negotiate compatibility.
4. Obtain a fresh `session.snapshot`.
5. Reconcile each non-terminal incarnation and operation using exact stored
   evidence.
6. Mark ambiguity as `unknown`; it MUST NOT manufacture success or retry
   automatically.
7. Resume subscriptions only after the snapshot baseline is installed.
8. Preserve all open obligations independently of Herdr liveness.
9. Complete, rather than restart, any renew whose clear was already submitted,
   and terminate any renew policy whose incarnation is no longer Ready.

Recovery MUST be idempotent. Repeating it with unchanged durable and Herdr state
MUST produce no new external effects.

## Messaging contract

Kelpie MUST expose semantic operations equivalent to:

- `tell(recipient, payload)`;
- `ask(recipient, payload)`;
- `reply(message_id, payload, progress|final)`;
- `pending(agent)`;
- `ask-info(message_id)` — read-only re-read of one ask's durable body,
  parties, delivery outcome, replies with their delivery outcomes, and
  obligation state, through the id its reminder carries;
- `cancel(message_id, reason)`.

A cancel MUST settle an `open` or `in_progress` ask for any same-user
requester claim. It MUST NOT require the requester to be the waiter. It MUST
record the claimed requester and the reason. An absent ask or a terminal
obligation MUST be refused without mutation. A renew cycle's prepare ask MUST
NOT be settled this way: that obligation is ended only by `renew.cancel` or by
the cycle itself, so a same-user `cancel` cannot disarm supervision. This is
attribution, not authentication.

Cancelling an ask settles its obligation `cancelled` with the stated reason,
and MUST deliver a response naming the reason to the asker when that asker is
addressable: into the asker's Ready pane for `herdr_prompt`, or into the
asker's socket inbox for `socket_inbox`. When the cancelled ask is no longer a
queued delivery, Kelpie MUST also deliver a Kelpie-authored cancellation
notice to the owing agent when that agent is addressable, with the reason.
The owing occupant's envelope MUST NOT carry `reply-to` and MUST NOT look like
a new ask. Neither notice MUST be attributed to the asker or the responder,
and cancellation MUST NOT set the obligation `resolved`. With no addressable
asker the asker's response MUST stay recorded against the obligation, and MUST
be surfaced to the asker's first obligation check after it is again
addressable. With no addressable owing agent the owing notice MUST stay
recorded against the obligation, and MUST be surfaced to the owing agent's
first obligation check after it is again addressable. A cancellation notice
whose transport outcome is unknown MUST NOT be retried; the settled obligation
and the recorded notices are the durable truth.

Every agent-facing message rendering MUST include enough information for the
recipient to reply through Kelpie without guessing:

- sender address;
- message ID;
- reply expectation;
- exact reply command or machine-readable invocation context.

Sender attribution and sender authentication MUST be represented separately.
Until Herdr supplies authenticated per-agent authority, Kelpie MUST document
local sender claims as same-user attribution and MUST NOT present them as a
security boundary.

Kelpie SHOULD support receiver acknowledgement and message-ID deduplication.
Delivery through terminal input MUST be modeled as at-least-once-capable and
potentially ambiguous.

## Local client protocol

Command RPCs (`tell`, `ask`, `reply`, `waiter.register`, `waiter.retire`, and
the other methods) MAY use one
newline-delimited JSON request per connection.

Kelpie MUST expose one `who` identity read accepting exactly one live alias,
pane ID, logical agent ID, or incarnation ID selector. With no selector the
typed client MUST default to its calling pane. The result MUST identify the
logical agent, its public name, its delivery transport, whether it is currently
addressable, and its incarnation and attribution when it has an incarnation.
An alias history read MUST return every claimant and unresolved obligation for
that name. The legacy `whoami`, `name.info`, and `attribution` methods MUST
remain available with their existing result shapes during this migration.

Administration of a socket waiter MUST accept either its logical agent ID or
its unique active public alias. Alias ambiguity MUST fail closed.

That one-shot RPC MUST NOT be the receive path for `socket_inbox` deliveries.
`pending` lists what an agent owes. `ask-info` re-reads one ask by id. Those
methods MUST NOT be the receive path for `socket_inbox` deliveries.

A socket waiter MUST reconnect with `inbox.claim`, naming its LogicalAgent id as
same-user attribution (not authentication), and drain queued deliveries for that
waiter id as `inbox.delivery` events on that connection. `inbox.ack` is the
client acknowledgement. Claiming an id that is not an active socket waiter MUST
be refused. A dropped connection MUST NOT resolve an obligation. A disconnected
host leaves those deliveries `queued` until the same waiter acknowledges them.
`target_unavailable` means the waiter identity is gone, not that the connection
dropped.

## Herdr adapter contract

The Herdr adapter MUST:

- speak the documented socket protocol directly;
- never parse human-formatted CLI output;
- preserve Herdr error codes and messages in operation evidence;
- distinguish connection failure, target absence, malformed response,
  rejection, timeout, and unknown outcome;
- use `session.snapshot` as the baseline for present state;
- treat subscriptions as low-latency hints and reconcile after any gap;
- bind waits and effects to exact observed terminal/incarnation evidence;
- tolerate additive unknown response fields within the supported protocol;
- refuse unsupported protocol versions explicitly (exactly protocol 20);
- avoid the private client/rendering socket and Herdr persistence files.

Kelpie MAY use an upstream typed Herdr client crate if one becomes supported.
Until then, its client types SHOULD be generated from or checked against the
schema bundled with each supported Herdr version.

## Adoption contract

Kelpie MAY bind durable identity to an already-running Herdr agent without
issuing `agent.start`. Explicit adoption is the primitive; silent fleet-wide
auto-adoption is out of scope.

Kelpie MAY perform targeted lazy adoption when a command needs an unbound
calling pane or addresses a missing alias. Caller adoption MUST select the
exact pane and observed terminal. When that exact pane and terminal already
record a unique `lost`, `unknown`, `declared`, or `failed`
incarnation, lazy caller adoption MUST continue that logical agent and MUST
keep its recorded public name: an unnamed live occupant is claimed under that
name, and a live name that does not equal it MUST fail closed. `declared` and
`failed` are included so a rejected or interrupted name claim on first use can
be retried against the same agent rather than wedging the pane. `starting`
incarnations on the same pane and terminal or more than one such logical agent
MUST fail closed and name the ids so the caller can pass an explicit
`logical_agent_id`. A backend change MUST NOT prevent continuation. When none exist, it MAY create a new logical
agent. Lazy caller adoption MUST NOT mint a new logical agent while a
continuable prior incarnation occupies that exact pane and terminal, MUST NOT
replace the continued agent's alias with a working-directory basename, and
MUST NOT continue under a public name another logical agent still holds
unresolved obligations on. Recipient adoption MUST require exactly one
unnamed, non-launch-pending live agent. If its recorded seat has a unique
continuable identity, recipient adoption MUST continue it. A working-directory
basename MAY create a new identity only when the alias has no prior claimant.
Ambiguous or absent matches MUST fail closed. Targeted
lazy adoption MUST use the normal durable adoption contract below and MUST NOT
scan and adopt unrelated live agents.

A parsed adopt request MUST contain:

- exact live selector: Herdr pane ID and expected terminal ID;
- logical agent ID or explicit create-new intent;
- explicit parent or explicit parentless marker (create-new only);
- Herdr session identity as recorded by the caller;
- caller-generated idempotency key.

It MAY constrain the live match with an expected public name and backend kind.
When those constraints are absent, Kelpie MUST use the observed snapshot values
and MUST still fail closed if the agent is missing, still launch-pending, or
bound to a different terminal than selected. Herdr `interactive_ready` means
only that a managed `agent.start` reached Active; it MUST NOT be required for
adopting an already-detected occupant. An observed backend kind plus exact
pane/terminal match is required. A Ready incarnation's public alias MUST equal
the live Herdr public agent name, or a durably recorded pending rename target
(see Rename contract). Kelpie MUST NOT invent `adopted-` aliases.

Adoption MUST:

1. obtain a fresh authoritative `session.snapshot` (or an equivalent
   authoritative present-state baseline);
2. match the exact selector against that snapshot;
2a. resolve a Ready alias conflict against that same snapshot before refusing.
   When the requested public name is held by a Ready incarnation whose exact
   pane and terminal are absent from the snapshot, that binding MUST be marked
   lost for authoritative binding absence and the adoption MUST proceed; a
   binding the snapshot still shows MUST refuse as before. Kelpie MUST NOT
   refuse an adoption on the authority of a stored Ready binding it has not
   checked against Herdr, because liveness is Herdr's fact and a stale binding
   would otherwise make its alias unadoptable until the next recovery. The
   release MUST record an operator notice;
3. persist durable adopt intent before any name-claim effect. If the occupant
   already has a Herdr public name, bind Ready to that exact name. If it has
   none, derive a Herdr-legal name from the working-directory basename (or a
   single pane-derived suffix on collision), persist that intended name, claim
   it through Herdr `agent.rename`, and record Ready only after a fresh
   snapshot shows the same pane, terminal, backend, and name. Rejected or
   unknown rename outcomes MUST NOT be retried blindly;
4. NOT issue `agent.start` or otherwise mutate Herdr topology;
5. treat public names as aliases: create-new never inherits history of a prior
   logical agent that used the same name; continue reuses only an explicit
   logical-agent ID. Continue MUST refuse a logical agent whose
   `delivery_transport` is `socket_inbox`. Because create-new inherits nothing,
   it MUST fail closed when a logical agent already holding that public name has
   an obligation in `open` or `in_progress`, owing or waiting, and the refusal
   MUST name that logical agent. Continuing that agent, or first terminating the
   obligation, are the two paths forward; Kelpie MUST NOT choose either on the
   caller's behalf;
6. reject a second Ready adoption of the same exact live binding unless the
   caller is continuing through an approved supersession path.

Adopted Ready incarnations MUST obey the same recovery and exact-incarnation
rules as launched agents: delayed events MUST NOT mutate a replacement
incarnation; exact absence or replacement in a fresh snapshot yields Lost (or
completed retirement) under existing rules. Recovery MUST require live
public-name equality for every Ready binding. Exact pane, terminal, and
backend kind remain required.

A recorded backend-native agent session is attribution evidence, not a binding
component. It identifies a conversation, which a live runtime MAY rotate on its
own (clear, resume, compaction, fork). Recovery MUST NOT require a later
snapshot to present the same session, and MUST NOT treat a changed session as
absence or replacement. When the binding is otherwise exact and a fresh
authoritative snapshot reports a different session, Kelpie MUST replace the
recorded reference with the reported one, so attribution reads the live
conversation rather than an abandoned one.

A long-lived direct Herdr socket subscription (for example `pane.agent_detected`
and release events) MAY accelerate discovery, but MUST be bootstrapped and
reconciled with `session.snapshot` after startup and reconnect. One-shot Herdr
plugin hooks are optional accelerators only and MUST NOT be the sole authority.

## Rename contract

Correcting a Ready agent's public name spans two authorities: Herdr owns the live
name and Kelpie mirrors it. Kelpie MUST expose that correction as one operation,
because composing it from rename, recovery, and adoption leaves an agent
unreachable if it stops partway, and records a new incarnation for a change that
binds no new runtime.

A rename MUST NOT create an incarnation. An incarnation is one attempt to bind a
logical agent to a live runtime; a rename changes no pane, terminal, process, or
backend, so recording one would misreport history to every consumer of that
history.

A rename MUST record its target name durably before asking Herdr. While that
target is recorded, a Ready binding is exact when the live agent answers to
either the committed name or the target, which is the only permitted divergence
from live-name equality and exists so a failure between intent and commit cannot
strand a live agent as `lost`.

Recovery MUST settle a pending rename from a fresh snapshot: commit it when the
exact pane, terminal, and backend answer to the target, and discard it when they
still answer to the committed name. A rename Herdr rejects MUST leave the
committed name in place. A rename whose outcome cannot be proven MUST remain
pending rather than be retried blindly.

The alias belongs to the logical agent, so a rename renames that agent across its
whole history. Kelpie MUST refuse a name another Ready agent holds, MUST refuse
a name a socket waiter holds, and MUST refuse a name outside Herdr's grammar,
before any external effect.

## Launch contract

A parsed launch request MUST contain:

- logical agent ID or explicit create-new intent;
- public name;
- explicit parent or explicit parentless marker;
- Herdr session and target runtime placement intent;
- backend kind and backend arguments;
- structured initial message with an explicit `tell` or `ask` kind, explicit
  reply expectation implied only by that kind, body or durable reference, and
  sender attribution;
- working directory;
- caller-generated idempotency key;
- bounded readiness timeout;
- requested keep-open or retire-after behavior as instruction, not inferred
  daemon policy.

Requested model, provider, and reasoning effort MAY be part of backend
arguments. Kelpie MUST record requested configuration separately from observed
backend execution metadata. Requested configuration MUST NOT be reported as
proof of what served a turn.

Recorded attribution MUST be readable for an exact incarnation without opening
the durable store by hand, because a caller that cannot read the evidence
cannot verify it. A report MUST keep requested and observed under separate
fields and MUST NOT merge or substitute one for the other. It MUST distinguish
three states that are not interchangeable: no observation has been recorded, an
adapter recorded an explicitly `undetermined` field, and an adapter reported a
value. An absent incarnation MUST be an error, never an empty report. An agent's
self-report about its own model is not observed attribution and MUST NOT be
recorded as such.

An implementation MAY compose several Herdr calls to create topology
and start an agent. It MUST journal the intended sequence and every accepted
resource ID so partial failure can be reconciled. It MUST NOT imply that a
multi-call launch is atomic.

Runtime start and initial-message delivery are separate durable operations. An
incarnation MAY become `ready` regardless of the initial-message delivery
outcome. A launch response MUST expose the runtime-start outcome and the
initial-message delivery outcome separately and MUST NOT collapse them into one
success flag. The initial message MUST have its own immutable message ID,
operation, delivery attempt, and outcome. Its semantic kind and reply
expectation MUST be explicit `tell` or `ask` data and MUST NOT be inferred from
message text. An unknown initial-message delivery MUST remain durable,
operator-visible, and MUST NOT be automatically resent.

## Persistence

The durable store MUST provide:

- atomic commits across records that establish one invariant;
- crash-safe intent recording before external side effects;
- unique constraints for immutable IDs and non-failed uses of idempotency keys;
- append-preserving evidence for attempts and outcomes;
- schema versioning and explicit migrations;
- consistent reads during recovery;
- storage outside repositories operated on by managed agents.

Mutable summary state MAY be materialized for efficient queries, but it MUST be
derivable from authoritative records or updated in the same atomic transaction.

The implementation MUST NOT rewrite a prior external outcome to hide history.
A correction MUST be represented as later evidence or a superseding outcome.

## Error and retry model

Errors MUST be classified as:

- `invalid_request`: rejected before durable intent;
- `incompatible_runtime`: Herdr protocol or capability mismatch;
- `unavailable`: dependency could not be reached;
- `not_found`: exact target is authoritatively absent;
- `conflict`: identity, idempotency, or lifecycle precondition failed;
- `rejected`: dependency received and refused the operation;
- `timeout`: the requested bound elapsed;
- `unknown_outcome`: an external effect may have happened;
- `internal`: Kelpie invariant or storage failure.

Only an operation with a proven non-delivery result MAY be automatically
retried. Unknown outcomes require idempotent reconciliation or explicit caller
policy. Error reporting MUST retain the difference between Herdr being
unreachable and Herdr authoritatively reporting that a target does not exist.

No command or API call may fail silently. Every failure MUST have a stable class,
a human-readable explanation, and the relevant operation/message/incarnation
identifier.

## Security and safety

- Secrets MUST NOT be stored in messages, operation arguments, logs, or durable
  evidence unless a later explicit secret-handling design authorizes it.
- Kelpie MUST pass credential references or inherited authorization to backend
  adapters without logging secret values.
- Destructive cleanup MUST require an explicit exact target and MUST be separate
  from ordinary retire/recovery operations.
- A message body MUST be treated as untrusted text when rendered for terminals.
  Rendering MUST escape or delimit it so it cannot alter envelope metadata.
- Local socket possession and claimed sender name MUST NOT be described as
  authenticated agent identity.
- Application authorization decisions MUST remain above Kelpie unless a later
  general authorization model is specified.

## Observability

Kelpie MUST emit structured logs containing stable identifiers, not message
bodies by default. Relevant events include:

- logical agent and incarnation creation;
- lifecycle transition with prior and next state;
- operation intent, attempt, and resolution;
- message creation and delivery transition;
- obligation creation, progress, resolution, cancellation, and orphaning;
- Herdr connect, disconnect, protocol negotiation, snapshot, and resubscribe;
- recovery decisions and every transition to `unknown`;
- invariant violations.

Logs MUST NOT be the source of truth. Every fact required for recovery MUST be
in durable state.

## Core conformance scenario

Every conforming implementation MUST prove this end-to-end path:

1. Connect directly to one Herdr socket and negotiate the protocol.
2. Persist one logical agent and one start intent.
3. Start or bind one Herdr-managed agent and record its exact incarnation.
4. Persist and deliver one correlated `ask`.
5. Accept one explicit final reply through a Kelpie helper.
6. Resolve the exact obligation.
7. Recover correctly after killing Kelpie or Herdr at each boundary in that
   sequence.

The scenario does not require recurring schedules, remote transport,
application workflow policy, a general workflow engine, or a graphical UI.

A conforming implementation MUST also prove the same ask, accepted final, and
resolve path for a `socket_inbox` waiter, using a reconnectable inbox client
rather than a pane. A final on that path MUST NOT resolve on persist. It MUST
resolve only on accepted socket acknowledgement. Pane waiters MUST keep the
Herdr prompt proofs above.

## Conformance matrix

| Invariant | Required proof |
| --- | --- |
| Intent precedes external effect | Fault test kills Kelpie before and after every Herdr request write and response commit. |
| Old results cannot mutate a new incarnation | Reuse a public name and pane, then deliver delayed results for the old incarnation. |
| Unknown is not success or failure | Disconnect after request submission and verify recovery preserves ambiguity. |
| Ask persists independently of runtime | Kill and restart both Kelpie and Herdr with an open obligation. |
| Progress does not resolve an ask | Send multiple progress replies followed by one final reply. |
| Wrong reply cannot clear an obligation | Reply from the wrong sender and with the wrong `reply_to`. |
| Delivery is not blindly duplicated | Crash after Herdr acceptance but before local outcome commit; verify deduplication or explicit unknown state. |
| Herdr events are not authority | Overflow or omit events, reconnect, and recover from a fresh snapshot. |
| Runtime replacement cannot satisfy old waits | Replace the pane occupant while an operation is pending. |
| A rotated conversation is not a replaced runtime | Reconcile a snapshot reporting a different backend-native session for an otherwise exact binding and verify the incarnation stays Ready with its obligations attached. |
| Attribution evidence follows the live conversation | Reconcile a rotated session and verify the recorded reference is replaced with the reported one. |
| Retirement preserves artifacts | Retire an incarnation and verify worktree and durable records remain. |
| Protocol mismatch fails explicitly | Run compatibility fixtures for supported and unsupported protocol versions. |
| Operator notification is durable | Disable or fail Herdr notification and verify the local inbox record remains. |
| A cleared context is always re-seeded | Kill Kelpie after the clear is submitted and before the resume prompt; verify recovery completes the injection rather than restarting the renew. |
| Clear completion is observed, not assumed | Hold the backend-native session reference unchanged and verify the resume prompt is never submitted. |
| An unproven clear is not retried | Leave a clear `unknown` with no rotation observed, request another, and verify it is refused with `clear_unproven` and no command is submitted. |
| A dead binding does not hold its alias | Close the pane of a Ready incarnation, adopt the same public name on a new pane, and verify the adoption succeeds, the prior incarnation is `lost`, and a notice records the release. |
| Lazy caller adoption does not fork a pane | Lose the Ready binding on a pane that still hosts the same live agent, run a caller command from that pane, and verify the same logical agent is continued with its obligations attached. |
| A renew cannot swallow a message | Deliver a tell while a renew is awaiting or performing its clear; verify it stays queued and arrives after the resume prompt. |
| A renew policy dies with its incarnation | Remove the incarnation's Ready binding and verify the policy terminates instead of re-arming. |
| A policy never dies quietly | Remove the incarnation's Ready binding and verify one operator notice names the agent, the incarnation, the renew, and the reason, and that `report` stops showing that agent as armed. |
| An ended cycle owes nothing | Let a prepare deadline elapse, then verify the ask is cancelled with a reason, absent from `pending`, and no longer reminding. |
| A cycle needs no third party | Arm a policy from one agent onto another, retire the arming agent, and verify the renewed agent can still answer its prepare and the cycle reaches `ready`. |
| A policy cannot be armed on a name | Request a renew naming a live public name and verify it is refused, with no policy armed, over both the CLI and the socket. |
| `--every` ignores idle occupancy | Keep a Ready agent `idle` longer than `--every` and verify it never enters Preparing; then observe `working` for that accumulated interval and verify it does. A cycle already Preparing still completes its clear. |
| A policy aimed wrong can be undone | Arm a policy on another agent, cancel it as that agent, and verify it stops being armed, its prepare ask is settled, and a notice names the canceller and the reason. |
| Supervision cannot be disarmed by a stranger | Cancel a policy as an agent that is neither its requester nor its target and verify the refusal names both and leaves the policy armed. |
| A cancel never abandons an injection | Cancel a policy whose cycle is clearing and verify it is refused, the cycle still completes its resume prompt, and the refusal says to retry after the cycle. |
| A cancellation reaches the asker | Cancel an ask whose asker is Ready and verify a Kelpie-authored `cancellation` message names the reason in the asker's pane, with the obligation `cancelled`, not `resolved`. |
| A cancellation tells the owing agent to stop | Cancel an ask whose delivery to the owing pane was accepted and verify a Kelpie-authored cancellation names the ask id and reason in the owing pane, with no `reply-to`, obligation `cancelled` not `resolved`. |
| A cancellation outlives the asker | Cancel an ask whose asker has no Ready incarnation, then re-adopt that asker and verify pending surfaces the cancellation with its reason and never attributes it to the responder; verify a cancellation whose response was already accepted into a pane does not re-surface. |
| An owing cancellation outlives the owing agent | Cancel an ask whose owing agent has no Ready incarnation, then re-adopt that owing agent and verify pending surfaces the cancellation with its reason; verify a stop-notice already accepted into a pane does not re-surface. |
| Only the responder can reply | As the asker (or a third party), reply to an open ask and verify the refusal names the owing agent, the obligation stays untouched, and nothing is delivered to any pane. |
| Socket-inbox final resolves only on ACK | Occupant `reply` final to a `socket_inbox` waiter: no Herdr prompt to the waiter, persist does not resolve, ACK resolves once, a dropped host leaves the obligation open. |
| Socket-inbox cancel reaches the waiter | Cancel an ask whose asker is a socket waiter and verify a Kelpie-authored `cancellation` reaches the inbox, state `cancelled` not `resolved`, not attributed to the responder. |
| Same-user cancel is not waiter-only | With the waiter gone, cancel as the owing agent or a third Ready agent by ask id and reason; verify `cancelled` (not `resolved`) and the requester recorded. Cancel with the waiter's id still works. A wrong ask id fails closed. A renew prepare ask is refused and the policy stays armed. |
| Socket-inbox reconnect drains one waiter | Create an ask, disconnect, reconnect as the same waiter id, drain the later reply, and ACK; claiming an id that is not an active socket waiter is refused. |
| Waiter retire does not strand open asks | `waiter.retire` while an ask is `open` or `in_progress`: those asks become `cancelled` with reason `waiter retired`, the owing agent is notified when addressable, occupant `pending` does not list them, and a later final is refused as not an open obligation. Queued finals for that waiter are no longer `queued`. Retire with no open asks still only ends targeting. |

Tests SHOULD use deterministic fake Herdr protocol fixtures for state-machine
coverage and real Herdr integration tests for transport, lifecycle, and failure
boundaries.

## Optional upstream Herdr interfaces

Kelpie MUST work against the current public socket API before requiring Herdr
changes. These neutral runtime primitives could simplify the integration:

1. Caller idempotency keys and durable operation handles for agent start,
   prompt, and pane execution.
2. Server-generated opaque agent-instance IDs on inventory and lifecycle events.
3. Server boot/session epoch identifiers.
4. Replayable event cursors, snapshot watermarks, gap detection, and wildcard
   lifecycle subscriptions.
5. A supervised plugin service lifecycle.
6. Authenticated or scoped caller/session identity.
7. Plugin-defined agent providers and resume adapters.
8. A supported typed client crate extracted from Herdr's schema and transport.

Kelpie-specific parentage, message bodies, obligations, schedules, and workflow
policy MUST NOT be prerequisites for these Herdr contributions.

## Open questions

1. **Agent acknowledgement:** exact receiver acknowledgement and deduplication
   handshake for terminal-delivered messages.
2. **Retention:** message, delivery, operation, and operator-inbox retention and
   compaction rules.

## Conformance requirements

Kelpie conforms to this specification only when:

- all steps in the core conformance scenario work against a real Herdr instance without
  invoking the Herdr CLI;
- the corresponding conformance tests pass;
- failure at every external-effect boundary produces a recoverable, explicit
  state;
- no stale result can mutate a replacement incarnation;
- one ask survives restart and is resolved by one correctly correlated final
  reply;
- current state can be reconstructed through durable Kelpie records plus a
  fresh Herdr snapshot.
