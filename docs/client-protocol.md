# Local client protocol

`kelpied` is a foreground process. It opens and recovers the durable store
before binding its local Unix socket:

```text
kelpied
```

The database defaults to `$XDG_STATE_HOME/kelpie/kelpie.sqlite3` (or
`~/.local/state/kelpie/kelpie.sqlite3`), and the local socket defaults to
`$XDG_RUNTIME_DIR/kelpie/kelpie.sock`. Herdr uses `$HERDR_SOCKET_PATH` when set,
then `$XDG_CONFIG_HOME/herdr/herdr.sock` (or `~/.config/herdr/herdr.sock`).
`--database`, `--socket`, and `--herdr-socket` override these conventions for
isolated instances. An existing Kelpie socket is never removed automatically.

Herdr's socket appears when Herdr starts, which under a supervisor can be later
than `kelpied` starts. Startup waits up to `--herdr-wait-ms` (default 120000)
for that path to exist before recovering; `0` checks once and fails. The wait
polls the path and never connects, because a probe connection would consume an
`accept` on Herdr. Socket presence is not readiness: the first and only
connection is the one that negotiates the protocol, so an unreachable or
incompatible Herdr still fails through the normal classified path.

The local protocol is one newline-delimited JSON request per connection. Each
request contains `id`, `method`, and `params`; each response echoes `id` and
contains either `result` or an error with a stable `class` and human-readable
`message`. Initial methods are `recover`, `start`, `adopt`, `tell`, `ask`,
`reply`, `clear`, `renew`, `renew.cancel`, `pending`, `cancel`, `retire`,
`waiter.register`, `waiter.retire`, `inbox.claim`, `inbox.ack`,
`notice.create`, `notice.list`, `name.info`,
and `whoami`, using the fields in the corresponding SPEC contracts.

`inbox.claim` is the exception to one-request-per-connection. The host process
reconnects, names its waiter LogicalAgent id (same-user attribution, not
authentication), and keeps the connection. Kelpie writes `inbox.delivery`
events for queued deliveries of that waiter only. The client acknowledges with
`inbox.ack`. A delivery line is complete only at the newline; discard a trailing
fragment with no newline and wait for reconnect to re-offer that queued row.
Dropping the connection leaves those deliveries `queued`. Claiming an id that
is not an active socket waiter is `conflict`. `pending` and `ask.info` are not
this receive path.

`kelpie adopt --pane ID --terminal ID [--logical-id ID]` is the client form.
`--logical-id` continues that exact logical agent in a new incarnation, keeping
its obligations, messages, and history; without it adoption creates a new logical
agent that reuses the public name and inherits none of them. That distinction is
the difference between recovering an agent and duplicating it, so an agent that
is alive in Herdr but has no Ready binding is adopted with `--logical-id` rather
than replaced by a fresh start.

`adopt` binds durable identity to an already-running Herdr agent without
`agent.start`. Params require `pane_id`, `expected_terminal_id`, `parent`,
`herdr_session`, and `idempotency_key`. Optional `public_name`, `backend_kind`,
and `logical_agent_id` (continue) further constrain the match. Kelpie snapshots
Herdr, fail-closes on missing, launch-pending, or mismatched agents, and
returns `logical_agent_id`, `incarnation_id`, `operation_id`, and `outcome`.
Unnamed occupants claim a cwd-derived Herdr name before Ready. Name reuse on
create-new does not inherit history. A notice is committed to the local
inbox before any optional display mechanism; display failure cannot remove it.
Claimed sender IDs provide same-user attribution only and are not an
authentication boundary.

## Identity and addressing

- Durable IDs (`logical_agent_id`, `incarnation_id`, `message_id`, operation and
  delivery IDs) are immutable primary handles.
- Public names are reusable live aliases. They are never primary keys.
- `start` without `logical_agent_id` creates a new logical agent. With
  `logical_agent_id`, Kelpie continues that exact agent in a new incarnation and
  preserves its obligations and history. A new logical agent that reuses a
  public name does not inherit prior history.
- `tell`, `ask`, and `clear` accept either exact `recipient` +
  `recipient_incarnation`, or
  `recipient_alias` resolved once at send time to the unique Ready agent for
  that public name. The resolved IDs are what durable records store; later
  alias reuse does not retarget them. Results echo the resolved recipient IDs.
- `waiter.register` creates a pane-less LogicalAgent with
  `delivery_transport=socket_inbox`. It mints no incarnation. `waiter.retire`
  ends that targeting and releases the name. `ask` `from_operator` attributes
  the stored sender as the operator; `waiting_agent_id` stays the waiter, and
  occupant envelopes still use `from=` equal to the waiter's public name.
- `inbox.claim` holds a reconnectable inbox for that waiter id. Deliveries
  arrive as `inbox.delivery` on that socket. `inbox.ack` marks the named
  delivery `accepted`. Persist is not acceptance.

## Messaging methods

Waiting for readiness distinguishes "not yet" from decisive failure, mirroring
Herdr's own wait loop. A pane whose terminal, backend kind, or public name *is
reported and disagrees with* the intent fails immediately rather than being waited
on, because that pane is hosting something else. A field Herdr has not populated
yet is undetermined, never a conflict: Herdr detects a pane before it identifies
what runs there, so a null backend kind or an unbound name early in the window is
"not yet". An agent that is neither `interactive_ready` nor `launch_pending` *and*
whose name is gone fails immediately: Herdr has no pending start, no confirmed
agent, and no binding, so the condition can never become true. Polling
a decisive failure to the deadline reports `unknown` for a start that definitely
failed, and an `unknown` start is what callers resolve by spawning a duplicate.

`start` fails closed before any durable intent when the live pane is absent, holds
a different terminal, has a different cwd, or already hosts an agent. An occupied
pane is `conflict` with `code` `pane_occupied`, naming the occupant's terminal,
backend kind, and public name. That check matters because Herdr reports both "no
usable shell yet" and "an agent already lives here" as `agent_pane_busy`, and only
the first is worth waiting on.

A start rejected with `agent_pane_busy` is retried, because Herdr received and
refused it, which is proven non-delivery. Retries stop after ten seconds or at
`--timeout-ms`, whichever comes first, and every attempt keeps its own journaled
record with its own request ID. Each retry re-snapshots and aborts immediately if
an agent has taken the pane. No other start rejection is retried:
`agent_pane_not_found`, `agent_pane_unavailable`, `invalid_agent_name`,
`unsupported_agent_kind`, and duplicate names are deterministic. The retry is a
workaround for herdrdev/herdr#2773, fixed upstream after 0.8.0, and is marked for
deletion in `slice.rs` once the minimum supported Herdr carries the fix.

Error bodies carry a stable `class` and, where one exists, a finer `code`:
Herdr's own rejection code passed through unaltered, or a Kelpie code such as
`pane_occupied`. Branch on `class` and `code`, never on `message`.

`start` is a composed launch request. Its result reports `runtime_start` and
`initial_message` separately. `runtime_start` contains its operation ID and
outcome. `initial_message` contains its immutable message ID, independent
operation ID, and delivery outcome. A Ready incarnation does not imply that the
initial message was delivered, and an `unknown` delivery is never resent
automatically.

The client accepts `--due-in 10m` (units `s`, `m`, `h`, `d`) and `--due-at`
with a UTC RFC3339 timestamp ending in `Z` or `+00:00`, resolving both to
`due_at_ms` before the request is sent; exactly one of the three forms is
allowed. They exist because a wrong epoch fails in the worst way available —
silently, as a delivery at the wrong moment — while a malformed duration or
timestamp fails at parse. `--due-at` is UTC only: applying an offset to a wall
clock a caller may not have meant is the same silent failure.

`tell` and `ask` deliver structured messages to an exact Ready incarnation.
`ask` refuses `due_at_ms` with `invalid_request`: postponing an ask creates an
obligation the recipient cannot see, owed on the server and absent from their
pane, which reads to every observer as an ask that went unanswered. Reminders
(`remind_after_ms`) cover being nudged about an ask already in flight, and a
`tell` covers a message that should arrive later.

On a `tell`, optional `due_at_ms` (Unix epoch milliseconds, same store
`SystemTime` clock as other timestamps) persists the delivery as `queued` and
fires it once when
`now_ms >= due_at_ms` against that exact Ready incarnation. A reminder is a
delayed tell. There is no recurring schedule and no receiver ack. Cancel of a
queued delivery is legal only before the first Herdr write; after submit, the
existing no-resend and unknown rules apply. If the due time elapses while
`kelpied` is down, recover marks the delivery `unknown` instead of firing it
on restart. `kelpied` uses a non-blocking accept timeout so due work runs with
no client connected. Durable attempt intent is recorded before the Herdr
write. A due-vs-accepted race is resolved in one SQLite writer: submit
requires the row still `queued` and due; cancel requires the row still
`queued` with no submitted attempt.

Every ask creates a correlated pending-reply reminder with a five-minute
default interval. `--remind-after-ms MS` changes the interval and `--no-remind`
disables automatic nudges for that ask. The interval begins only after Herdr
accepts the ask. When overdue, `kelpied` obtains a fresh snapshot and injects
only if the exact owing incarnation is `idle` or `done`. Progress resets the
interval. If an unanswered receiver first works and then reaches `idle` or
`done`, the stopped boundary can trigger the initial reminder before the
interval. Final reply resolution stops reminders. `reminder-snooze <ask-id>
--until-ms MS` pauses injection, and `reminder-disable <ask-id>` stops it without
resolving the ask.
Agent-facing prompt text uses compact HTML-like envelopes (not NDJSON):

```text
<kelpie from=alice>
BODY
</kelpie>

<kelpie from=alice reply-to=<message-id>>
BODY
</kelpie>
```

Bodies escape `<`, `>`, and `&`. Tell IDs, `to`, `kind`, and body wrappers are
omitted from the envelope. Machine client-to-daemon traffic remains strict
NDJSON.

Kelpie never sends `agent.prompt`'s optional `wait` option. That option blocks
until the recipient's turn settles to `idle`, `done`, or `blocked` — it waits for
the answer, not for the delivery. Kelpie reports whether a message reached an
agent and returns; a reply arrives later as its own delivery, which is what lets
a sender stay idle instead of holding a turn open for work that can take hours.

`reply` takes `reply_to` (the ask message ID), `body`, `disposition`
(`progress` or `final`), and `idempotency_key`. Kelpie resolves the exact owing
sender and waiting recipient from the durable obligation and binds the waiter's
receive path. A `herdr_prompt` waiter is the unique Ready incarnation, delivered
as a compact receiver envelope through Herdr:

```text
<kelpie from=bob re=<ask-message-id> progress>
BODY
</kelpie>

<kelpie from=bob re=<ask-message-id> final>
BODY
</kelpie>
```

A `socket_inbox` waiter is that inbox, with no Herdr prompt. Persist queues the
delivery; the socket client's `inbox.ack` is acceptance. Dropping the host
leaves the delivery queued and the obligation open.

Wrong or stale correlation fails closed. Progress sets the obligation
`in_progress` when the reply is recorded and does not resolve it. A final reply
resolves the obligation only when delivery is accepted; rejected or unknown
final deliveries leave the obligation open/in-progress and report the delivery
outcome without claiming the waiter received the answer. Ambiguous submitted
reply prompts are never blindly resent. On success the result includes
`message_id`, `delivery_outcome`, and `obligation_state`. Pane replies also
include `operation_id` and `recipient_incarnation`.

`clear` replaces one Ready incarnation's backend-native conversation without a
prepare ask or resume prompt. Params take the same recipient shapes as `tell`
(`recipient_alias`, or exact `recipient` plus `recipient_incarnation`) and an
`idempotency_key`. The result carries `operation_id`, the resolved recipient
IDs, and `outcome`.

The command is resolved from the same verified backend table used by `renew`.
Unknown kinds fail before durable intent with class `incompatible_runtime` and
code `renew_unsupported_backend`; Kelpie never defaults to `/clear`. For
`claude`, `codex`, `grok`, and `pi`, the operation submits the backend command
and waits until Herdr exposes a session reference differing from the pre-clear
reference. For `opencode`, it submits `/clear` and returns after acceptance:
the caller's next prompt allocates the replacement conversation, so waiting
would deadlock. Every clear is journaled before the Herdr write, and an
ambiguous submission is never resent automatically.

The client connection stays open while an on-clear backend rotates, but the
daemon parks that connection and continues serving other requests. An absent
session reference is not rotation. Clear also waits out the same short settle
gap renew uses after a preceding prompt write attempt, including reminders and
unknown outcomes, because back-to-back prompt submissions can be accepted and
lost. Deliveries and reminders stay queued while an on-clear operation awaits
rotation, including progress and final replies; a final reply resolves its
obligation only after the queued delivery is accepted. The first prompt after
any submitted standalone clear that succeeds or becomes unknown also waits out
the settle gap, including the prompt that allocates an OnNextPrompt backend's
replacement conversation. The fire-time gate also holds previously scheduled
deliveries, reminders, and renew prepare prompts. Queued deliveries carry the
post-clear deadline durably, so `recover` during the gap preserves them rather
than reporting an ambiguous missed wake. Clear intent stores the settle duration
used when recovery must reconcile a submitted clear. If recovery must mark that
submitted clear unknown, it creates an operator notice because the original
client connection is gone. One
incarnation cannot have standalone clear and an in-flight renew cycle together;
a future scheduled renew cycle is deferred until clear finishes. Recovery fails
a clear intent interrupted before its first Herdr write so it cannot wedge later
delivery or lifecycle work.

```sh
kelpie clear <recipient> | --recipient-id ID --recipient-incarnation ID
```

`renew` bounds one incarnation's backend-native context by clearing it and
re-seeding it. Params take `requester`, exact `recipient` plus
`recipient_incarnation`, `prepare_prompt`, `prompt`, `on_timeout` (`abort` or
`proceed`), `prepare_timeout_ms`, and at most one of `due_at_ms` or
`every_ms`. Unlike every other addressed operation, `renew` takes **no**
`recipient_alias` and the daemon refuses one: a public name is a reusable live
alias, and a policy aimed at the agent that happens to hold it clears that
agent's context once a cycle. The client sends
the caller's own IDs when no recipient is given. The result carries `renew_id`,
the resolved recipient IDs,
`scheduled_at_ms`, `on_timeout`, `phase`, and `every_ms` when the renew is a
policy. Nothing is written to Herdr by the call; the daemon's phase driver owns
every external effect, the same way due deliveries and reminders do.

The backend's clear command is resolved before any durable intent, so an
unsupported runtime is refused with class `incompatible_runtime` and code
`renew_unsupported_backend` rather than having its context destroyed by a guess.
Each entry pairs a command with when that backend's replacement conversation
becomes observable: `/clear` on-clear for `claude` and `codex`, `/new` on-clear
for `grok` and `pi`, and `/clear` on-next-prompt for `opencode`. Commands are
read from each backend's own shipped documentation or binary and none is
inferred from another; the timing is measured against a live session, because
documentation does not answer it. An incarnation may
hold one active renew, and a target without an exact Ready binding is
`conflict`.

Phase one is a real ask. The prepare prompt is delivered under the renew's own
idempotency key with the ask's obligation, reminders, `pending` visibility, and
cancellation, and only that ask's accepted final reply opens the clear. The
obligation is owed by the incarnation being renewed, not by whoever armed the
policy: it authorises a destructive local operation, so it must not depend on a
third party being Ready. The requester stays on the policy for attribution and
for the cancel permission, and is the prepare envelope's sender. An agent
must end its turn to issue a final reply, so the clear acts on a settled
incarnation rather than interrupting a live turn. The rendered envelope quotes
the resume prompt verbatim, because the checkpoint's only reader is that agent
with an empty context holding nothing but the resume prompt.

Phase two uses the shared clear command and rotation-proof path, submitting the
clear once and polling `agent.get` until the observed
backend-native session reference differs from the reference recorded before the
clear, then injects the resume prompt. The pre-clear reference is durable before
the write, since completion can only be proven against it. Two prompts submitted
back to back are silently accepted and lost by the backend, which is why nothing
is sent until rotation is observed; elapsed time and `idle` cannot distinguish
"not cleared yet" from "cleared" and are not used.

For a backend that allocates its replacement conversation on its next prompt,
those two steps swap. Waiting for a rotation before injecting cannot terminate
there — the injection is what produces it — so the resume prompt is submitted
once a short gap has passed, and the rotation is then required before the renew
completes. The gap exists only because two prompts submitted back to back are
lost; nothing is concluded from it. If no new conversation appears afterwards,
the clear never landed and the resume prompt went into the context it was meant
to replace, which is reported rather than recorded as success.

A clear still unproven 60 seconds after it was sent raises one operator notice
naming the backend, and only one. The renew keeps polling and never completes:
the deadline bounds how long the condition can go unreported, not how long
Kelpie will try to re-seed an incarnation whose context is already gone.

The clear and the injection have deliberately opposite retry rules. The clear
follows Kelpie's usual no-blind-resend rule — a second one would discard the
context that was just re-seeded. The injection does not: a duplicate resume
prompt tells an agent its own instructions twice, while a missing one leaves it
cleared, idle, and instructionless forever, with nothing inside it that could
notice. Each injection attempt is journaled under its own request ID.

Completion replaces the recorded observed backend-native session reference,
because the clear is what makes the prior reference false; leaving it would
point `attribution` at a transcript that will never grow again. Recovery does
not read that change as a replaced runtime: a session reference is not part of
the exact live binding at all, for any agent. Over the
clear window, deliveries addressed to that incarnation stay `queued` with their
schedule pushed past the injection.

`due_at_ms` renews once. `every_ms` re-arms after each completed injection and
terminates when the incarnation stops being Ready. A prepare timeout applies the
recorded disposition and records an operator notice; it never suspends a policy.
The client command is:

```sh
kelpie renew [--recipient-id ID --recipient-incarnation ID] \
  (--prepare-prompt TEXT | --prepare-prompt-file PATH) \
  (--prompt TEXT | --prompt-file PATH) \
  --on-timeout (abort | proceed) \
  [--prepare-timeout 10m | --prepare-timeout-ms MS] \
  [--due-in 45m | --due-at RFC3339 | --due-at-ms MS | --every 45m]
```

With no recipient the client resolves the caller through `whoami` and arms the
policy on that incarnation. A caller identified only by agent id cannot
self-target, because an agent id does not name an incarnation; those callers
pass both exact IDs.

`renew.cancel` ends a policy before its incarnation does. Params take `renew_id`,
`requester_agent_id`, and `reason`; the result carries `renew_id` and the
`notice_id` of the operator notice recording the cancel. Permitted only to the
policy's requester or its target — anyone else is refused with class `conflict`
and a message naming both. A cancel is also refused while the cycle is
`clearing`, because the context is already gone and only the resume prompt
restores it; the refusal says to retry once the cycle finishes. Cancelling
settles the cycle's unanswered prepare obligation in the same transaction.

```sh
kelpie renew-cancel <renew-id> --reason TEXT
```

The two prompts take named text/file flags rather than the shared
`--body`/`--file`/`--stdin` form, which cannot address two prompts
unambiguously. `--on-timeout` is required and has no default; `--prepare-timeout`
defaults to ten minutes, generous because the prompt queues behind whatever turn
the agent is already running. Both prompts are read once and stored, so editing
the source files later changes nothing about an armed policy.

`pending` takes one logical `agent_id` and returns that agent's durable `open`
and `in_progress` final-reply obligations in creation order, followed by the
agent's cancelled asks whose response no pane has received — the ones settled
while the agent had no Ready binding, up to the current binding's creation
(each with `state` `cancelled`, `cancellation_reason`,
`cancellation_requester_agent_id`, and `cancelled_at_ms`). A failure reading
cancellations fails the whole request. It does not infer anything from current
Herdr runtime state.

`name.info` takes one public `name` and returns, read-only, every logical agent
holding that name (`logical_agent_id`, `created_at_ms`, `live`,
`unresolved_count`) and every unresolved ask touching them, each with both
parties resolved to agent IDs, names, and liveness (`asker` is the waiter,
`responder` is the agent that owes the final reply). It is the diagnosis behind
a create-new refusal in one command: a refusal under this name lists the same
asks, parties, and three remedies — continue the claimant with `--logical-id`,
cancel each ask (`kelpie cancel <ask-id> --reason <why> --sender-id <asker-id>`),
or take a different name by renaming the agent in Herdr and adopting under it.

`retire` records that an incarnation is finished. On its own it sends nothing to
Herdr, so the pane stays occupied and the caller holds the other half of one
intent. `close_pane` (client `--close-pane`) completes it: the pane is released
and the retirement is reconciled from a fresh snapshot in the same call, with
`pane_released` reporting whether absence was proven. Closing is opt-in because
it ends a live process; it preserves the worktree, transcripts, messages,
obligations, and durable records, so it releases a runtime rather than cleaning
anything up. The exact live binding is re-proved immediately before the close and
the close is refused if the pane now hosts a different agent — a reused pane
closes just as readily as the intended one. A refused close leaves the retirement
intent standing.

`rename` moves one Ready agent to a new public name as a single operation. Params
take exactly one of `agent_id` or `alias`, plus the new `name`. It keeps the same
incarnation, process, pane, terminal, working directory, lineage, and
obligations, and it deliberately creates no incarnation: a rename binds no new
runtime, and recording one would misreport history.

Order is intent, effect, proof, commit. The target name is durable before Herdr is
asked, and the committed name changes only after a fresh snapshot shows the same
exact pane, terminal, and backend answering to it. While a target is pending, a
Ready binding is exact under either name, so a failure between intent and commit
cannot strand a live agent as `lost`; recovery commits the rename when the target
is live and discards it when the committed name still is. A rejected rename keeps
the committed name; an unprovable one stays pending rather than being retried.
Names another Ready agent holds, or names outside Herdr's grammar, are refused
before any external effect. The client command is
`kelpie rename [alias] | --sender-id ID --name NEW-NAME`.

`report` returns every durable node and edge Kelpie owns, at one moment. Nodes are
logical agents with their incarnations, newest first, so one row per agent renders
the current incarnation without walking retired history. Edges are parentage
(`parent_agent_id`) and reply obligations, which carry owing and waiting agent,
state, creation and last-activity times, and the resolving message when resolved.
`alias_collisions` maps each public name held by more than one agent to those
agent ids; counting identical strings is arithmetic, not a verdict.

Each incarnation carries `native_session_rotated_at_ms`, when its current
backend-native conversation was observed to start. The client renders it as
`conversation=1d16h` beside `incarnations=`. It is not `created_at_ms`: that
records when the incarnation was bound to a runtime, and the two agree only
until the conversation first rotates. A clear, compaction, resume, fork, or
renew starts a new conversation while the incarnation continues, so an agent
bound three days ago may be four hours into its current context.

The value is `null`, and renders as `conversation=unknown`, until Kelpie
observes a rotation. It is never defaulted to `created_at_ms`. Every existing
incarnation begins unknown and becomes known at its next rotation, because there
is no honest way to learn a conversation start that was never observed, and a
backfilled column would report the incarnation's age as the conversation's for
as long as that agent lives. The stamp is written where reconciliation already
refreshes the session reference, so it costs no extra Herdr traffic and records
when the boundary was seen rather than when it happened.

Each incarnation also carries `renew`: `null` when no cycle is armed, otherwise
`renew_id`, `phase`, `cycle`, `every_ms`, and `cycle_due_at_ms`. The client
renders it as `renew=scheduled cycle=97 every=45m0s next-in=15m0s`. A policy is the
only thing bounding an agent's context and it terminates when its incarnation
stops being Ready, so `null` on a long-lived root is the answer that matters:
nothing else in the report distinguishes a supervised agent from an unsupervised
one, and adoption restores addressing without restoring the policy.

`every_ms` is `null` for a one-shot renew and set for a standing policy.
`cycle_due_at_ms` is the due time of the cycle named by `phase`, written once
when that cycle was armed and never updated, so it is the next fire only while
the phase is `scheduled`; for a cycle already in flight it is that cycle's own
due time, in the past. The client renders `next-in=` only for a scheduled cycle
for that reason. `renew_id` names the cycle, not the rule: a standing policy
mints a new id for each successor, so continuity is `every_ms` plus a rising
`cycle`, not a stable id.

The report never interprets. No state is labelled healthy, stuck, or missing,
because whether a state warrants attention is the consumer's policy. Requested
model, provider, effort, and `backend_args` appear under `requested` on each
incarnation and are launch intent; observed attribution stays behind
`attribution`, and neither is presented as the other.

`live` (client `--live`) attaches Herdr's current agent status to each incarnation
under `live`, matched by exact observed pane and terminal so a replaced runtime
cannot lend its status to an older incarnation, and sets `live_snapshot_at_ms`.
That status is Herdr's fact taken at report time, not durable Kelpie state, which
is why it is opt-in and timestamped. The client command is `kelpie report
[--live]`, printing a parentage tree; `--json` returns the graph. Params take
exactly one selector: `incarnation_id` (exact), `agent_id` (that agent's newest
incarnation by creation order), `alias` (requires a live Ready binding), or
`pane_id` (read-only; unlike `whoami` it never lazily adopts). Two selectors are
`invalid_request`; an absent target is `conflict`.

The result carries `logical_agent_id`, `incarnation_id`, `public_name`,
`backend_kind`, `incarnation_state`, a `requested` object, the latest
`observed` observation, and the full append-only `observations` history oldest
first. `requested` and `observed` are separate keys and are never merged:
requested is launch intent, observed is evidence. Three states are distinct and
a verifier must not conflate them — `observed` is `null` with an empty
`observations` when nothing has been observed; an observed field is
`{"status":"undetermined"}` when an adapter ran but could not determine it; and
it is `{"status":"reported","value":…}` when it did. Adapters exist for
`claude`, `codex`, and `opencode`; every other backend kind records
`undetermined`. The client command is
`kelpie attribution [alias] | --pane ID | --agent-id ID | --incarnation-id ID`,
defaulting to `$HERDR_PANE_ID`.

Binding-time observation only sees what a backend has already written, and a
backend may record its serving model only after its first turn. `--refresh`
(param `refresh`) observes again and appends the result, so an earlier
`undetermined` stays in the history as the honest answer for that moment
instead of being rewritten. A refresh reads local backend artifacts and, when no
native session was recorded yet, takes one read-only Herdr snapshot to learn it;
it mutates nothing in Herdr. The session is accepted only from a live agent
still matching that incarnation's exact pane, terminal, backend kind, and public
name, and only while the recorded session is empty, so a replacement in the same
pane cannot donate its session to an older identity.

A refresh that still determines nothing reports `undetermined_because`, which is
diagnostic rather than evidence and is not stored. It distinguishes a session
that has produced no assistant turn yet — ask again after its first reply — from
one absent from every store, from an incarnation with no native session recorded
at all. `undetermined` is never softened into a guess.

The `opencode` adapter reads OpenCode's own SQLite stores. One directory holds
several (`opencode.db`, `opencode-local.db`, per-workspace files), so the session
is searched for rather than assumed to live in a default file, and model identity
is read from the newest assistant row because a session can change model mid-run.

`cancel` takes `requester_agent_id`, `ask_message_id`, and a non-empty `reason`.
The requester is an unauthenticated same-user identity claim. Kelpie checks the
neutral durable ownership invariant that it equals the obligation's
`waiting_agent_id`, records the claim and reason, and permits only `open` or
`in_progress` to become `cancelled`. A pane waiter receives Kelpie's
cancellation through Herdr when Ready. A socket waiter receives it on the inbox;
the obligation is `cancelled`, not `resolved`, and the message is not attributed
to the responder. Authenticated or capability-bearing
transports must validate the requester claim above Kelpie before invoking this
method.

`recover` negotiates Herdr compatibility, obtains a fresh authoritative
snapshot, and reconciles durable state without retrying ambiguous effects. A
previously Ready incarnation becomes Lost when the snapshot no longer contains
its exact pane, terminal, backend kind, and public name. A recorded native
agent session must still match when present. This preserves the logical
identity, messages, obligations, and recorded working directory.

`kelpie` is the local client. Ordinary use is typed (`kelpie tell NAME --stdin`,
`kelpie ask NAME --file PATH [--due-at-ms MS]`, `kelpie reply ASK --final --stdin`,
`kelpie start --name NAME --pane ID --terminal ID --backend KIND --cwd PATH
--timeout-ms N --keep-open|--no-keep-open --parentless|--parent-id ID
--tell|--ask --stdin|--file|--body`). The CLI
constructs request IDs and idempotency keys. Typed `start` builds the existing
`StartIntent` params and does not add fields. `start --ask` needs an agent
waiting identity because an ask opens an obligation someone must be owed; an
absent `--sender-id` resolves to the caller's Ready binding, the way `tell`,
`ask`, `pending`, and `cancel` resolve theirs. `--sender-id` remains the explicit
override, and it stays distinct from `--parent-id`: a parent is the new agent's
lineage, a sender is who waits for the answer, and a coordinator may start a
worker whose reply is owed to a third agent. `start --tell` has no obligation, so
it stays operator-attributed unless `--sender-id` says otherwise. Message bodies should come from
`--stdin` or `--file` so the shell does not expand them. Exact addressing uses
`--recipient-id` plus `--recipient-incarnation` and must not also take a name.
The parser fails closed on unknown, duplicate, conflicting, or extra tokens.
`--json` prints the daemon NDJSON. Default receipts show accepted, rejected,
target-unavailable, and unknown outcomes; any non-success exits nonzero.

`kelpie KELPIE_SOCKET` remains the raw socket client. It reads exactly one JSON
request from standard input, half-closes its write side, reads exactly one
NDJSON response line (without requiring the daemon process to exit), persists
that line to a receipt file, best-effort writes it to standard output, and
exits nonzero only for a missing/invalid socket response or protocol error body.
Stdout write failures (for example EPIPE when a capture pipe is already gone)
do not fail a successful RPC once the receipt is on disk.

Default receipt path: `$XDG_RUNTIME_DIR/kelpie/last-response.ndjson` (or
`$TMPDIR/kelpie-client/last-response.ndjson`). Override with
`KELPIE_RECEIPT_PATH`. A companion `last-client-trace.json` records connect /
request / response / receipt / stdout timing for diagnosis.

The daemon flushes and half-closes its write side after each response. If a
client loses its output capture after a successful RPC, recover the correlated
JSON from the receipt file. For example:

```sh
printf '%s\n' '{"id":"check-1","method":"unknown","params":{}}' \
  | kelpie "$XDG_RUNTIME_DIR/kelpie/kelpie.sock"
```

Build and test both executables with an external target directory:

```sh
export CARGO_TARGET_DIR="${TMPDIR:-/tmp}/kelpie-target"
cargo build --all-targets
cargo test --all-targets
```

Install the agent-facing skill globally with the open skills CLI:

```sh
npx skills add dcadenas/kelpie --skill kelpie -g
```

Omit `-g` for a project-local installation. The skills CLI selects supported
agents and owns installation, updates, and removal. `kelpie --skill` prints the
release-matched copy embedded in the binary for manual installation or
inspection.
