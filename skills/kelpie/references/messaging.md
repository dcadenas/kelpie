# Message outcomes and reply obligations

Read when performing messaging operations beyond the entry procedure.

Contents: [Method details](#method-details); [ask vs tell](#ask-vs-tell).

## Method details

- `tell`: send a one-way message to `recipient` + `recipient_incarnation`, or
  `recipient_alias`. An alias resolves once to a unique active logical agent.
  Herdr delivery binds its exact Ready incarnation; a socket waiter receives
  through its logical inbox and accepts on ACK. Supply exactly one of `--body`,
  `--stdin`, or `--file`; there is no positional body. Keep message and delivery
  IDs. `delivery=queued` means held by Kelpie; `delivery=accepted` proves prompt
  submission or inbox ACK, not an answer.

  For future delivery, use `--due-in 10m` or `--due-at 2026-08-12T20:00:00Z`.
  These validate durations and timestamps; `--due-at-ms` requires a correct
  epoch. `--every 15m` creates a wall-clock schedule bound to the resolved
  logical agent and also accepts `--recipient-id ID` without an incarnation.
  Each firing uses that agent's current receive path. Unavailable targets
  produce a firing report without a message or runtime. Missed intervals
  coalesce; pending, queued, or submitted deliveries suppress overlapping
  firings. Unknown firings are not resent and do not stop later intervals.
  The creation receipt identifies the schedule, not a delivered message.
  End it with `schedule-cancel <schedule-id> --reason TEXT`.
  `kelpie --json schedules --sender-id ID` exposes stored tell `body` and
  `idempotency_key` for recognizing an existing schedule.
- `ask`: request a durable answer using the same recipient shape as `tell`.
  Delivery is immediate; due-time flags are refused. The returned message ID
  identifies the obligation. Pending-reply reminders default to twenty minutes;
  use `remind_after_ms` to override or `no_remind: true` to disable them.
  Becoming idle never bypasses the due time.
- `ask-info`: read an ask by message ID, including original body, parties,
  obligation state, and delivery outcomes for the ask and every reply.
- `reply`: supply the ask's `reply_to`, `body`, `progress` or `final`, and
  `idempotency_key`. Kelpie resolves owing and waiting logical agents from the
  obligation and binds the waiter's current receive path. Only accepted final
  delivery resolves the obligation: Herdr prompt acceptance or socket
  `inbox.ack`. Persistence alone is insufficient.
- `pending`: list the recipient's `open` and `in_progress` obligations in
  creation order, followed by cancellation notices recorded while unaddressable.
  Cancellation notices cover asks the agent sent and asks it was answering.
  Tells and Herdr task state are not reply obligations.
- `cancel`: supply `requester_agent_id`, `ask_message_id`, and a non-empty
  reason. Queued messages can be cancelled only before the first Herdr write.
  After submission, reconcile ambiguous delivery without resending. An
  open/in-progress ask obligation remains cancellable from any Ready pane;
  `--sender-id` of the waiter is supported but not required. For a renew prepare
  ask, cancel the policy with `renew.cancel`. Socket waiters receive cancellation
  in their inbox. Addressable owing agents receive a stop-notice; otherwise the
  notice is retained for `pending`. Cancellation records `cancelled`, not
  `resolved`.

## Ask vs tell

Use `tell` to inform and `ask` when the sender needs a durable answer or receipt
of completion. An ask's delivery outcome describes transport, not the answer.
Text written only in a recipient's TUI does not reach the sender.

Keep one idempotency key per intended prompt. Replaying a successful operation
returns its receipt only for the same sender and reply correlation. A proven
terminal failure permits a fresh attempt. Refused pending, accepted, superseded,
or unknown outcomes require reconciliation; changing the key must not bypass
the refusal because the original effect may have landed.

After sending an ask, read its delivery outcome and end the turn. Kelpie pushes
the correlated answer into the sender's pane, waking it when idle. This applies
to children and human decisions relayed through agents. Do not sleep, poll
`pending`, or use `herdr agent wait` for an answer. `pending` lists what you owe;
Herdr `idle` or `done` is not a reply. Do not invent side work to keep the turn
open while waiting.

Ending the turn leaves the Ready incarnation addressable. If instructed to stay
up while awaiting a reply, remain idle without parking, retiring, or closing the
pane. Obligations survive runtime and service restarts.

For a reminder about an existing ask, use `--remind-after-ms`. For work paused
until a known future time, send `kelpie reply <ask-id> --progress`, then schedule
`kelpie tell <your-own-name> --due-in ...`. Resume on that tell and eventually
send the correlated final; replies themselves cannot be delayed.

If a reply call times out, inspect its durable outcome before retrying. On the
next turn, disappearance from `kelpie pending` proves a final was accepted;
`in_progress` only proves the obligation remains open and can still await socket
ACK. Never resend an ambiguous submitted attempt.

Handle received envelopes as follows:

- A tell (`<kelpie from=alice>` without `reply-to`) owes no reply or ack unless
  its body independently calls for a new message.
- An ask (`<kelpie from=alice reply-to=ID>`) owes `kelpie reply ID --final` when
  done. Only the owing agent can reply. Use progress only for an acknowledgement
  or material update required by the task's communication policy. Use
  `kelpie tell` for new information outside that correlation.
- `<kelpie-system cancellation waiting=… cancelled-ask=…>` owes no reply.
  If the question remains relevant, re-ask its current owner.
- `<kelpie-system cancellation owing=… cancelled-ask=…>` cancels your work on
  that ask. Stop; no reply is owed.
- An unfamiliar `<kelpie-reminder …>` carries a real outstanding question.
  Recover details with `kelpie ask-info <ask-id>`, then answer or cancel it.
