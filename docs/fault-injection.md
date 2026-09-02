# Deterministic subprocess fault injection

Kelpie's subprocess tests synchronize through explicit Unix-socket rendezvous
points. They do not use sleeps or timing windows. Default daemon execution does
not activate any point: `KELPIE_TEST_FAULT_POINTS` must contain an exact point
name and `KELPIE_TEST_FAULT_SOCKET` must identify a listening harness socket.
When activated, the daemon reports the point name and blocks until the harness
writes one byte or kills the process.

The compiled points are test infrastructure, not a public operational API:

- `daemon_bound`: startup recovery is complete and the Kelpie socket is bound;
- `start_after_submitted_before_write`: the start attempt is durably
  `submitted`, a Herdr connection exists, and no `agent.start` request byte has
  been written.
- `start_after_write_before_response`: the complete `agent.start` request has
  been written and flushed, while no response has been read or decoded.
- `start_after_response_before_commit`: a structured `agent.start` response has
  been decoded, while the durable operation remains `pending`, the attempt
  remains `submitted`, and the incarnation remains `starting`.
- `ask_after_submitted_before_write`: the ask message, Open obligation, prompt
  operation, delivery, and request attempt are durable; the attempt and delivery
  are `submitted`; a Herdr connection exists; and no `agent.prompt` request byte
  has been written.
- `ask_after_write_before_response`: the complete ask `agent.prompt` request has
  been written and flushed, while no response has been read or decoded.
- `ask_after_response_before_commit`: a structured `agent.prompt` acceptance
  response has been decoded, while the prompt operation remains `pending`, its
  request attempt and delivery remain `submitted`, and its obligation remains
  `open`.
- `tell_after_submitted_before_write`: the tell message, prompt operation,
  delivery, and request attempt are durable; the attempt and delivery are
  `submitted`; no reply obligation exists; a Herdr connection exists; and no
  `agent.prompt` request byte has been written.
- `tell_after_write_before_response`: the complete tell `agent.prompt` request
  has been written and flushed, while no response has been read or decoded.
- `tell_after_response_before_commit`: a structured `agent.prompt` acceptance
  response has been decoded, while the tell's prompt operation remains
  `pending`, its request attempt and delivery remain `submitted`, and no reply
  obligation exists.
- `cancellation_after_submitted_before_write`: the settled obligation, the
  Kelpie-authored `cancellation` response message, its prompt operation,
  delivery, and request attempt are durable; the attempt and delivery are
  `submitted`; and no `agent.prompt` byte has been written.
- `cancellation_after_write_before_response`: the complete cancellation
  `agent.prompt` request has been written and flushed, while no response has
  been read or decoded.
- `cancellation_after_response_before_commit`: a structured `agent.prompt`
  acceptance response has been decoded, while the cancellation's prompt
  operation remains `pending` and its attempt and delivery remain `submitted`.
- `inbox_after_queued_before_write`: a socket-inbox delivery is durably
  `queued` and selected to offer, and no `inbox.delivery` byte has been written.
- `inbox_after_write_before_ack`: the complete `inbox.delivery` line has been
  written and flushed, while the delivery remains `queued` and no `inbox.ack`
  has been committed.
- `inbox_after_ack_before_resolve`: a parsed `inbox.ack` names that queued
  delivery, while the delivery remains `queued` and the obligation has not been
  resolved.
- `initial_message_after_submitted_before_write`: runtime start is independently
  `succeeded` and its incarnation is `ready`; the initial tell's separate
  message, prompt operation, delivery, and request attempt are durable; the
  operation is `pending`; the attempt and delivery are `submitted`; no reply
  obligation exists; and no `agent.prompt` byte has been written.
- `initial_message_after_write_before_response`: runtime start remains
  independently `succeeded`/`ready`; the complete initial-tell `agent.prompt`
  request has been written and flushed, while no response has been read or
  decoded.
- `initial_message_after_response_before_commit`: runtime start remains
  independently `succeeded`/`ready`; a structured `agent.prompt` acceptance for
  the initial tell has been decoded; its separate operation remains `pending`,
  its attempt and delivery remain `submitted`, and no reply obligation exists.
- `clear_after_submitted_before_write`: the standalone clear operation, its
  pre-clear session reference, and its request attempt are durable; the attempt
  is `submitted`; a Herdr connection exists; and no clear-command byte has been
  written.
- `clear_after_write_before_response`: the complete standalone clear
  `agent.prompt` request has been written and flushed, while no response has
  been read or decoded.
- `clear_after_response_before_commit`: a structured acceptance response for
  the standalone clear has been decoded, while its operation remains `pending`
  and its attempt remains `submitted`.
- `renew_after_intent_before_prepare`: the renew is durably `preparing` with its
  prepare ask, obligation, delivery, and deadline recorded, and no prepare
  envelope byte has been written. The delivery that follows passes through the
  ordinary ask points.
- `renew_after_ready_before_clear`: the prepare obligation is resolved, the
  pre-clear backend-native session reference has been probed and stored, the
  renew is `clearing`, its `clear` attempt is `submitted`, and no clear-command
  byte has been written.
- `renew_after_clear_before_inject`: the clear was accepted, a session reference
  differing from the stored pre-clear one has been observed, the `inject`
  attempt is `submitted`, and no resume-prompt byte has been written. This is
  the boundary that matters: past it the context is gone.
- `renew_after_inject_before_commit`: a structured `agent.prompt` acceptance for
  the resume prompt has been decoded, while its attempt remains `submitted`, the
  renew remains `clearing`, and the recorded observed native session has not yet
  been replaced.

The first process-kill test stops `kelpied` at the second point. On restart, a
fresh empty Herdr snapshot cannot prove the external effect, so Kelpie records
the start and incarnation as `unknown`. The restarted daemon sends only `ping`
and `session.snapshot`; it does not blindly resend `agent.start`.

The start post-write process-kill test additionally proves the fake Herdr
server parsed the complete original `agent.start` request and withheld its
response. A fresh empty snapshot cannot prove that request's runtime effect, so
restart records the start and incarnation `unknown` without replay.

The second process-kill boundary stops after the structured start response but
before its local commit. On restart, an exact authoritative Ready snapshot
reconciles the original operation to `succeeded` without replay. A snapshot
showing only the still-launching identity cannot prove the outcome and preserves
it as `unknown`, also without replay.

The ask pre-write process-kill test proves the prompt connection receives zero
bytes before `SIGKILL`. On restart, the fresh snapshot cannot prove terminal
input delivery, so the operation, attempt, and delivery become `unknown` without
resending `agent.prompt`. The durable final-reply obligation remains `open` and
is returned by the local client's `pending` method.

The ask post-write process-kill test proves fake Herdr parsed the complete
original prompt envelope and withheld its response. Restart cannot infer a
per-turn receipt from the snapshot, so operation, attempt, and delivery become
`unknown` without replay while the exact obligation remains `open` and visible.

The ask post-response process-kill test proves the fake Herdr server received
and accepted the original `agent.prompt`, then stops Kelpie before that
acceptance is committed locally. Herdr snapshots have no per-turn receipt, so
restart conservatively makes the operation, attempt, and delivery `unknown`,
does not replay the prompt, and keeps the obligation `open` and visible through
`pending`.

The tell pre-write process-kill test proves the prompt connection receives zero
bytes before `SIGKILL`. Restart makes the tell's operation, attempt, and
delivery `unknown` without replay. Direct durable-state checks before and after
recovery, plus an empty `pending` result, prove the tell never creates a reply
obligation.

The tell post-write process-kill test proves fake Herdr parsed the complete
original tell envelope and withheld its response. Restart makes operation,
attempt, and delivery `unknown` without replay, preserves the tell, and retains
zero obligations and an empty `pending` result.

The tell post-response process-kill test proves the fake Herdr server received
and accepted the original `agent.prompt`, then stops Kelpie before local
acceptance. Restart cannot infer a per-turn receipt from the snapshot, so it
makes operation, attempt, and delivery `unknown` without replay. The tell
message remains durable, its obligation count remains zero, and `pending`
remains empty.

The initial-tell pre-write process-kill test first drives runtime start through
raw acceptance and a later exact authoritative Ready snapshot. It then proves
zero initial-prompt bytes before `SIGKILL`. Restart preserves the terminal
runtime `succeeded`/`ready` state while making only the separate initial
delivery operation, attempt, and delivery `unknown` without replay. The initial
tell remains durable and its obligation count remains zero.

The initial-tell post-write process-kill test proves fake Herdr parsed the
complete original initial-tell envelope and withheld its response. Restart
preserves terminal runtime `succeeded`/`ready`, makes only the separate initial
operation, attempt, and delivery `unknown`, never replays the prompt, preserves
the initial tell, and retains zero obligations.

The initial-tell post-response process-kill test proves the fake Herdr server
received and accepted the original initial `agent.prompt`, then stops Kelpie
before local delivery acceptance. Restart preserves terminal runtime
`succeeded`/`ready`, makes only the separate initial operation, attempt, and
delivery `unknown`, never replays the prompt, preserves the initial tell, and
retains zero obligations.

The standalone-clear process-kill test covers all three write boundaries. Each
restart turns the attempted clear operation and its attempt `unknown` without
submitting another command, because a second clear could discard a conversation
created after the first one landed.

The renew pre-clear process-kill test proves the clear connection receives zero
bytes before `SIGKILL`. The pre-clear session reference and the `submitted`
clear attempt are both durable before the write, so a restart can tell that a
clear may already have escaped and must not send a second one. The context is
intact and no injection attempt exists.

The renew post-clear process-kill test seeds a renew already `clearing` against
a Herdr whose session reference has rotated, so the clear landed and the resume
prompt is owed. It stops Kelpie with the injection `submitted` and zero resume
bytes written — the worst durable state in the feature: cleared, not yet
re-seeded. Restart completes that renew rather than restarting it. Exactly one
prompt crosses, it is the resume envelope carrying `resumed cycle=1`, it does
not contain the clear command, and no second `clear` attempt is recorded. This
is the one place Kelpie retries a submitted prompt: a duplicate resume prompt
tells an agent its own instructions twice, while a missing one leaves an agent
cleared, idle, and instructionless, with nothing inside it that could notice.

The socket-inbox process-kill tests cover the same three boundaries on the
inbox write rather than a Herdr prompt. Persist queues the delivery and does
not resolve. Kill before write proves zero `inbox.delivery` bytes. Kill after
write proves the client parsed that line while the delivery stayed `queued`.
Kill after ACK proves the acknowledgement was read and the obligation stayed
open because resolve was not committed. Restart leaves the same queued row;
reconnecting as that waiter id drains it. That drain is the original attempt
completing, not a resend, and a second delivery row is never inserted.
Socket-inbox never records `unknown` for those kills: persist precedes every
inbox byte, a torn line has no newline, and the same queued row is what
reconnect offers.

The deterministic process-kill matrix covers all three explicit external-
effect boundaries for runtime start, ask prompt, tell prompt, initial-tell
prompt, standalone clear, and socket-inbox delivery: after durable submission
but before request write, after complete write and flush but before response
read, and after structured response decode but before local outcome commit.
Renew is covered at its two distinct external effects rather than at three
boundaries each, because its response boundary is the ordinary prompt boundary
and its recovery obligation is asymmetric: the clear must never be repeated and
the injection must never be abandoned. See `real-herdr-test.md` for the isolated
integration procedure.
