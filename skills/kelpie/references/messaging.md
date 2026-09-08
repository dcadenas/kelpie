# Message outcomes and reply obligations

Read when performing messaging operations beyond the entry procedure.

Contents: [Method details](#method-details); [additional procedure](#ask-vs-tell).

## Method details

- `tell`: deliver a structured one-way message. Provide either exact
  `recipient` + `recipient_incarnation`, or `recipient_alias`. An alias resolves
  once to the unique active logical agent and then follows its fixed transport:
  a Herdr recipient binds its exact Ready incarnation; a socket waiter queues
  to its logical ID and becomes accepted only on inbox ACK. An optional due
  time persists the delivery as `queued` and offers it once due. `--every 15m`
  instead creates a wall-clock schedule bound to the resolved logical agent;
  it also accepts `--recipient-id ID` without an incarnation.
  Each firing resolves that agent's current incarnation or socket inbox and
  materializes a normal tell. If the target is unavailable, Kelpie records and
  reports the firing but creates no message or runtime; it never starts,
  revives, or restarts an agent. Missed intervals coalesce into one firing, and
  a new firing is skipped while an earlier schedule delivery is still pending,
  queued, or submitted. An unknown firing is never resent but does not stop later
  intervals.
  Prefer `--due-in 10m` or `--due-at 2026-08-12T20:00:00Z` over computing
  `--due-at-ms` yourself: a wrong epoch does not fail, it delivers at the wrong
  moment, while a bad duration or timestamp fails immediately. Keep returned
  message and delivery IDs.
- `ask`: same recipient shape as `tell`, delivered immediately; a due time is
  refused. Every ask
  creates a twenty-minute pending-reply reminder by default. Use
  `remind_after_ms` to override it or `no_remind: true` for the explicit
  exception. Becoming idle never bypasses the reminder due time. Keep the returned message
  ID; it identifies the durable reply obligation.
- `ask-info`: re-read an ask by message ID, including its original body,
  parties, obligation state, current delivery outcome, and every progress or
  final reply with its current delivery outcome.
- `reply`: provide `reply_to` (the ask message ID), `body`, `progress` or
  `final` disposition, and `idempotency_key`. Kelpie resolves the exact owing
  and waiting logical agents from the durable obligation and binds the waiter's
  receive path: a pane waiter's Ready incarnation through Herdr, or a socket
  waiter's inbox with no Herdr prompt. Persist is not acceptance. Only an
  accepted final reply resolves the obligation — Herdr prompt acceptance, or
  socket `inbox.ack`.
- `pending`: list the recipient's durable `open` and `in_progress` obligations
  in creation order. It does not infer task state from Herdr.
- `cancel`: provide `requester_agent_id`, `ask_message_id` (the message ID),
  and a non-empty reason. A queued tell or ask can be cancelled only before
  the first Herdr write. After submit, existing no-resend and unknown rules
  apply; open/in-progress ask obligations remain cancellable from any Ready
  pane. The waiter is not required; `--sender-id` of the waiter still works.
  A renew prepare ask is not this `cancel` — end the policy with
  `renew.cancel`.
  A socket waiter receives the Kelpie-authored cancellation on its inbox;
  the owing agent receives a stop-notice when addressable, recorded for
  `pending` when not; state is `cancelled`, not `resolved`.

## Ask vs tell

Choose the verb by whether you need a durable answer the other side can see.

**Sender**

- `tell` informs. Supply its body with exactly one of `--body`, `--stdin`, or
  `--file`; there is no implicit positional body. For a pane,
  `delivery=accepted` means the prompt was submitted. For a socket waiter it
  means the inbox client ACKed. It does not mean they will write back.
  `pending` will not list a tell.
- `ask` requests work or a decision. Keep the returned message id. The outcome
  you get back describes the channel — whether the ask reached them — never the
  answer.
- Need to know they finished or heard you? `ask`. Text they type only in their
  TUI never arrives here.
- Keep one idempotency key for one intended prompt. Repeating it after success
  returns the recorded receipt only for the same sender and reply correlation;
  repeating it after a proven terminal failure starts a fresh attempt. Pending,
  accepted, superseded, and unknown outcomes are refused with the prior outcome
  named. Never vary the key to bypass that refusal, because the original effect
  may already have landed.

**Never wait for a reply.** Send, read the delivery outcome, and end your turn.
The reply is pushed to you: when they answer, Kelpie delivers
`<kelpie from=… re=YOUR_ASK_ID final>` into your pane, which wakes you with the
answer already in hand. Nothing is lost while you are idle, and the obligation
survives restarts of you, them, Kelpie, and Herdr.

So do not block on `herdr agent wait`, do not sleep, and do not loop on
`pending` hoping an answer appears — `pending` lists what *you* owe, not what
you are owed. Herdr `idle` or `done` is not the answer either.

This holds however long the answer will take and whoever it has to come from. An
answer a person still has to give, relayed back to you by another agent, is the
case where waiting looks most defensible and costs the most: end the turn. Filling
the wait with side work you would not otherwise do now is the same mistake wearing
a useful face — you are still holding a turn open for a message that will arrive
without you.

It holds for a child you started, too. Waiting on a subordinate's final reply is
the same channel as waiting on a superior's answer, and a parent that sleeps
until its child reports has burned a cycle to learn something Kelpie would have
handed it.

**Ending a turn is not ending the agent.** Yielding leaves a Ready incarnation
that Kelpie can wake; parking, retiring, or closing the pane is what ends it, and
those are separate acts you have to perform. So an instruction to *stay up* while
someone owes you a reply — a policy several fleets impose on a parent mid-round —
means do not park, retire, or close: do not read it as a demand for a tool call
loop that keeps the turn open. A live idle incarnation satisfies it. `sleep`
satisfies nothing, and is strictly worse, because a delivery landing mid-`sleep`
sits unread until the command returns.

An ask is always delivered now. `--due-in`, `--due-at`, and `--due-at-ms` are
refused on `ask` and exist only on `tell`, because a postponed ask creates an
obligation the recipient cannot see: owed on the server, absent from their pane,
indistinguishable from an ask they simply have not answered. To be nudged about
an ask already in flight, use `--remind-after-ms`. To send something that should
arrive later, use `tell`. On a `tell`, `delivery=queued` means Kelpie is holding
the message, not that anyone received it; only `delivery=accepted` is dispatched.
`tell --every 15m` instead creates a repeating wall-clock schedule. Its receipt
names a schedule, not a delivered message. Each firing targets the logical
agent's current receive path; an unavailable firing reports and delivers
nothing, and Kelpie never starts or revives an agent for it. End it with
`schedule-cancel <schedule-id> --reason TEXT`.
Use `schedules` to recover schedule ids and inspect the latest firing outcome.
`kelpie --json schedules --sender-id ID` returns the stored tell `body` and
`idempotency_key` so a matching repeating tell can be recognized without
creating another one.

For work that must pause until a known future time, compose the existing verbs:
send `kelpie reply <ask-id> --progress` so the obligation visibly remains held,
then schedule `kelpie tell <your-own-name> --due-in ...` as the wake. When that
tell arrives, resume the work and eventually send the correlated final. Do not
invent a delayed reply: it would blur "still working" with "answer sent."

A client timeout while sending a reply is not evidence that delivery failed.
Do not immediately resend. On the next turn, run `kelpie pending`: disappearance
of the ask proves a final was accepted, while `in_progress` proves only that the
obligation remains open and may still be awaiting asynchronous socket ACK.
Inspect the durable outcome before retrying an ambiguous attempt.

Blocking is worse than merely slow: it is self-defeating. A reply is written
into your pane the moment it is sent, but a blocking command keeps you mid-turn,
so the reply sits unread in your input queue until that command returns. Wait ten
minutes for an answer that arrived in the first minute and you will still time
out, having held the answer the whole time.

**Receiver**

- `<kelpie from=alice>` with no `reply-to` is a tell. Do not `kelpie reply`.
  Do not `kelpie tell` an ack unless the body independently requires a new
  message. Answering only in the Grok or Codex pane does not notify the sender.
- `<kelpie from=alice reply-to=ID>` is an ask. You owe
  `kelpie reply ID --final` when done. Use a progress reply only when the
  task communication policy requires an acknowledgement or material update. That is the only reply the sender can correlate. Only the agent
  that owes the ask can reply to it — replying to an ask you asked will be
  refused. To push new information to another agent mid-task, `kelpie tell`
  them.
- `<kelpie-system cancellation waiting=… cancelled-ask=…>` is Kelpie's own
  notice that one of your asks was cancelled, with the reason. No reply is
  owed; re-ask whoever holds the name now if the question still matters.
- `<kelpie-system cancellation owing=… cancelled-ask=…>` is Kelpie's own
  notice that an ask you were answering was cancelled. Stop working on it.
  No reply is owed; it is not a new ask.
- `kelpie pending` lists asks you owe, then any of your asks cancelled while
  you had no Ready binding (state `cancelled`, with the reason), then any
  asks you were answering that were cancelled while you had no Ready binding.
  It does not list tells.
- A `<kelpie-reminder …>` for an ask you don't remember is the amnesia
  protocol: your context was replaced but the obligation is real, and the
  reminder carries the original question. `kelpie ask-info <ask-id>` re-reads
  the full ask any time. Answer it or cancel it — never ignore it.

