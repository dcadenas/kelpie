# Schedules, reminders, and context renewal

Read when performing scheduling operations beyond the entry procedure.

Contents: [Method details](#method-details); [additional procedure](#writing-renew-prompts).

## Method details

- `schedule-cancel <schedule-id> --reason TEXT`: end a repeating tell schedule.
  Only its requester or target may cancel it.
- `schedules [alias]`: list schedules requested by or targeting that logical
  agent, including ended schedules and the latest firing outcome.
  `--json` also returns stored `requester_agent_id`. Tell rows include the
  exact `body` and `idempotency_key`. Renew rows leave those two fields
  `null`; they are not the resume prompt. Listing does not create a schedule.
- `clear`: replace one Ready agent's backend-native conversation without a
  prepare ask or resume prompt. Same recipient shape as `tell`. Verified
  on-clear backends (`claude`, `codex`, `grok`, `pi`) return only after Herdr
  exposes a different session reference. `opencode` returns after `/clear` is
  accepted because its next prompt allocates the replacement conversation and
  waiting first would deadlock. The caller stays connected during an on-clear
  wait, but the daemon continues serving the fleet. Clear waits out the backend
  settle gap after a preceding prompt and before the first following prompt,
  even when the clear outcome is unknown or that next prompt was scheduled
  earlier. It queues all prompt deliveries (including replies) while awaiting
  rotation, persists their post-clear deadline across recovery, and conflicts
  with an in-flight renew cycle.
  Unknown kinds fail closed; no command is guessed. An ambiguous submitted
  clear is never resent automatically.
- `reminder-snooze`: only the owing receiver can postpone an open ask. Use
  `--for 2h` or `--until-ms MS`, never both. Relative durations accept positive
  integers in `s`, `m`, `h`, or `d`; the daemon resolves the deadline.
- `reminder-interval`: `--every 40m` increases an owned ask's stored interval;
  decreases are rejected. An increase preserves later deadlines and snoozes.
  Progress replies preserve snoozes; expiry resumes normal reminders. Receipts
  and `ask-info` expose effective timing under `reminder`. Snoozing cannot retract
  a reminder already submitted. Disabled policies stay disabled.
- `renew`: bound one agent's context by clearing it and re-seeding it. Its
  recurrence uses the shared schedule ledger with an overlap guard while
  retaining renew's active-occupancy clock and exact incarnation binding. With no
  recipient it arms on the caller; it accepts no alias, only `--recipient-id`
  with `--recipient-incarnation` for a deliberate cross-target. Two phases: the
  `--prepare-prompt` is delivered as
  an ask ("save your progress to progress.md"), and only its accepted FINAL
  REPLY authorises the clear. Then Kelpie sends the backend's clear command,
  waits until the backend-native session reference actually changes, and injects
  `--prompt`. Nothing is inferred from elapsed time or idle state.
  `--on-timeout abort|proceed` is REQUIRED and has no default: `abort` leaves
  the agent untouched when it never confirms (its context keeps growing);
  `proceed` clears regardless (unsaved work is lost). A prepare timeout raises an
  operator notice either way and never disarms a policy.
  `--due-in`/`--due-at` renew once; `--every 45m` re-arms after every cycle and
  ends only when the incarnation stops being Ready. `--every` accumulates only
  while Herdr observes the incarnation as `working` or `blocked`; `idle` and
  `done` do not advance `next-in`. A policy's first cycle is one interval of
  that active time away, so arming one does not clear you on the spot. A cycle
  already preparing or clearing is not paused because the agent went idle. Every
  other ending re-arms — skipped, aborted, or abandoned unproven — so a policy never
  stops quietly while the agent believes it is still supervised. Only backends with a
  verified clear protocol are accepted — `claude`, `codex`, `opencode`, `grok`,
  and `pi`; anything else fails closed as `incompatible_runtime` with code
  `renew_unsupported_backend`, before any durable intent. `opencode` allocates
  its replacement conversation on the next prompt rather than on the clear, so
  there the resume prompt is sent first and the rotation is required afterwards;
  the proof is the same, its position is not. It is also the one backend where a
  failed clear puts the resume prompt into the context it was meant to replace:
  if you receive `<kelpie-renew ... resumed>` and the conversation before it is
  still there, the clear did NOT land — say so instead of re-reading your
  checkpoint and re-planning work you can still see. A clear the backend never confirms
  raises one operator notice and never completes the renew; the injection is
  never abandoned, because the context is already gone. Long after that notice
  the cycle is abandoned and the next one armed, rather than left running
  forever on a proof that is not coming. Messages addressed to an agent mid-renew are held and
  delivered after it is resumed, never into the context being discarded.
  Obligations survive a renew: they live in Kelpie, not in a context window.
  A policy ends when its incarnation stops being Ready, and only then. Being
  adopted back afterwards restores addressing, not the policy, so an agent can
  keep working with nothing bounding its context. That termination raises an
  operator notice naming the agent, the incarnation, and the renew, and
  `kelpie report` shows a live agent's armed cycle as
  `renew=scheduled cycle=97 every=45m0s next-in=15m0s`. No renew on a long-lived
  root means no policy is armed. Re-arming is a decision for whoever owns that
  agent; Kelpie will not do it, and `renew` still refuses a second policy on an
  incarnation that already has one. That refusal is per incarnation, which is
  why arming one on yourself to see whether you are already supervised is safe
  and arming one on somebody else is not.
- `renew-cancel <renew-id> --reason TEXT`: end a policy before its incarnation
  does. Only its requester or its target may cancel, so nobody can quietly
  disarm another agent's supervision. Refused while a cycle is mid-clear — the
  context is already gone and only the resume prompt brings it back — so wait
  for that cycle to finish and cancel then. A cancel raises an operator notice
  naming the policy, the target, whoever ended it, and the reason.

## Writing renew prompts

A renew has three layers, and putting an instruction in the wrong one is the
main way renewals go wrong.

| Layer | Runs | Holds |
| --- | --- | --- |
| Start prompt (`start --tell/--ask`) | Once, ever | One-time bootstrap: clone the repo, create the branch, install deps |
| Standing resume prompt (`renew --prompt`) | Every cycle, forever | Invariants only: who you are, where things live, how to work |
| Checkpoint file (written by the prepare) | Rewritten each cycle | Non-recoverable intent, decisions, active safety/resource grants, and source pointers |

With `--every`, the resume prompt is a program that runs forever. It MUST be
reentrant. Anything destructive, one-time, or order-dependent belongs in the
start prompt or the checkpoint, never in the standing prompt. "Create the
branch" creates it once and fails every cycle after. "Reset the scratch
directory" silently destroys the previous cycle's work. "Continue where we left
off in the migration" is stale on cycle two. The resume envelope carries
`cycle=N`, so a resumed agent can see whether this is the first run.

Prefer a standing prompt that only points at files:

```text
prepare.txt: Replace progress.md with the continuation a fresh reader cannot
             recover from live sources: unresolved intent, decisions and why,
             active safety/resource grants, and absolute source pointers.
             Replace superseded notes; do not copy live inventories or history.
resume.txt:  Read instructions.md for how to work, then continue the pending
             work in progress.md.
```

```sh
kelpie renew \
  --prepare-prompt-file prepare.txt \
  --prompt-file resume.txt \
  --on-timeout abort --every 45m
```

With no recipient that arms the policy on YOU, which is almost always what you
want. `renew` is the one verb that takes no live name: a name can belong to
another agent by the time it resolves, and a policy aimed at the wrong agent
clears its conversation once a cycle. To bound somebody else's context on
purpose, name it exactly with `--recipient-id` and `--recipient-incarnation`.

Both prompts are read once, when the renew is created, and stored durably. A
policy does not re-read those files, so editing `resume.txt` later changes
nothing; the standing prompt keeps whatever text it was armed with. That is also
why the standing prompt should point at files the AGENT reads at run time
(`instructions.md`, `progress.md`) — those are the parts you can still change.

The checkpoint's only reader is you with an empty context holding nothing but
the resume prompt. Write it for that reader: absolute paths, no "the approach we
discussed", no "the second option", decisions recorded with their reasoning
rather than just their conclusions. The prepare envelope quotes the resume
prompt so you can check your checkpoint actually satisfies it.

Keep only continuation facts that live sources cannot reconstruct. Preserve the
scope, owner, and release condition of active safety/resource grants, with their
evidence pointers. Recover fleet membership, task status, and obligations from
their current owners after resume. Replace superseded notes instead of appending
another account of the same work. The checkpoint is a current continuation,
not a transcript or a second inventory.

## Receiver envelopes

Prompt text delivered into an agent uses compact HTML-like envelopes. The
machine client protocol remains NDJSON. Bodies escape `<`, `>`, and `&`.

```text
<kelpie from=alice>
BODY
</kelpie>

<kelpie from=alice reply-to=<message-id>>
BODY
</kelpie>

<kelpie from=bob re=<ask-message-id> progress>
BODY
</kelpie>

<kelpie from=bob re=<ask-message-id> final>
BODY
</kelpie>

<kelpie-renew from=alice reply-to=<ask-message-id> prepare cycle=N deadline-ms=MS>
BODY
</kelpie-renew>

<kelpie-renew from=alice resumed cycle=N checkpointed-at-ms=MS>
BODY
</kelpie-renew>
```

- Omit `to`, `kind`, body wrappers, and tell IDs. The receiver already knows it
  is the target; tells create no reply obligation.
- `reply-to` and `re` carry the durable message handle.
- Bare `progress` and `final` are boolean flags.
- `from` names the reply target. `from=operator` is the user with no agent in
  between.
- `<kelpie-renew ... prepare>` means your context is about to be cleared. It is
  an ask: write your checkpoint, then `kelpie reply ID --final`. It quotes the
  exact prompt you will receive after the clear inside `&lt;resume&gt;` tags —
  that is a preview so you can make the checkpoint sufficient, NOT an
  instruction to follow now. Following it now skips the checkpoint entirely.
- `<kelpie-renew ... resumed>` means your context was just cleared and you are
  continuing work a previous instance of you wrote down. Do not start over and
  do not assume any conversation preceded it. `cycle=N` tells you how many times
  this has already happened.
- Envelopes arrive in the same role as a human's own messages, and nothing else
  in the conversation tells them apart. A turn is the human only when it carries
  no envelope. Every `<kelpie ...>` and `<kelpie-renew ...>` turn is another
  agent, however conversational its body reads.
- Answer an envelope with what the envelope cannot already contain: what you did
  after reading it, what state changed, what you now understand. A human sharing
  this pane is probably not present, so prose written to inform them inside an
  envelope reply is lost.
- When a turn without an envelope arrives, treat the human as having read none of
  the envelopes and none of your replies to them. Answer what they asked, and
  state inline whatever that answer depends on that arrived while they were away.
  Do not summarize the gap; they did not ask what happened, and a recap buries
  the answer.

To answer an ask, reply with that ask's message ID only:

```sh
kelpie reply <ask-message-id> --final --stdin <<'EOF'
done
EOF
```

A final reply resolves the obligation only when delivery is accepted. Rejected
or unknown final deliveries leave the obligation open so you can send another
final after reconciling; never resend an ambiguous submitted attempt.
