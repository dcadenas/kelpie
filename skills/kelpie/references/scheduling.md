# Schedules, reminders, and context renewal

Read when performing scheduling operations beyond the entry procedure.

Contents: [Method details](#method-details); [writing renew prompts](#writing-renew-prompts);
[receiver envelopes](#receiver-envelopes).

## Method details

- `schedule-cancel <schedule-id> --reason TEXT`: end a repeating tell schedule.
  Only its requester or target can cancel it.
- `schedules [alias]`: list schedules requested by or targeting that logical
  agent, including ended schedules and latest firing outcomes. `--json` exposes
  `requester_agent_id`. Tell rows include stored `body` and `idempotency_key`;
  renew rows leave these null. Listing creates no schedule.
- `clear`: replace a Ready agent's native conversation without preparation or
  a resume prompt. Use the same recipient form as `tell`. For `claude`, `codex`,
  `grok`, and `pi`, completion requires a changed native-session reference.
  For `opencode`, completion follows accepted `/clear`; the next prompt
  allocates the replacement conversation. Unsupported backends fail closed.

  The caller waits through rotation while the daemon serves other requests.
  Clear respects backend settle gaps around prompts. While awaiting rotation,
  all prompt deliveries, including replies, are queued. The post-clear deadline
  survives recovery and applies after unknown clear outcomes and to previously
  scheduled prompts. Clear conflicts with an in-flight renew. An ambiguous
  submitted clear is never automatically resent.
- `reminder-snooze`: the owing receiver can postpone an open ask with `--for 2h`
  or `--until-ms MS`, never both. Relative durations are positive integers in
  `s`, `m`, `h`, or `d`; the daemon resolves their deadlines.
- `reminder-interval`: `--every 40m` increases an owned ask's stored interval;
  decreases are rejected. Increases preserve later deadlines and snoozes.
  Progress replies preserve snoozes; expiry resumes the stored reminder cadence.
  Mutation receipts and `ask-info` expose effective timing under `reminder`.
  Snoozing cannot retract a submitted reminder. Disabled policies stay disabled.
- `renew`: clear and re-seed one agent's context. With no recipient, it targets
  the caller. For another agent, supply both `--recipient-id` and
  `--recipient-incarnation`; aliases are not accepted. The recurrence uses the
  shared schedule ledger with an overlap guard and exact incarnation binding.

  First Kelpie sends `--prepare-prompt` as an ask. Its accepted final reply
  authorizes the clear. Elapsed time and idle state do not confirm preparation.
  Required `--on-timeout abort|proceed` determines an unconfirmed timeout:
  `abort` leaves the context intact; `proceed` clears despite unsaved work.
  Either timeout raises an operator notice without disarming the policy.

  Kelpie clears, proves native-session rotation, and injects `--prompt`.
  `opencode` requires injection before rotation proof because its next prompt
  creates the conversation. Supported backends are `claude`, `codex`,
  `opencode`, `grok`, and `pi`; others fail before durable intent with
  `incompatible_runtime` / `renew_unsupported_backend`.

  An unconfirmed clear raises one operator notice and cannot complete the cycle.
  Resume injection is retried until accepted because a cleared agent needs its
  continuation. After the proof deadline, an unproven cycle is abandoned and
  the next cycle armed. Messages addressed mid-renew wait until resume.
  Obligations survive the context replacement.

  `--due-in` or `--due-at` creates a single renewal. `--every 45m` counts only
  Herdr-observed `working` or `blocked` time; `idle` and `done` do not advance
  `next-in`. The first cycle needs one full active interval. A cycle already
  preparing or clearing continues when the agent becomes idle. Completed,
  skipped, aborted, and abandoned cycles re-arm the recurring policy.

  The policy ends when its incarnation stops being Ready or an authorized
  cancellation ends it. Incarnation termination raises an operator notice naming
  agent, incarnation, and renew. Adoption restores addressing, not the policy.
  The agent's owner decides whether to re-arm. A second policy on the same
  incarnation is refused. `kelpie report` shows an armed cycle as
  `renew=scheduled cycle=97 every=45m0s next-in=15m0s`; no renew field means
  no armed policy.
- `renew-cancel <renew-id> --reason TEXT`: only the requester or target can end
  the policy. Cancellation is refused mid-clear; wait for the cycle to finish
  so its resume prompt can restore context. Cancellation raises an operator
  notice naming the policy, target, requester, and reason.

## Writing renew prompts

Keep the three prompt layers separate:

| Layer | Runs | Holds |
| --- | --- | --- |
| Start prompt (`start --tell/--ask`) | Once | One-time bootstrap |
| Standing resume prompt (`renew --prompt`) | Each cycle | Identity, instruction paths, and continuation route |
| Checkpoint file | Replaced by each prepare | Intent, decisions, active safety/resource grants, and source pointers unavailable from live sources |

Make the standing prompt reentrant. Put destructive, one-time, and order-dependent
work in the start prompt or checkpoint. `cycle=N` identifies the renewal cycle.

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

Kelpie reads and stores both prompts at policy creation. Later file edits do
not change the stored prompts. Point the resume prompt at files the agent reads
at runtime for instructions that need to evolve.

Write the checkpoint for a reader with only the resume prompt: use absolute
paths, explicit decisions and reasons, and each active grant's scope, owner,
release condition, and evidence pointer. Replace superseded notes. Recover fleet
membership, task status, and obligations from their live owners after resume.
The prepare envelope quotes the stored resume prompt for checking checkpoint
sufficiency.

## Receiver envelopes

Agent prompts use HTML-like envelopes; the machine protocol remains NDJSON.
Bodies escape `<`, `>`, and `&`.

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

- Envelopes omit `to`, `kind`, body wrappers, and tell IDs. `reply-to` and `re`
  carry durable message IDs; `progress` and `final` are boolean flags.
- `from` names the reply target. `from=operator` identifies the user directly.
  Other enveloped turns are agent messages even though they use the human role.
- On `<kelpie-renew ... prepare>`, write the checkpoint, then send
  `kelpie reply ID --final`. The escaped `&lt;resume&gt;` text previews the next
  prompt; use it to check the checkpoint, not to resume work before saving.
- On `<kelpie-renew ... resumed>`, follow the stored continuation. If the prior
  conversation remains visible, report that clear did not land instead of
  re-reading and re-planning visible work. Otherwise treat the context as fresh.
- Replies report the result or changed state needed by the sender. For a human
  question outside an envelope, answer directly with any necessary facts from
  agent exchanges; assume the human has not read those exchanges.

Answer an ask using its message ID:

```sh
kelpie reply <ask-message-id> --final --stdin <<'EOF'
done
EOF
```

A final resolves only on accepted delivery. For rejected or unknown delivery,
reconcile the durable outcome before another final; never resend an ambiguous
submitted attempt.
