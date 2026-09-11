# Socket waiters and protocol integration

Read when performing protocol operations beyond the entry procedure.

Contents: [Method details](#method-details), [typed client examples](#typed-client-examples).

## Method details

- `waiter.register`: create a pane-less LogicalAgent with socket-inbox delivery.
  No incarnation, no pane occupant. `waiter.retire` ends that targeting and
  releases the public name; it accepts the active waiter's name or
  `--logical-id`. Open or in-progress asks that waiter is waiting on
  are cancelled in the same step, reason `waiter retired`; the owing occupant
  is notified when addressable unless that ask never left the queue, and a later
  final is refused as not an open obligation rather than as an undeliverable
  waiter. The receipt names cancelled ask ids and whether each owing notice was
  delivered or only recorded. `--from operator` on `ask`
  is sender attribution only; `waiting_agent_id` is the waiter, and occupant
  `from=` is the waiter's public name. The host receives deliveries on a
  long-lived `inbox.claim` connection for that waiter id, then `inbox.ack`.
  `pending` and `ask.info` are not the socket-waiter receive path.
- `replies.claim`, `replies`, `replies.ack`: ask-scoped pull sink for reverse
  traffic when the ask used `reply_delivery=pull`. Not `inbox.claim`. `replies`
  is a non-destructive log for that ask only. Poll authorization is the waiting
  logical agent. A live lease is required to ACK a final. Do not document that
  as authentication.
- `notice.create` and `notice.list`: write and inspect durable operator notices.

Read `docs/client-protocol.md` and `SPEC.md` in the release for exact fields.
Responses contain the same request `id` and either `result` or a stable error.
Kelpie durable IDs are positive JSON numbers. Typed CLI ID arguments are positive
decimal integers; UUIDs and zero fail before the request is sent.



## Typed client examples


Ordinary use is typed commands. The CLI builds request IDs, idempotency keys,
and NDJSON internally. Default the socket to `$XDG_RUNTIME_DIR/kelpie/kelpie.sock`.
Read multiline or agent-generated bodies with `--stdin` or `--file` so the
shell never re-evaluates the text. `--body` is only for short trusted text.

```sh
kelpie --skill
kelpie --version
kelpie tell coordinator --stdin <<'EOF'
text containing backticks, $(), quotes, HTML, and newlines
EOF
kelpie tell coordinator --due-in 10m --stdin <<'EOF'
one-shot reminder; not cron
EOF
kelpie tell coordinator --every 15m --file supervision-pass.txt
kelpie schedule-cancel <schedule-id> --reason supervision-moved
kelpie schedules
kelpie ask kelpie-envelope-builder --file ./task.md
kelpie ask kelpie-envelope-builder --reply-delivery pull --file ./brief.md
kelpie replies-claim <ask-id>
kelpie replies <ask-id> --after 0 --timeout 30s
kelpie replies-ack <ask-id> <message-id> --lease 1
kelpie clear kelpie-envelope-builder
kelpie ask kelpie-envelope-builder --remind-after-ms 600000 --file ./long-task.md
kelpie ask kelpie-envelope-builder --no-remind --file ./parked-question.md
kelpie reply <ask-id> --progress --stdin <<'EOF'
working
EOF
kelpie pending
kelpie reminder-snooze <ask-id> --for 2h
kelpie reminder-interval <ask-id> --every 40m
kelpie reminder-disable <ask-id>
kelpie recover
kelpie who
kelpie who reviewer
kelpie report
kelpie report --live
kelpie rename reviewer --name divine-context-pr75-sj2
kelpie handoff --replace <incarnation-id> --logical-id <agent-id> \
  --name coordinator --pane w2:p1 --terminal term-9 --backend opencode \
  --cwd /new/checkout --timeout-ms 90000 --keep-open --parentless --tell --stdin
kelpie start --name worker --pane w1:p1 --terminal term-1 --backend grok \
  --cwd /tmp/work --timeout-ms 5000 --keep-open --parentless --tell --stdin
kelpie waiter-register --name inbox --parentless
kelpie ask worker --sender-id <waiter-id> --from operator --stdin
kelpie waiter-retire inbox
```


Raw NDJSON remains the advanced client protocol. Read `docs/client-protocol.md`
and `SPEC.md` from the matching release for exact fields.

```sh
kelpie "$KELPIE_SOCKET" < request.json
```
