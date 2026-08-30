---
name: kelpie-diagnose
description: >
  Read kelpie's durable record to settle what actually happened to a message,
  delivery, renew, or start — with timestamps rather than guesses. Use when an
  agent says it never received a prompt, a renew looks stuck or wrongly failed,
  a delivery says accepted but nothing arrived, an obligation is still open, a
  notice needs explaining, or a peer reports a kelpie bug. Also use before
  accepting any claim about kelpie behaviour that the database can confirm or
  refute.
---

# Diagnosing kelpie from its durable record

Kelpie writes intent before every external effect and records the outcome, so
almost every question of the form "did X reach Y, and when" has an exact answer
in SQLite. Reports from agents are worth taking seriously and worth checking:
the useful reply is usually the same finding with a number attached.

Read only. Open the live database with `sqlite3 -readonly`, and never write to
it behind the running daemon's back — a phase edited by hand is a durable lie,
and the daemon owns those transitions.

The path comes from the unit, not from memory: `systemctl --user cat kelpied`
gives `--database`.

## Schema facts that cost time to rediscover

- Every timestamp is Unix epoch **milliseconds**. Render with
  `datetime(<col>/1000, 'unixepoch', 'localtime')`.
- `operation_attempts` has `started_at_ms` and `resolved_at_ms`. There is no
  `created_at_ms` on that table.
- `incarnations` has no readiness timestamp. Use the `start` row in `operations`:
  its `resolved_at_ms` is the moment readiness was proven.
- `obligations` carries `last_activity_at_ms`; once resolved, that is the moment
  the final reply was accepted.
- Attempt `request_id`s are namespaced and greppable: `kelpie:renew:clear:<id>`,
  `kelpie:renew:inject:<id>:<clock>`, `kelpie:initial:to:<operation-id>`.

## The question that answers most reports

"What reached this pane, and when, relative to what else." One incarnation's
whole external history:

```sql
select id, kind, outcome, created_at_ms, resolved_at_ms
from operations
where target_incarnation_id = '<incarnation-id>'
order by created_at_ms;
```

Adjacent rows are the finding. Two prompts into one pane milliseconds apart is
the failure mode this codebase keeps meeting: backends silently accept the pair
as one submission and lose the text. Worked examples, all real:

- a renew's authorising reply committed at `…515387`, its clear submitted at
  `…515392` — **5ms**, and the clear never landed
- a start proven ready at `…388232`, its brief submitted at `…388240` — **8ms**,
  and the brief never arrived
- a clear at `…138706`, a replacement conversation at `…144127` — **5,421ms**,
  which is a gap working as designed

Subtract, do not eyeball. The difference between a bug and correct behaviour here
is three orders of magnitude on numbers that look identical at a glance.

## `accepted` is not proof of receipt

A delivery is `accepted` because *Herdr* accepted the prompt request. Nothing in
that record proves the backend kept the text. So a report of "it says delivered
but I never got it" is not a contradiction to argue with — it is consistent with
the record, and the record cannot refute it. Look at what surrounded the write.

## When a proof fails, suspect the reporter

Kelpie proves a clear landed by watching the backend-native session reference
rotate. That proof reads a value Herdr reports, which is a different thing from
the value being true. Before concluding the subject misbehaved, compare all
three:

```sh
# 1. what Kelpie recorded
sqlite3 -readonly <database> \
  "select observed_native_session_json from incarnations where id = '<id>';"

# 2. what Herdr reports live
herdr agent get <pane-id>

# 3. what the backend itself believes (opencode)
sqlite3 -readonly ~/.local/share/opencode/opencode.db \
  "select id, time_created, time_updated from session
   where directory = '<cwd>' order by time_created desc limit 3;"
```

Two-way disagreement is ambiguous. Three-way disagreement names the liar. A real
case: Kelpie and Herdr agreed on a session id, opencode had created a new one
5.4 seconds after the clear and was still writing to it hours later — so the
clear had worked, and the stale report was Herdr's. The tempting conclusion from
the first two sources alone, that the backend had changed behaviour between
versions, was wrong.

If a backend's behaviour genuinely is in question, read the shipped binary rather
than its docs or its changelog:

```sh
rg -a -o --no-filename '.{250}"session\.new",title:"New session".{250}' <binary>
```

## Renews

```sql
select id, phase, cycle, every_ms, scheduled_at_ms, clear_deadline_ms,
       inject_not_before_ms, pre_clear_session_json, termination_reason
from renews order by created_at_ms desc limit 5;
```

`requester_agent_id = logical_agent_id` means the agent renews itself, so it is
its own waiter and the prepare's final reply is delivered into the pane about to
be cleared. That is not a curiosity; it is where the prompts crowd together.

`termination_reason` says why a cycle ended. A policy's successor is a separate
row with `cycle` incremented — a policy that ended without one is a supervision
chain that stopped, which is worth saying out loud.

## Obligations and notices

An open obligation is not evidence of a lost message. Check whether the owing
agent is alive and busy before treating it as one:

```sql
select l.public_name, o.state, o.created_at_ms, o.last_activity_at_ms
from obligations o
join messages m on m.id = o.ask_message_id
join deliveries d on d.message_id = m.id
join incarnations i on i.id = d.recipient_incarnation_id
join logical_agents l on l.id = i.logical_agent_id
where o.state = 'open' and i.state = 'ready'
order by o.created_at_ms desc limit 15;
```

`last_activity_at_ms == created_at_ms` means no progress reply has ever been
sent, which is suggestive and not conclusive. Cross-check with `kelpie report
--live`: an agent that is `herdr=working` and has spawned children of its own got
its brief.

Notices are the operator-facing record and read well in a report:

```sql
select substr(body, 1, 200) from operator_notices order by created_at_ms desc limit 5;
```

## Reporting what you find

Quote the timestamps. "Five milliseconds" ends an argument that "too quickly"
starts. Say plainly which claims the record settles and which it cannot — a
delivery marked `accepted` that a backend dropped leaves no trace here, and
saying so is more useful than an inference dressed as a finding.
