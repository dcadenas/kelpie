# Start, adopt, recover, or retire an identity

Read when performing lifecycle operations beyond the entry procedure.

Contents: [Method details](#method-details); [additional procedure](#addressing-recovery).

## Method details

- `start`: persist and launch one logical agent. Include the exact Herdr
  session, pane, expected terminal, public name, backend kind, working
  directory, idempotency key, and explicit initial message kind (`tell` or
  `ask`). Optional `logical_agent_id` continues that exact logical agent in a
  new incarnation. Read separate `runtime_start` and `initial_message` outcomes.
  `--ask` defaults its sender to you; pass `--sender-id` only when a different
  agent waits for the answer. That is not `--parent-id`, which is lineage.
  A pane that already hosts an agent fails closed as `conflict` with code
  `pane_occupied` — do not retry it, find another pane. A pane whose shell is
  not up yet is retried for you.
- `handoff`: replace a running agent's RUNTIME while keeping its identity and
  its whole child tree. Same arguments as `start`, plus `--replace
  INCARNATION-ID` naming the incarnation being taken over from, and
  `--logical-id` naming the agent being continued. Use it to move an agent to a
  new working directory, backend, or pane without becoming a different agent:
  children keep pointing at the same parent, open obligations still resolve, and
  message history is continuous. The predecessor is demoted to `superseded` in
  the same transaction that proves the successor Ready, so there is never a
  moment with two ready incarnations of one agent (which makes alias resolution
  and reply correlation ambiguous) or none (which makes the agent
  unaddressable). Refused when the predecessor is not a ready incarnation of
  that exact logical agent. Starting a NEW logical agent instead would strand
  every child on a parent id nobody answers to.
  PREREQUISITE: Herdr binds a public name to a pane and refuses a second live
  claim on it, so while the predecessor keeps running it still holds the name and
  Herdr rejects the successor with `agent_name_taken`. Release the name first,
  which does not stop the process: `herdr agent rename <predecessor-pane>
  --clear`. Kelpie names your own predecessor and that pane in the error when it
  happens. Handoff is also run on a busy tree by definition, and a start holds
  the daemon for its readiness wait, so prefer a short `--timeout-ms` (15-20s;
  opencode reaches ready in about 3.6s) over the 90s a fresh start can afford.
  `--cwd` is compared to the pane's actual cwd exactly, so read it back from
  Herdr and pass it verbatim rather than the path you meant.
- `adopt`: bind an already-running Herdr agent (exact `pane_id` +
  `expected_terminal_id`) without `agent.start`. Pass `--logical-id` to continue
  an existing logical agent in a new incarnation, keeping its history,
  obligations, and messages. Without it you create a NEW logical agent that
  merely reuses the public name and inherits none of that, so an agent that is
  alive in Herdr but unaddressable through Kelpie is recovered with
  `kelpie adopt --pane ID --terminal ID --logical-id <id>`, never by starting a
  replacement. Snapshot is authoritative;
  fail closed if missing, launch-pending, or mismatched. Occupants started
  outside Kelpie (idle Codex with no `interactive_ready`) are valid. Named
  occupants keep their Herdr name. Unnamed occupants persist intent, claim a
  cwd-basename Herdr name through `agent.rename` (one pane suffix on
  collision; never `adopted-`), and become Ready only after a confirming
  snapshot. Optional `public_name` and `backend_kind` constraints; optional
  `logical_agent_id` continues that identity. Idempotent replay returns the
  same binding. Explicit only — no silent auto-adopt of every Herdr agent.
  `--arg`, `--requested-model`, `--requested-provider`, and `--requested-effort`
  record the configuration the caller believes the runtime was launched with, so
  a start that ended `unknown` can be recovered without losing what it requested:

  ```sh
  kelpie adopt --pane w22:p5 --terminal term_x --logical-id <id> \
    --arg --dangerously-skip-permissions --arg --model --arg claude-opus-5 \
    --requested-model claude-opus-5
  ```

  These are a claim about intent, never evidence. Adoption observes a runtime
  Kelpie did not start, so requested configuration is never reported as observed;
  use `kelpie who --refresh` for what actually served the turn.
- `rename`: move a Ready agent to a new public name in one step. Keeps the same
  incarnation, process, pane, terminal, cwd, lineage, and obligations, and adds
  no incarnation. Use this instead of renaming in Herdr and re-adopting; that
  sequence leaves the agent unreachable if it stops halfway and records a binding
  attempt that never happened. Fails closed on a name another Ready agent holds.
- `retire`: record desired retirement for an incarnation. On its own it sends
  nothing to Herdr and leaves the pane occupied. Add `--close-pane` to release
  the pane in the same step; it ends that process but keeps the worktree,
  transcripts, messages, obligations, and durable records. Kelpie re-proves the
  exact binding first and refuses to close a pane another agent now holds.
- `recover`: obtain a fresh Herdr snapshot and reconcile durable records. A
  missing name on the recorded pane and terminal is projection drift: Kelpie
  records repair intent, restores the desired name, and confirms it. A present
  different name fails closed. A backend replacement ends the current
  incarnation but does not prevent a new incarnation from continuing the same
  logical agent on that seat. Native sessions are attribution while the seat
  is still Ready; a rotation there refreshes the record and is not
  continuation. After the recorded seat is gone, a unique match between a
  continuable incarnation's last native session id and a live occupant
  continues that logical agent onto the revived pane and terminal when the
  live name equals the alias or the occupant is unnamed. Ambiguous matches,
  a different live name, a bare shell, a socket waiter, and
  retiring/retired/superseded rows fail closed. That is restoration of an
  identity Kelpie already bound, not fleet auto-adoption. Exact seat absence
  is required to complete retirement. After bind, kelpied retries this recover
  for two minutes so Herdr-restored agents that appear after a client attaches
  can still unique-continue. If restore lands later, run `kelpie recover`.
  kelpied never launches the backend.

## Addressing recovery

Caller identity defaults to the Ready binding for `$HERDR_PANE_ID`. Use exactly
one recipient form: a live name, or both `--recipient-id` and
`--recipient-incarnation`. Exact addressing does not take a fake alias.
When the calling pane has no Ready binding, Kelpie lazily adopts its exact live
agent. If that pane and terminal already have a unique lost, unknown,
declared, or failed incarnation, the adoption continues that logical agent and
keeps its recorded alias. An unnamed occupant is renamed back to that alias. A
different live name or several continuable agents fail closed. Backend kind is
runtime evidence, not an identity precondition. A missing recipient alias may
likewise continue one unique unnamed live agent on a previously recorded seat;
cwd-derived adoption creates a new identity only when the name has no prior
claimant. Ambiguity fails closed.

`no ready agent for alias X` means Kelpie has no Ready binding under that name.
It does not mean the agent is gone, and it is not grounds for starting a
replacement. Lazy adoption deliberately skips a live agent that already *has* a
Herdr name: Kelpie names what it binds, so an unnamed occupant is provably
unclaimed, while a named one may be a later runtime wearing a name an older
incarnation left behind. Names are reusable aliases, never primary keys, so
Kelpie will not infer identity from one. Look before you conclude absence:

```sh
herdr pane list
kelpie report --live
```

Match on cwd, terminal, and Herdr agent name; `report --live` shows what Kelpie
has bound, checked against Herdr now.

If Herdr shows a live agent that should answer to the alias, bind it explicitly
with its exact pane and terminal — and check for a recorded logical id first:

```sh
kelpie adopt --pane w7:p2B --terminal term_6592f21297a941 --logical-id <id>
```

Omitting `--logical-id` continues a unique recoverable logical agent already
recorded on that pane and terminal. It mints a NEW logical agent only when the
seat has no recoverable identity; that new identity inherits none of another
agent's history, obligations, or messages.

Kelpie refuses that bare adopt outright when a prior agent under the same name
has an `open` or `in_progress` obligation, naming the id to continue. A dead pane
does not settle a debt — obligations belong to the logical agent and outlive
every runtime it had — so the two ways past the refusal are continuing that agent
with `--logical-id`, or cancelling the obligation and saying why. Reusing a name
whose prior owner has nothing outstanding is unaffected.
