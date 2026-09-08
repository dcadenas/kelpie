# Start, adopt, recover, or retire an identity

Read when performing lifecycle operations beyond the entry procedure.

Contents: [Method details](#method-details); [addressing recovery](#addressing-recovery).

## Method details

- `start`: persist and launch one logical agent. Supply the exact Herdr session,
  pane, expected terminal, public name, backend, cwd, idempotency key, and initial
  message kind (`tell` or `ask`). Optional `logical_agent_id` continues that
  identity in a new incarnation. Read `runtime_start` and `initial_message`
  outcomes separately. `--ask` defaults its sender to you; `--sender-id` selects
  who waits for the answer, while `--parent-id` records lineage. For
  `conflict` / `pane_occupied`, select another pane. Kelpie retries a shell that
  is not ready yet.
- `handoff`: replace a Ready runtime while preserving logical identity, children,
  obligations, and messages. Use the `start` arguments plus `--replace
  INCARNATION-ID` and `--logical-id` for that predecessor's exact logical agent.
  The transaction proving the successor Ready marks the predecessor
  `superseded`. Herdr allows one live claim on a public name: first release the
  predecessor's name with `herdr agent rename <predecessor-pane> --clear`.
  This leaves its process running. A conflicting claim returns
  `agent_name_taken` and identifies the predecessor pane. Use a short
  `--timeout-ms` (15–20s) on a busy tree. Read the pane's actual cwd from Herdr
  and pass it verbatim to `--cwd`.
- `adopt`: bind an existing Herdr agent by exact `pane_id` and
  `expected_terminal_id`, without `agent.start`. Use `--logical-id` to continue
  its recorded identity. Snapshot evidence must show a matching agent with no
  pending launch; externally started occupants need no `interactive_ready`.
  Named occupants keep their Herdr name. For unnamed occupants, Kelpie records
  intent, claims the cwd basename (one pane suffix on collision, no `adopted-`
  prefix), and confirms
  the name before marking Ready. Optional `public_name` and `backend_kind`
  constrain the binding. Idempotent replay returns the same binding. Adoption
  is explicit, not a fleet-wide operation.

  `--arg`, `--requested-model`, `--requested-provider`, and `--requested-effort`
  preserve the caller's launch intent during recovery:

  ```sh
  kelpie adopt --pane w22:p5 --terminal term_x --logical-id <id> \
    --arg --dangerously-skip-permissions --arg --model --arg claude-opus-5 \
    --requested-model claude-opus-5
  ```

  Requested configuration is not observed evidence. When observation is needed,
  use `kelpie who --refresh`.
- `rename`: change a Ready agent's public name while preserving its incarnation,
  process, seat, cwd, lineage, and obligations. Use this operation for a name
  change; it rejects a name held by another Ready agent.
- `retire`: record desired retirement without sending to Herdr. Add
  `--close-pane` to end the process and release its pane after Kelpie re-proves
  the exact binding. Worktrees, transcripts, messages, obligations, and durable
  records remain. A pane held by another agent cannot be closed through this
  retirement.
- `recover`: reconcile durable records against a fresh Herdr snapshot. On a
  recorded pane and terminal, Kelpie repairs a missing name and confirms the
  projection; a different live name fails closed. A backend replacement ends
  the incarnation while permitting continuation of its logical agent on that
  seat. Native-session rotation on a Ready seat refreshes attribution only.

  After the recorded seat disappears, a unique match between the last native
  session of a continuable incarnation and a live occupant can restore that
  identity on the revived seat. The live name must match the alias or be absent.
  Ambiguous matches, different names, bare shells, socket waiters, and
  retiring/retired/superseded records cannot take this continuation route.
  Retirement completes only after exact seat absence. After binding, kelpied
  retries recovery for two minutes to catch later Herdr restoration. For a
  later restoration, run `kelpie recover`. Recovery never launches a backend.

## Addressing recovery

Caller identity defaults to the Ready binding for `$HERDR_PANE_ID`. Address a
recipient by its live name or by both `--recipient-id` and
`--recipient-incarnation`.

If the calling pane lacks a Ready binding, Kelpie lazily adopts its exact live
agent. A unique lost, unknown, declared, or failed incarnation on that pane and
terminal continues with its logical ID and recorded alias. An unnamed occupant
regains that alias. Several continuable identities or a different live name
fail closed; backend kind is runtime evidence rather than an identity condition.

A missing recipient alias can similarly continue a unique unnamed live agent
on its recorded seat. Cwd-derived adoption creates a new identity only when the
name has no prior claimant. Named occupants require explicit binding: a reused
Herdr name alone does not prove identity.

For `no ready agent for alias X`, inspect Herdr and Kelpie before choosing a
recovery operation:

```sh
herdr pane list
kelpie report --live
```

Match cwd, terminal, and Herdr agent name. If the matching agent is live, bind
its exact seat and recorded logical ID:

```sh
kelpie adopt --pane w7:p2B --terminal term_6592f21297a941 --logical-id <id>
```

Without `--logical-id`, adoption continues a unique recoverable identity already
recorded on the seat. It creates a new identity only when none exists; a new
identity inherits no other agent's history or obligations. If the prior owner
of the name has an `open` or `in_progress` obligation, bare adoption is refused.
Continue the named logical ID, or cancel the obligation with a reason.
