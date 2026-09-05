# Adoption and live Herdr watching

## Explicit adopt

`adopt` is the primitive for binding durable Kelpie identity to a pre-existing
Herdr agent. It never calls `agent.start`. Authority remains:

1. Fresh `session.snapshot` for present-state facts.
2. Durable store for logical identity, incarnation, and adopt intent.

Plugin hooks and event streams are not required for correctness of a single
adopt call.

Herdr `interactive_ready` is not an adopt requirement. That flag means only
that a managed `agent.start` reached Active. Occupants started outside Kelpie
(idle Codex, omitted `interactive_ready`) are adoptable when the exact pane
and terminal match, a backend kind is observed, and `launch_pending` is false.

Every Ready Kelpie alias MUST equal the live Herdr agent name. Named
occupants are adopted under that exact name. Unnamed occupants persist adopt
intent, claim a cwd-basename name through Herdr `agent.rename` (one
pane-derived suffix if that basename is taken; never `adopted-`), and become
Ready only after a fresh snapshot shows the claimed name. A later missing name
is projection drift: recovery restores it when the recorded seat still matches.

Kelpie also performs targeted lazy adoption. A command that needs the calling
pane's identity adopts that exact live occupant when no Ready binding exists.
If that pane and terminal already record a unique lost, unknown,
declared, or failed incarnation, the adoption continues that logical agent and
keeps its recorded alias. Several continuable agents, a live-name mismatch, or
a starting occupant fail closed. Backend changes do not change logical identity.
An alias-addressed tell or ask may continue one unique unnamed live agent on a
recorded seat. It may use a working-directory basename to create an identity
only when the alias has no prior claimant. Ambiguous matches fail closed. This
does not scan or silently adopt the rest of the Herdr fleet.

## Why not only Herdr plugin hooks

Herdr plugin v1 hooks are one-shot commands, not supervised daemons. They can
notify that something happened, but they cannot own durable identity or survive
as the reconciliation authority. A thin hook may later *wake* a watcher; it
must not *be* the watcher.

## Event-driven watcher constraints

Herdr supports:

- `session.snapshot` — authoritative baseline after connect/reconnect.
- `events.subscribe` including `pane.agent_detected` (and release) and pane
  status updates — low-latency hints only.

A correct long-lived adopter would:

1. Connect to the documented Herdr socket.
2. `ping` / negotiate protocol.
3. Install a snapshot baseline.
4. Subscribe to agent/pane events.
5. On any gap, reconnect, discard cached present-state assumptions, and
   re-snapshot before applying policy.
6. Never auto-create logical agents unless policy is enabled; default remains
   explicit `adopt` only.
7. Apply delayed events only to the exact incarnation IDs recorded at adopt
   time so replacements cannot absorb old mutations.

Event subscription, fleet auto-adoption, and plugin packaging are optional
extensions. Explicit adoption and recovery do not depend on them.
