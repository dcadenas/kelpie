# Herdr v20 outcome mapping

Kelpie speaks Herdr's documented newline-delimited JSON protocol directly. The
recorded support policy is exactly protocol 20. Additive
unknown fields are ignored. Any other protocol returned by `ping` or
`session.snapshot`, including 19, is `incompatible_runtime` and no mutation is
sent. A Herdr package version is not a protocol number.

Kelpie maps observations as follows:

| Herdr/transport observation | Kelpie operation | Delivery |
| --- | --- | --- |
| Intent transaction committed; no attempt journaled | `pending` | `pending` |
| Attempt journal committed immediately before request write | `pending` | `submitted` for prompts |
| Matching raw `agent_started` response with exact pane and terminal | `accepted` | not applicable |
| Later authoritative `agent.get` or snapshot proves exact readiness | `succeeded` | not applicable |
| Matching `agent_prompted` response with exact pane and terminal | `succeeded` (delivery accepted, not task complete) | `accepted` |
| Herdr structured error before any accepted effect | `failed` | `rejected` or `target_unavailable` |
| Disconnect, timeout, malformed response, or crash after entering the write boundary | `unknown` | `unknown` |
| Fresh snapshot proves exact interrupted start is ready | `succeeded` | not applicable |
| Fresh snapshot cannot prove an attempted start | `unknown` | not applicable |
| Any attempted prompt found during recovery | `unknown` | `unknown` |

`submitted`, `accepted`, and `unknown` are never automatically resent, with one
recorded exception: a renew's resume prompt. Everywhere else the recipient may
already hold the message, so resending risks a duplicate against a recipient
that can notice one. An agent whose context was just cleared cannot: a duplicate
resume prompt repeats its own instructions, while a missing one leaves it idle
and instructionless forever. The clear itself keeps the ordinary rule and is
submitted at most once. The raw
`agent_started` response means only that Herdr accepted the launch and leaves the
incarnation `starting`. A later fresh snapshot or `agent.get` may prove readiness
only with exact terminal, pane, public name, backend kind, `interactive_ready`
true, and `launch_pending` false. It cannot prove whether terminal input was consumed, so
it cannot convert an interrupted prompt into success or failure.

Deterministic process-kill tests cover every documented external-effect
boundary for start, ask, tell, initial tell, and both of a renew's external
effects. Isolated integration tests exercise the supported public Herdr
transport and lifecycle.

## Renew

Clearing a context is not an outcome Herdr reports. `agent.prompt` acknowledges
the clear command the way it acknowledges any other text, so acceptance proves
submission and nothing more. Completion is proven only by observing that the
agent's backend-native session reference differs from the reference recorded
before the clear; `idle` cannot distinguish "not cleared yet" from "cleared",
and elapsed time proves nothing at all.

That rotation also makes the recorded observed native session false, so a
completed renew replaces it. It is the only operation permitted to overwrite
that write-once evidence, and it does so because the alternative is an
`attribution` record pointing at a transcript that will never grow again.

## Backend-native session references

Herdr reports the backend's own conversation reference for an agent. Kelpie
records it and uses it for attribution: it names the transcript an adapter reads
to learn which model actually served a turn.

It is not runtime identity and is not part of a binding. A live agent rotates it
on its own — Herdr's terminal state machine recognises `startup`, `clear`,
`resume`, `compact`, `new`, and `fork` as session-start sources, and four of
those are the same process continuing. Herdr knows which one occurred; protocol
20 does not expose that, and Kelpie reads snapshots rather than subscribing, so
a rotation and a replacement look identical from outside.

Kelpie therefore treats a changed reference as a stale record, not as absence.
When the pane, terminal, backend, and desired name still match, the incarnation
stays Ready and the recorded session observation is refreshed. A missing name
is repaired rather than treated as absence. `recover` reports changed references
as `native_sessions_refreshed`. A later runtime in the recorded seat can
continue the logical identity even when its backend changed.

## Retirement

Retirement is durable desired state, not pane cleanup. Recording intent moves
an exact Ready incarnation to Retiring without a Herdr mutation. A fresh
authoritative snapshot leaves it Retiring while the exact pane and terminal
binding remains live, and moves it to Retired only after that exact binding is
absent. A replacement terminal in the same pane is not attributed to the old
incarnation. Kelpie never closes a pane or deletes a working directory,
transcript, message, obligation, or operation as part of ordinary retirement.

Herdr v20 has no documented graceful `agent.stop` or equivalent. Such a
general-purpose lifecycle operation, distinct from destructive pane closure,
is a desired neutral upstream seam.
