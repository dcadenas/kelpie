# Renew across backends

Renew is only offered for backends whose clear Kelpie can *prove*. `/clear`
for `claude`, `codex`, and `opencode`; `/new` for `grok` and `pi`. Every other
backend is refused as `incompatible_runtime` before any durable intent, because
guessing a clear command that is not one is a context destroyed for nothing.
`/clear` is a near-convention rather than a convention: pi ships a full slash
command list with no `/clear` in it, so each entry is read from what that
backend ships and none is inferred from the others.

The command is only half of an entry; the other half is when the replacement
conversation becomes observable. Most backends rotate when cleared, so the
rotation gates the injection. `opencode` does not: its `/clear` is a
client-side route change that never reaches its server, and the replacement is
allocated by the next prompt. Waiting for a rotation there would deadlock, so
the order inverts — inject after a short gap, then require the rotation before
the renew may complete. Rotation is the proof either way; only its position
moves. Nothing is admitted on documentation alone, since documentation cannot
answer that question: each entry is watched clearing a live session
(`tests/real_herdr_clear.rs`).

A clear the backend never confirms raises one operator notice and never
completes the renew. Before an injection it keeps retrying, which is the only
thing that can re-seed a context that is already gone; after one, it means the
resume prompt may have gone into the context it was meant to replace, and the
cycle needs a look. Long past that notice the cycle is abandoned rather than
driven forever, and a policy arms its next one — the cycle that could not be
proven is the last reason to stop bounding that context.

That is the rule everywhere: a cycle skipped, aborted, or abandoned still arms
the next. Only the incarnation ending ends the policy. Nothing else may leave an
agent looking supervised while nothing is scheduled.

Deliveries addressed to an incarnation stay queued across the destructive
window and fire after the resume prompt; the durable mapping from Herdr
observations to Kelpie outcomes lives in [herdr-outcomes.md](herdr-outcomes.md),
and the normative scheduling rules live in [SPEC.md](../SPEC.md).
