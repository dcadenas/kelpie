# Kelpie

[![CI](https://github.com/dcadenas/kelpie/actions/workflows/ci.yml/badge.svg)](https://github.com/dcadenas/kelpie/actions/workflows/ci.yml)

> **Structured messaging semantics for agents running in Herdr.**

[Herdr](https://github.com/herdrdev/herdr) can deliver text to an agent. Kelpie
turns that raw input into recognizable peer messages with explicit senders,
intent, and reply correlation.

Those semantics make pending answers, progress updates, reminders, and recovery
possible.

## Messaging Flow

The first rows can begin with a user asking an agent to contact or start a peer.
The remaining messages are normally sent by the agents themselves, or by Kelpie,
as the work proceeds.

| What happens | Peer receives | Protocol effect |
| --- | --- | --- |
| You ask a coordinator to inform a reviewer. | `<kelpie from=coordinator>`<br>`The API changed.`<br>`</kelpie>` | The coordinator sends a one-way message. No reply is expected. |
| You ask a coordinator to get a review. | `<kelpie from=coordinator reply-to=ASK_ID>`<br>`Check the change.`<br>`</kelpie>` | The coordinator creates a pending obligation identified by `ASK_ID`. |
| You ask a coordinator to start a worker on a task. | `<kelpie from=coordinator reply-to=ASK_ID>`<br>`Review PR 783.`<br>`</kelpie>` | The new agent's first input is already a peer message. Launching the runtime and delivering the brief are reported as separate outcomes. |
| The reviewer is still working. | `<kelpie from=reviewer re=ASK_ID progress>`<br>`Still checking restart behavior.`<br>`</kelpie>` | The reviewer reports progress. The ask stays pending and its reminder timer resets. |
| The reviewer finishes the work. | `<kelpie from=reviewer re=ASK_ID final>`<br>`The change handles restarts correctly.`<br>`</kelpie>` | The reviewer sends the required final reply. Its accepted delivery resolves `ASK_ID`. |
| The reviewer finishes a turn without replying. | `<kelpie-reminder waiting=coordinator reply-to=ASK_ID>`<br>`Pending final reply.`<br>`</kelpie-reminder>` | Kelpie reminds the reviewer at a working-to-`idle`/`done` boundary. Later reminders use a five-minute interval. |
| The reviewer needs more time before another reminder. | No peer message is sent. | The reviewer can snooze reminders while leaving the ask pending. |
| Reminders are not useful for this ask. | No peer message is sent. | The reviewer can disable reminders while leaving the ask pending and visible. |
| A message should arrive later, not now. | The same envelope, at the due time. | Only a tell can be scheduled: Kelpie holds it and fires it once. An ask is always delivered now, so nobody is ever owed work they cannot see. |
| An ask is no longer needed. | No peer message is sent. | The waiter cancels the obligation with a reason, so it stops being owed instead of lingering. |
| An agent checks which answers it still owes. | A list of pending ask IDs and waiting agents. | Kelpie reads durable obligations instead of guessing from conversation text. |
| Kelpie, Herdr, or an agent restarts. | No message needs to be replayed. | Pending obligations survive and remain attached to their logical agents. |
| Delivery crosses an uncertain write boundary. | The message may already have arrived. | Records an `unknown` outcome instead of resending blindly. |

Nobody waits. An ask returns whether it reached the other agent, never their
answer. The reply is pushed into the waiting agent's session when it is written,
so a sender is free to do other work, or to sit idle, without polling and
without a timeout to outlive.

`ASK_ID` represents the immutable identifier Kelpie assigns to an ask. The
installed skill tells agents when to send progress and final replies and how to
preserve that ID. Users normally communicate in natural language rather than
driving each protocol message.

## Keeping Agents Addressable

Messages are only useful if the intended recipient can still be named. Kelpie
treats that as its own responsibility.

| Situation | What you do | What is preserved |
| --- | --- | --- |
| An agent is alive in Herdr but Kelpie cannot address it. | Adopt it, continuing the same logical agent. | Its history, messages, and pending obligations. |
| An agent has the wrong public name. | Rename it in one step. | The same running process, pane, and obligations. No new incarnation. |
| A worker has finished. | Retire it, optionally releasing its pane. | Its worktree, transcripts, messages, and durable records. |
| You want to see what is running now. | Ask for a report. | The living agents, who started whom, and every reply still owed. Full history stays available. |

A launch that cannot be proven ready fails quickly and says which condition
failed, rather than reporting an uncertain outcome that invites a duplicate
worker. One agent's slow launch does not delay messages between other agents.

## Keeping Agents Coherent

An agent that stays addressable for weeks still fills its context window, and
its host compacts it. Compaction summarizes: it decides what matters, discards
the rest, and does so non-reproducibly. Each cycle summarizes the previous
summary, so the working state drifts away from what actually happened and
nothing records what was lost.

`kelpie renew` replaces that with something you can read and diff. The agent
writes what matters to a file, its context is cleared outright, and a fixed
prompt tells it to read that file and continue. The runtime, the pane, the
process, the logical agent, its children, its obligations, and its message
history are untouched — only the context is replaced.

| What happens | Agent receives | Protocol effect |
| --- | --- | --- |
| You schedule a renew for a long-running worker. | Nothing yet. | Both prompts are durable before anything reaches Herdr, so an interrupted renew is finished from the stored record rather than restarted. |
| The renew comes due. | `<kelpie-renew from=coordinator reply-to=ASK_ID prepare cycle=1 deadline-ms=MS>`<br>`Write progress.md so it resumes this work.`<br>`</kelpie-renew>` | The prepare prompt is a real ask. It states what is about to happen, what survives, what does not, and quotes the resume prompt verbatim, so the checkpoint is written for the reader that will actually receive it. |
| The agent finishes its checkpoint. | No peer message is sent. | `kelpie reply ASK_ID --final` is the ready signal. An agent must end its turn to reply finally, so the clear lands on a settled agent by construction. |
| The agent needs more time, or cannot serialize some state. | No peer message is sent. | It is an ordinary obligation, so progress replies, reminders, snooze, and cancel all apply. Disclosure is what lets an agent say now is a bad time instead of failing silently. |
| The agent never confirms. | No peer message is sent. | `--on-timeout` decides and has no default. `abort` protects unsaved work and lets the context keep growing; `proceed` bounds the context and destroys what was not saved. Either way an operator notice is raised. |
| Kelpie clears the context. | Nothing — the context is gone. | Kelpie sends the backend's clear command, then polls until the backend-native session id actually changes. Elapsed time and `idle` cannot distinguish "not cleared yet" from "cleared", so neither is used. |
| The context is empty again. | `<kelpie-renew from=coordinator resumed cycle=1 checkpointed-at-ms=MS>`<br>`Read progress.md and continue.`<br>`</kelpie-renew>` | The resume prompt is injected. The agent is told it is a continuation and which cycle it is, so it does not re-plan finished work or greet an operator who is not there. |
| Someone messages the agent mid-renew. | The same envelope, after the resume prompt. | Deliveries stay queued across the destructive window. A message dropped into a context about to be wiped would be recorded `accepted` for an agent that will never see it. |
| The renewed agent still owes replies. | Its usual reminders. | Obligations live in SQLite, not in a context window. A renewed agent is reminded of asks it can no longer remember receiving. |

`--due-in`/`--due-at` renew once. `--every 45m` re-arms after each injection and
becomes a standing rule for the life of the agent, ending when the incarnation
stops being Ready. Its first cycle is one interval away, not immediate: an agent
arms a policy once it has read itself in, and clearing on arming would discard
exactly what it just paid for. A prepare timeout is reported, never silently
disarming: an agent that will not checkpoint is a problem to look at, while an
agent that no longer exists needs nothing.

Only backends whose clear Kelpie can *prove* are accepted — `/clear` for
`claude`, `codex`, and `opencode`, `/new` for `grok` and `pi`. Every other
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

This splits prompt authorship into three layers, and putting an instruction in
the wrong one is how renewals go wrong. One-time bootstrap belongs in the start
prompt. Invariants belong in the standing resume prompt, which runs on every
cycle forever and so must be reentrant. Current work belongs in the checkpoint
file, which is rewritten each cycle. The [`kelpie`
skill](skills/kelpie/SKILL.md) teaches that split to the agents that have to
write for it.

## Why a Separate Layer?

| Herdr | Kelpie |
| --- | --- |
| Runs and observes agents | Identifies logical senders and receivers |
| Delivers terminal input | Marks input as a peer message |
| Reports agent lifecycle | Uses lifecycle for safe reminders |
| Accepts prompt requests | Records delivery outcomes |
| Provides live agent names | Correlates asks and replies across restarts |
| Launches the backend | Records which model actually served a turn |
| Reuses panes and names freely | Never mistakes a reused name for the same agent |
| Accepts a clear command | Proves the clear landed and re-seeds the empty context |

Herdr owns the runtime. Kelpie owns what messages mean.

Kelpie distills messaging ideas explored in
[Ouija](https://github.com/dcadenas/ouija) into a focused layer on top of Herdr.
The same need is now reflected in [Claude Code's native cross-session
messaging](https://code.claude.com/docs/en/cross-session-messaging), while
Kelpie provides richer messaging semantics independently of any one coding
agent.

## Install

Install the Kelpie client and daemon from this repository:

```sh
cargo install --path .
```

Install the agent-facing skill:

```sh
npx skills add dcadenas/kelpie --skill kelpie -g
```

Restart existing coding-agent sessions after installing or updating the skill.
Sessions retain the instructions loaded when they started.

Kelpie also embeds the release-matched skill in its binary:

```sh
kelpie --skill
```

## Run

```sh
kelpied
```

Kelpie waits for Herdr if it is not up yet, so either order works.

Agents opened manually inside Herdr are adopted lazily when they first use
Kelpie. Existing Herdr names are kept. Unnamed agents receive a name based on
their working directory, with a suffix if that name is already taken. To use a
different available name, rename the agent in Herdr before adoption.

## Status

Kelpie is an early release. It supports Herdr protocol 20 and refuses
incompatible protocol versions.

The installed [`kelpie` skill](skills/kelpie/SKILL.md) is the agent-facing usage
guide. [`SPEC.md`](SPEC.md) defines the implementation and recovery contract.

## Develop

```sh
cargo fmt --check
cargo clippy --all-targets --all-features
cargo test
```

## License

[MIT](LICENSE)
