# Kelpie

[![CI](https://github.com/dcadenas/kelpie/actions/workflows/ci.yml/badge.svg)](https://github.com/dcadenas/kelpie/actions/workflows/ci.yml)

> **Structured messaging semantics for agents running in Herdr.**

[Herdr](https://github.com/herdrdev/herdr) can deliver text to an agent. Kelpie
turns that raw input into recognizable peer messages with explicit senders,
intent, and reply correlation — with pending answers, progress updates,
reminders, and recovery that survive restarts of Kelpie, Herdr, or the agent.

## A conversation, annotated

You ask a coordinator to get a review from a reviewer:

```text
<kelpie from=coordinator reply-to=01J9Q5…>Review PR 783.</kelpie>
```

The reviewer owes a final reply, and reports while working:

```text
<kelpie from=reviewer re=01J9Q5… progress>Still checking restarts.</kelpie>
```

When the reviewer sends the final reply and Kelpie sees it accepted into the
coordinator's pane, the obligation is resolved. Nobody polls: the reply is
pushed into the waiting session the moment it is written, so the sender is free
to do other work or sit idle.

```text
<kelpie from=reviewer re=01J9Q5… final>The change handles restarts correctly.</kelpie>
```

An ask is always delivered now — nobody is ever owed work they cannot see. A
tell can be scheduled for later. A reminder nudges a reviewer who went idle
without replying. A cancelled ask records who cancelled and why, instead of
lingering forever. A delivery that crosses an uncertain write boundary is
recorded `unknown` and never resent blindly.

## Install

```sh
cargo install --path .
npx skills add dcadenas/kelpie --skill kelpie -g
```

Restart existing coding-agent sessions after installing or updating the skill.
Kelpie also embeds the release-matched skill in its binary: `kelpie --skill`.

## Run

```sh
kelpied
```

Kelpie waits for Herdr if it is not up yet, so either order works. Agents
opened manually inside Herdr are adopted lazily when they first use Kelpie;
existing Herdr names are kept, and unnamed agents receive a name from their
working directory.

## What you get

- **Durable asks.** A reply obligation lives in SQLite, not in a context
  window. It survives restarts and is reminded at working-to-idle boundaries;
  it can be snoozed or disabled per ask.
- **Honest delivery.** Every message records whether it was accepted, rejected,
  or is unknown. An unknown is never retried blindly.
- **Addressability.** Adopt an agent that lost its binding — same identity,
  same history and obligations. Rename in one step without touching the
  process. Retire a finished worker while keeping its records. Report what is
  alive and what is owed.
- **Renew.** Bound a long-running agent's context deliberately: checkpoint to
  a file, clear, resume — with the runtime, identity, and obligations intact.

## Renew: replacing a context on purpose

An agent that runs for weeks gets compacted by its host. Compaction summarizes
summaries and loses the original. Renew replaces that with something you can
read and diff: the agent writes what matters to a file, the context is cleared
outright, and a fixed prompt tells it to read the file and continue. The
runtime, the pane, the logical agent, its children, its obligations, and its
message history are untouched — only the context is replaced.

```text
<kelpie-renew from=coordinator reply-to=01J9Q6… prepare cycle=1>Write progress.md so it resumes this work.</kelpie-renew>
… the agent writes progress.md and replies final …
<kelpie-renew from=coordinator resumed cycle=1>Read progress.md and continue.</kelpie-renew>
```

The prepare phase is a real ask, so the agent can answer "not now" instead of
failing silently, and the clear provably lands on a settled agent. Prompts
split into three layers — one-time bootstrap in the start prompt, invariants
in the standing resume prompt, current work in the checkpoint file — and the
[`kelpie` skill](skills/kelpie/SKILL.md) teaches that split to agents.

Backend support, proof rules, and the per-backend clear semantics live in
[docs/renew-backends.md](docs/renew-backends.md).

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

## Status

Kelpie is an early release. It supports Herdr protocol 20 and refuses
incompatible protocol versions.

The installed [`kelpie` skill](skills/kelpie/SKILL.md) is the agent-facing usage
guide. [`SPEC.md`](SPEC.md) defines the implementation and recovery contract,
and [`docs/`](docs/) carries the protocol details, outcome mappings, and
operational procedures.

## Develop

```sh
cargo fmt --check
cargo clippy --all-targets --all-features
cargo test
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for the ground rules.

## License

[MIT](LICENSE)
