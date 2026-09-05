# Kelpie

[![CI](https://github.com/dcadenas/kelpie/actions/workflows/ci.yml/badge.svg)](https://github.com/dcadenas/kelpie/actions/workflows/ci.yml)

> **Structured messaging semantics for agents running in Herdr.**

**Kelpie is alpha.** The CLI, the wire protocol, and the database schema can
change between releases, and it targets exactly one Herdr protocol version.
Expect to rebuild both when you upgrade either.

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
tell can be scheduled once with `--due-in`, or repeatedly on the wall clock with
`--every`. A repeating tell follows the logical agent across runtime changes but
never starts or revives one. A reminder nudges a reviewer who went idle
without replying. A cancelled ask records who cancelled and why, instead of
lingering forever. A delivery that crosses an uncertain write boundary is
recorded `unknown` and never resent blindly.

## Install

Kelpie is a layer on top of [Herdr](https://github.com/herdrdev/herdr) and does
nothing without it. Set up Herdr first.

**1. Install Herdr and run it once.** Herdr's README covers `brew install
herdr`, `mise use -g herdr`, and a curl installer. Kelpie speaks Herdr protocol
20, which ships in Herdr 0.8.2; older releases are refused at startup with
`incompatible_runtime`. Running `herdr` once creates its socket at
`~/.config/herdr/herdr.sock`, which is where Kelpie looks by default.

**2. Install Kelpie.** The crate is published as `kelpie-herdr`; the plain
`kelpie` name on crates.io belongs to an unrelated project.

```sh
cargo install kelpie-herdr
# or, from a checkout:
cargo install --path .
```

This puts two binaries on your PATH: `kelpie`, the client that agents call,
and `kelpied`, the daemon. Agents run `kelpie` from inside Herdr panes, so
`~/.cargo/bin` needs to be on the PATH those panes inherit.

**3. Install the skill for your coding agent.** The skill is what makes an
agent reach for `kelpie tell` and `kelpie ask` instead of raw Herdr prompts.
Name your agent with `-a` so the install is non-interactive:

```sh
npx skills add dcadenas/kelpie --skill kelpie -g -a claude-code -y
```

Use `-a codex`, `-a opencode`, and so on for other agents, or drop `-a` and
`-y` to be asked. That command installs from the git head. The installed binary
carries the copy that matches its own version, which is the one to use if you
pin Kelpie to a release:

```sh
mkdir -p ~/.claude/skills/kelpie
kelpie --skill > ~/.claude/skills/kelpie/SKILL.md
```

Restart coding-agent sessions that were already open; they index skills at
startup.

## Run

```sh
kelpied
```

`kelpied` is a long-running foreground process. It opens its database, waits
for Herdr's socket if Herdr is not up yet, then binds its own socket and prints
`listening on …` to stderr. Closing the terminal stops it, and everything
agents have asked each other stays in the database until it comes back.
Keeping it alive across logins is your own setup: a systemd user unit on
Linux, a launchd agent on macOS, or a tab you leave open.

Default paths, all overridable by flag:

| | Path |
| --- | --- |
| Database | `$XDG_STATE_HOME/kelpie/kelpie.sqlite3` |
| Kelpie socket | `$XDG_RUNTIME_DIR/kelpie/kelpie.sock` |
| Herdr socket | `$HERDR_SOCKET_PATH`, else `$XDG_CONFIG_HOME/herdr/herdr.sock` |

## Verify

Open a pane in Herdr, start your coding agent in it, and have it run:

```sh
kelpie who
```

The client reads the pane id Herdr exports, the daemon adopts that pane as a
logical agent on first contact, and the answer names it. Agents opened
manually inside Herdr are always adopted this way: existing Herdr names are
kept, and unnamed agents get a name from their working directory.

Then open a second pane with another agent and have it send the first one a
message by name:

```sh
kelpie tell <name> --body "hello from the other pane"
```

A `<kelpie from=… >` envelope appears in the first pane. That is the whole
loop: two agents, addressed by name, with the delivery recorded.

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
- **Beyond panes.** A process that is not a Herdr pane registers as a socket
  waiter with `kelpie waiter-register` and receives its deliveries over
  Kelpie's own socket. Agents address it by name like any other; it can ask,
  be asked, and cancel.

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

Kelpie supports exactly Herdr protocol 20 (Herdr 0.8.2) and refuses other
protocol versions before sending any mutation.

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
