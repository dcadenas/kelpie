# AGENTS.md

This file provides guidance to coding agents (Claude Code, Codex, and others) when
working with code in this repository. `CLAUDE.md` imports this file.

## Commands

```sh
cargo fmt --check
cargo clippy --all-targets --all-features
cargo test
```

Lints are strict: `clippy::pedantic` is warn-level workspace-wide, plus
`missing_debug_implementations` and `unsafe_op_in_unsafe_fn`.

Subprocess tests spawn the real `kelpie`/`kelpied` binaries via
`env!("CARGO_BIN_EXE_...")`, so use `--all-targets` when building for them.
`docs/client-protocol.md` recommends an external target dir to keep the tree clean:

```sh
export CARGO_TARGET_DIR="${TMPDIR:-/tmp}/kelpie-target"
cargo build --all-targets
cargo test --all-targets
```

Single test / single file:

```sh
cargo test --test process_kill_ask                      # one integration file
cargo test --test process_kill_ask ask_pre_write         # one test in it
cargo test --lib store::tests::                          # unit tests in a module
```

`tests/real_herdr.rs` is `#[ignore]`d and needs a live disposable Herdr session
plus `KELPIE_TEST_*` coordinates — see `docs/real-herdr-test.md` and
`docs/real-alpha-test.md` before running it:

```sh
cargo test --test real_herdr -- --ignored --nocapture
```

## Architecture

Kelpie is a durable coordination layer on top of [Herdr](https://github.com/herdrdev/herdr).
The single hardest thing to get right — and the thing most of the code exists to
protect — is the **authority split**: Herdr is the only authority for live runtime
facts (panes, terminals, processes, agent status); Kelpie is the only authority for
logical identity, message semantics, reply obligations, and recovery. Kelpie never
holds an independent opinion about whether something is alive; it re-snapshots.

`SPEC.md` is the normative contract (RFC 2119 MUST/SHOULD language) and outranks
convenience. Changing behavior that SPEC.md constrains means updating SPEC.md,
not just the code.

The Cargo package is `kelpie-herdr`, because `kelpie` on crates.io is an
unrelated crate. The library, both binaries, the socket path, the skill, and
the envelope tags are all still `kelpie`; `Cargo.toml` pins those names
explicitly. Do not rename them to match the package.

### Layers

- `src/bin/kelpied.rs` → `daemon.rs` — foreground daemon. Opens and recovers the
  store *before* binding the Unix socket. Dispatches one NDJSON request per
  connection (`recover`, `start`, `adopt`, `tell`, `ask`, `reply`, `clear`, `renew`,
  `renew.cancel`, `pending`, `cancel`, `retire`, `waiter.register`, `waiter.retire`,
  `reminder.snooze`, `reminder.disable`,
  `notice.create`, `notice.list`, `who`, and the legacy identity aliases). `inbox.claim` is the exception: it
  keeps the connection and drains `inbox.delivery` events for that waiter;
  `inbox.ack` is valid only on that claimed connection. Uses a non-blocking accept timeout
  so scheduled deliveries, reminders, and renew phases run with no client
  connected.
- `herdr_exec.rs` — bounded off-thread Herdr executor. Snapshot requests share
  one lane; mutations use one FIFO lane per pane. The daemon owns all SQLite
  transitions and advances parked operations from executor events.
- `src/bin/kelpie.rs` → `cli.rs` — the client. Typed commands build the NDJSON
  request; the legacy raw mode (`kelpie SOCKET`) forwards one JSON request from
  stdin. Every response is persisted to a receipt file so a lost stdout pipe
  never loses a successful RPC.
- `slice.rs` — the `Kelpie` facade wiring store + Herdr adapter. This is where the
  intent-before-effect ordering and outcome commits are sequenced.
  `slice/blocking.rs` is the only direct synchronous Herdr home, used by the
  facade and deterministic inline tests. Never call those blocking paths from
  `Daemon::poll`; park the operation through `HerdrExec` instead.
- `store.rs` — SQLite (rusqlite, bundled) durable state and every state-machine
  transition. The largest file by far; most invariants are enforced here in SQL
  transactions.
- `herdr.rs` — typed client for Herdr's documented NDJSON socket protocol. Never
  parses Herdr CLI output, never touches Herdr's private rendering socket or
  persistence files. Supports **exactly protocol 20**; anything else is
  `incompatible_runtime` and no mutation is sent.
- `domain.rs` — UUIDv7 newtype IDs and the state enums (`IncarnationState`,
  `OperationOutcome`, `ObligationState`, `DeliveryOutcome`, …).
- `envelope.rs` — the agent-facing `<kelpie from=… >` / `<kelpie-reminder …>` text
  representation. Bodies escape `<`, `>`, `&` so untrusted message text cannot
  forge envelope metadata. Client↔daemon traffic stays strict NDJSON; envelopes are
  only what gets typed into a terminal.
- `name.rs` — Herdr's name grammar (lowercase-leading, ≤32 chars) and cwd-basename
  derivation used by adoption.
- `attribution.rs` — keeps *requested* model/provider/effort strictly separate from
  *observed* execution metadata. Requested config is never reported as proof of what
  served a turn.
- `paths.rs` — XDG defaults: DB at `$XDG_STATE_HOME/kelpie/kelpie.sqlite3`, socket at
  `$XDG_RUNTIME_DIR/kelpie/kelpie.sock`, Herdr socket from `$HERDR_SOCKET_PATH` then
  `$XDG_CONFIG_HOME/herdr/herdr.sock`.
- `test_fault.rs` — compiled fault-injection rendezvous points (test infrastructure,
  not an operational API).

### Invariants that shape the code

- **Durable intent precedes every external effect.** The daemon acquires an
  executor lease before marking a mutation attempt submitted. It then writes to
  Herdr and commits the outcome. The daemon thread owns every row transition and
  MUST NOT wait on Herdr.
- **`unknown` is a real outcome.** It is never coerced to success or failure, and
  `submitted`/`accepted`/`queued`/`unknown` deliveries are never blindly resent —
  the recipient may already have the message. The single recorded exception is a
  renew's resume prompt, which is retried until accepted because its recipient
  has no context left to notice a duplicate and no way to survive its absence.
- **Exact incarnation targeting.** A delayed result for an old incarnation must not
  mutate a newer one, even when the pane, terminal, or public name was reused.
  Public names are reusable live aliases, never primary keys.
- **Obligations outlive runtimes.** An ask's obligation survives Kelpie and Herdr
  restarts. A progress reply sets `in_progress` but never resolves; a final reply
  resolves only on *accepted* delivery, and only for the obligation named by
  `reply_to`.
- **Fail closed on ambiguity.** Adoption, alias resolution, and reply correlation
  all reject ambiguous or mismatched matches rather than guessing.
- **A binding is pane + terminal + backend kind + public name.** The
  backend-native session reference is deliberately *not* part of it. It
  identifies a conversation, which a live agent rotates on clear, resume,
  compaction, or fork; requiring it to match read those rotations as runtime
  replacement and de-addressed live agents. Reconciliation refreshes the
  recorded reference instead. Do not reintroduce the check.
- **Recovery is idempotent.** Repeating it against unchanged durable + Herdr state
  produces no new external effects.

### Migrations

`migrations/NNN_*.sql` are applied in order by `store.rs` via `include_str!` and are
also listed in `Cargo.toml`'s `include`. A new migration means a new numbered file,
a new `execute_batch` call at the schema-version step, and updating the migration
lists used by the store's own tests. Never rewrite a prior external outcome to hide
history — corrections are later evidence or a superseding outcome.

### Fault-injection tests

`tests/process_kill_*.rs` are deterministic: the daemon blocks at a named rendezvous
point (`KELPIE_TEST_FAULT_POINTS` + `KELPIE_TEST_FAULT_SOCKET`) while a fake Herdr
server proves exactly how many request bytes crossed the wire, then the harness
`SIGKILL`s it. No sleeps, no timing windows. Each external-effect boundary has three
points — before write, after write/before response, after response/before commit.
A multi-phase operation instead gets a point per phase boundary, because what has
to be proved there is which phase may repeat: renew's four points separate the
clear, which must never be sent twice, from the injection, which must never be
abandoned. See `docs/fault-injection.md`. When adding an operation that writes to
Herdr, add the matching points and kill tests.

### Skill

`skills/kelpie/SKILL.md` is the agent-facing usage guide, embedded in the binary via
`include_str!` and printed by `kelpie --skill`. `tests/skill_package.rs` asserts the
printed text matches the file byte-for-byte, so edit the file, not a copy.

Its siblings are for working *on* Kelpie rather than using it, and ship with the
repository instead of the crate: `skills/kelpie-deploy` puts a build in front of a
running fleet, and `skills/kelpie-diagnose` reads the durable record when an
operation looks wrong. Install them for whatever agent you use with
`npx skills add . -a '*' -s kelpie-deploy -s kelpie-diagnose`; the per-agent trees
that creates are generated and git-ignored, and each one links back to
`skills/<name>/`. Only `skills/kelpie/SKILL.md` is listed in `Cargo.toml`'s
`include`, so adding a maintainer skill never changes what the crate publishes.
