# Contributing to Kelpie

Thanks for reading. Kelpie is a small, strict codebase; the fastest way to a
merged change is to read `SPEC.md` first and keep your diff honest about it.

## Ground rules

- `SPEC.md` is the normative contract. If your change touches behavior SPEC
  constrains, update SPEC in the same commit. Convenience never outranks the
  contract.
- Durable intent precedes every external effect, and `unknown` is a real
  outcome that is never coerced or resent. If your change writes to Herdr, add
  the matching fault-injection rendezvous points and kill tests — see
  `docs/fault-injection.md`.
- Never rewrite an existing migration. New schema changes mean a new numbered
  file in `migrations/`, a new step in the migration chain, and updates to the
  migration lists used by the store's own tests.

## Setup

A stable Rust toolchain is all you need; `rusqlite` uses the bundled SQLite, so
a C compiler is the only non-Rust requirement. Herdr itself is only needed for
the live-transport tests, which are `#[ignore]`d by default — see
`docs/real-herdr-test.md` if you want to run them.

Keeping the build tree outside the repository keeps it clean:

```sh
export CARGO_TARGET_DIR="${TMPDIR:-/tmp}/kelpie-target"
```

## The gates

CI runs exactly these; run them locally before pushing:

```sh
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets
```

`clippy::pedantic` is warn-level workspace-wide on purpose; code that passes
with `-D warnings` is the bar.

## Sending the change

Open a pull request against `main`. Describe what the change does to the
durable record and to Herdr's live state — the interesting risks in this code
live at that boundary, not in the syntax.
