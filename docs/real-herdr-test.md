# Real Herdr lifecycle test

The ignored integration test speaks directly to Herdr's documented socket API.
It does not invoke or parse the Herdr CLI. It starts an agent in an existing
shell pane, waits for exact readiness, delivers one ask, and records one
explicit correlated final reply without inferring task completion.

Use only a disposable Herdr session and an idle shell pane. Isolate both
`XDG_CONFIG_HOME` and `XDG_STATE_HOME`, use an explicit non-default
`--session`, and verify that `status server --json` reports the expected socket
and protocol 20 before pointing Kelpie at it. Create the fixture workspace in
that named session and take the pane and terminal IDs from its snapshot.

Herdr v20 has no documented graceful agent-stop operation. Stop the exact
disposable named session after the test; this terminates its disposable panes
without affecting the default server. Verify both named-session sockets are
removed and unreachable. Never run an unscoped stop command as part of this
test.

Set the following non-secret test coordinates:

- `KELPIE_TEST_HERDR_SOCKET`
- `KELPIE_TEST_PANE_ID`
- `KELPIE_TEST_TERMINAL_ID`
- `KELPIE_TEST_CWD`
- `KELPIE_TEST_AGENT_NAME` (unique in the session)
- optionally `KELPIE_TEST_AGENT_KIND` (defaults to `codex`)
- optionally `KELPIE_TEST_AGENT_ARGS_JSON` (defaults to `[]`)

Then run:

```sh
cargo test --test real_herdr -- --ignored --nocapture
```

## Proving a backend's clear before adding it to the renew table

`tests/real_herdr_clear.rs` is the check that qualifies a backend for renew, and
it takes the same coordinates plus a required `KELPIE_TEST_AGENT_KIND`:

```sh
KELPIE_TEST_AGENT_KIND=pi cargo test --test real_herdr_clear -- --ignored --nocapture
```

It launches the agent, warms the conversation with one prompt, records the
backend-native session reference, sends the shipped clear command for that kind,
and then checks the rotation the recorded protocol claims: immediately for an
`OnClear` backend, or — for an `OnNextPrompt` backend — that nothing rotates
until a prompt is sent and that something does once it is. Either direction of
mismatch fails, so a wrong entry is caught rather than tolerated.

Documentation cannot replace this run, because it does not answer the timing
question at all. opencode's `/clear` empties its context exactly as documented,
and its rotation still arrives only with the next prompt.

The ordinary deterministic suite compiles but does not execute this test:

```sh
cargo test --all-targets
```

For the broader isolated procedure covering tell, ask, pending, reply,
notices, and exact cleanup, see [`real-alpha-test.md`](real-alpha-test.md).
