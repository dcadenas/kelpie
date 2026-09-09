---
name: kelpie-deploy
description: >
  Put a new kelpie build in front of a running fleet: discover the daemon's
  unit and paths, back up the database, install every binary, restart, and
  prove it came back. Use when asked to deploy, ship, roll out, install, or
  restart kelpie or kelpied, when a fix is committed and the daemon is still
  serving the old code, after adding a migration, or when the running daemon
  and the working tree have diverged. Also use before concluding that a change
  is live.
---

# Deploying kelpie to a running fleet

A committed fix changes nothing until the daemon runs it. The daemon holds
durable state for every agent in the fleet, so replacing it is not a build step
with a restart appended — the database is migrated on open, and the migration is
the part that cannot be undone.

Restarting a service is an external effect. Confirm before doing it unless the
request already said to.

## Find what is actually running

Never assume the layout. Read it:

```sh
systemctl --user cat kelpied
```

`ExecStart` names the three things everything below depends on: `--database`,
`--socket`, and `--herdr-socket`. Use those paths, not remembered ones.

Then establish the gap you are closing:

```sh
systemctl --user status kelpied | rg "Active|ExecStart"
kelpie --version
sqlite3 -readonly <database> "PRAGMA user_version;"
ls migrations/ | tail -1
git log --oneline origin/main..HEAD
```

A `user_version` below the highest numbered migration means the running daemon
predates schema the code now expects. That is the strongest signal that a deploy
is owed, and it is invisible from `systemctl status`.

## Back up before anything

```sh
sqlite3 <database> ".backup '<database>.bak-schema<N>-$(date +%Y%m%d-%H%M%S)'"
```

Use `.backup`, not `cp`. The database runs in WAL mode, so a file copy of a live
database can miss committed transactions sitting in the `-wal` file.

Name it after the schema version you are leaving, because that is what the
backup is for. Migrations run automatically when the daemon opens the store and
there is no down-migration: after a schema bump, the previous binary refuses the
database as an unsupported version. The backup is the only rollback, and it is
worth taking even for a deploy with no new migration, because you cannot always
tell in advance.

## Install every binary, not the one you were thinking of

```sh
cargo build --release
which -a kelpie kelpied
```

`kelpie` and `kelpied` are separate binaries and are commonly installed in more
than one directory on `PATH` — `~/.local/bin` and `~/.cargo/bin` both being on it
is normal. Install to all of them:

```sh
install -m755 target/release/kelpie  <dir>/kelpie
install -m755 target/release/kelpied <dir>/kelpied
```

If one directory is updated and another is not, the client and the daemon can be
different versions with nothing reporting it. The daemon serves whatever the unit
file points at; your shell runs whatever `PATH` finds first.

Use the repo's external target directory when it is set, and read the binary from
there:

```sh
export CARGO_TARGET_DIR="${TMPDIR:-/tmp}/kelpie-target"
```

## Restart and prove it came back

### Integer-ID cutover

If the pending migration changes UUID IDs to integers, do not use a single
restart. Capture the currently Ready aliases, then stop the old daemon, migrate
with the newly installed binary, start it, and notify every incarnation that is
Ready after recovery:

```sh
kelpie report --live
systemctl --user stop kelpied
kelpied --database <database> --socket <socket> --herdr-socket <herdr-socket> --migrate-only
sqlite3 -readonly <database> "PRAGMA user_version;"
systemctl --user start kelpied
kelpie report --live
```

The pre-stop report is a reconciliation aid, not the notification target. The
post-start report owns the target set: send this text once to every exact Ready
incarnation through its current alias, and record each delivery outcome:

```text
Kelpie IDs are now positive integers. UUID IDs no longer work. Run `kelpie who`
and `kelpie pending` for current IDs. Open asks survived the migration under new
integer IDs. If you only retained a UUID for an ask, ask its sender to send the
ask again.
```

An accepted tell is the notification receipt. A rejected, unavailable, or
unknown delivery does not prove that Ready incarnation was notified; report it
for explicit reconciliation and do not blindly resend an unknown delivery.

```sh
systemctl --user restart kelpied
```

The daemon opens and recovers the store *before* it binds the socket, so a
migration that fails means no socket at all rather than a daemon serving wrong
answers. A client that cannot connect after a restart is the expected shape of
that failure — check the unit, not the data.

Verify all four, and do not report success on fewer:

```sh
systemctl --user status kelpied | rg Active
sqlite3 -readonly <database> "PRAGMA user_version;"          # matches the newest migration
sqlite3 -readonly <database> ".schema <table>" | rg <column>  # new columns are present
kelpie who                                                    # the socket answers
kelpie report | tail -3                                       # the fleet survived
```

`kelpie report` before and after should show the same agents. Recovery is
idempotent and a restart is not supposed to cost anything, so a changed count is
a finding, not a rounding error.

## Report honestly

Say which commit is now running, that the schema moved and to what, where the
backup is, and that the old binary can no longer open the migrated database. A
deploy report that omits the one-way migration leaves the reader believing they
can roll back by reinstalling.

If a durable record was expected to change as a result of the deploy — a stalled
operation that the new code should resolve — read it back and quote it rather
than predicting it. See the `kelpie-diagnose` skill for how.
