# Isolated live validation procedure

This is a procedure, not an automatic test. It must be approved before any
live resource is created. It never uses the default Herdr session and does not
install a global skill.

## Isolate the resources

1. Build Kelpie and Herdr from clean, known commits into external target
   directories.
2. Create one temporary root, for example `/tmp/kelpie-alpha-XXXXXX`.
3. Start Herdr with separate `XDG_CONFIG_HOME` and `XDG_STATE_HOME` roots and
   an explicit non-default named session, for example `kelpie-alpha`.
4. Verify that the named session's status and socket report the expected Herdr
   version and protocol before Kelpie connects. Create one disposable workspace
   and idle shell pane in that named session; take its pane and terminal IDs
   from the authoritative snapshot.
5. Start `kelpied` with only paths below the temporary root:

   ```sh
   kelpied /tmp/kelpie-alpha/db.sqlite3 \
     /tmp/kelpie-alpha/kelpie.sock \
     /tmp/kelpie-alpha/herdr.sock
   ```

   The Herdr socket is the exact named-session socket. Keep the Kelpie socket
   and database disposable and outside any repository.

## Exercise tell, ask, and reply

This procedure still uses JSON files and the raw client so shell quoting cannot
change IDs or bodies and so every response can be inspected as NDJSON:

```sh
kelpie /tmp/kelpie-alpha/kelpie.sock < start.json > start.out
kelpie /tmp/kelpie-alpha/kelpie.sock < tell.json  > tell.out
kelpie /tmp/kelpie-alpha/kelpie.sock < ask.json   > ask.out
kelpie /tmp/kelpie-alpha/kelpie.sock < pending.json > pending.out
```

The requests must use the exact pane, terminal, public name, backend kind, and
working directory from Herdr's snapshot. Assert all of the following from the
responses and by inspecting the saved JSON:

- `start` reports runtime start and initial-message outcomes separately and
  reaches exact Ready; the initial tell has no reply obligation.
- `tell` retains its immutable message, operation, attempt, and delivery IDs;
  its accepted delivery is not task completion and creates no obligation.
- `ask` returns an immutable message ID and creates one Open obligation for the
  recipient. `pending` returns that exact obligation.
- `reply` uses the ask message ID as `reply_to` and the exact logical sender and
  recipient IDs. A final reply resolves the obligation; progress does not.
- A second `pending` is empty after the correctly correlated final reply.

Also issue `notice.create` and `notice.list` once to prove the local durable
operator inbox is available. Use `recover` only when intentionally testing
fresh-snapshot reconciliation. Never retry an `unknown` operation manually
without recording a new, deliberate intent.

## Cleanup and rollback

Stop only the exact named Herdr session with the same binary, explicit session,
and XDG roots used to start it. Stop `kelpied`, then verify the disposable
Herdr API/client sockets and Kelpie socket are absent and unreachable. The
temporary database and root may then be removed as disposable test artifacts.

If any command reports a path outside the temporary root, or a socket/version
does not match the preflight evidence, stop immediately and leave all resources
untouched for diagnosis. Rollback is limited to stopping the exact disposable
Kelpie/Herdr processes and removing only the exact temporary root. Do not modify
the default Herdr session, global agent configuration, isolated caller
environments, or repository history.
