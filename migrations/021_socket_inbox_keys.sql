-- Idempotency for socket-inbox reply intent. Pane replies keep operations;
-- socket deliveries have no incarnation to target, so they cannot use that table.
BEGIN;

CREATE TABLE socket_inbox_keys (
    idempotency_key TEXT PRIMARY KEY,
    message_id TEXT NOT NULL REFERENCES messages(id)
);

PRAGMA user_version = 21;
COMMIT;
