-- Per-obligation reverse-path policy and ask-scoped pull sink.
-- LogicalAgent.delivery_transport stays herdr_prompt | socket_inbox.
PRAGMA foreign_keys=OFF;

BEGIN;

ALTER TABLE obligations ADD COLUMN reply_delivery TEXT NOT NULL DEFAULT 'inject'
    CHECK (reply_delivery IN ('inject', 'pull'));

CREATE TABLE IF NOT EXISTS ask_pull_leases (
    lease_id INTEGER PRIMARY KEY AUTOINCREMENT,
    ask_message_id INTEGER NOT NULL UNIQUE REFERENCES obligations(ask_message_id),
    waiting_agent_id INTEGER NOT NULL REFERENCES logical_agents(id),
    claimed_at_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS ask_pull_keys (
    idempotency_key TEXT PRIMARY KEY,
    message_id INTEGER NOT NULL REFERENCES messages(id)
);

CREATE TABLE deliveries_v31 (
    message_id INTEGER NOT NULL REFERENCES messages(id),
    delivery_transport TEXT NOT NULL DEFAULT 'herdr_prompt'
        CHECK (delivery_transport IN ('herdr_prompt', 'socket_inbox', 'ask_pull')),
    recipient_incarnation_id INTEGER REFERENCES incarnations(id),
    recipient_agent_id INTEGER REFERENCES logical_agents(id),
    attempt_number INTEGER NOT NULL,
    scheduled_at_ms INTEGER NOT NULL,
    attempted_at_ms INTEGER,
    resolved_at_ms INTEGER,
    herdr_request_id TEXT,
    outcome TEXT NOT NULL CHECK (outcome IN (
        'pending','submitted','accepted','queued','unknown','rejected',
        'target_unavailable','superseded'
    )),
    operation_id INTEGER REFERENCES operations(id),
    cancellation_requester_agent_id INTEGER REFERENCES logical_agents(id),
    cancellation_reason TEXT,
    cancelled_at_ms INTEGER,
    CHECK (
        (delivery_transport = 'herdr_prompt'
         AND recipient_incarnation_id IS NOT NULL
         AND operation_id IS NOT NULL)
        OR
        (delivery_transport = 'socket_inbox'
         AND recipient_agent_id IS NOT NULL
         AND recipient_incarnation_id IS NULL
         AND herdr_request_id IS NULL)
        OR
        (delivery_transport = 'ask_pull'
         AND recipient_agent_id IS NOT NULL
         AND recipient_incarnation_id IS NULL
         AND operation_id IS NULL
         AND herdr_request_id IS NULL)
    )
);

INSERT INTO deliveries_v31 (
    message_id, delivery_transport, recipient_incarnation_id, recipient_agent_id,
    attempt_number, scheduled_at_ms, attempted_at_ms, resolved_at_ms,
    herdr_request_id, outcome, operation_id,
    cancellation_requester_agent_id, cancellation_reason, cancelled_at_ms
)
SELECT
    message_id, delivery_transport, recipient_incarnation_id, recipient_agent_id,
    attempt_number, scheduled_at_ms, attempted_at_ms, resolved_at_ms,
    herdr_request_id, outcome, operation_id,
    cancellation_requester_agent_id, cancellation_reason, cancelled_at_ms
FROM deliveries;

DROP TABLE deliveries;
ALTER TABLE deliveries_v31 RENAME TO deliveries;

CREATE UNIQUE INDEX deliveries_herdr_attempt
    ON deliveries(message_id, recipient_incarnation_id, attempt_number)
    WHERE recipient_incarnation_id IS NOT NULL;
CREATE UNIQUE INDEX deliveries_socket_attempt
    ON deliveries(message_id, recipient_agent_id, attempt_number)
    WHERE recipient_agent_id IS NOT NULL;
CREATE INDEX deliveries_message_outcome ON deliveries(message_id, outcome);
CREATE INDEX ask_pull_deliveries_ask
    ON deliveries(recipient_agent_id, message_id)
    WHERE delivery_transport = 'ask_pull';

PRAGMA user_version = 31;
COMMIT;

PRAGMA foreign_keys=ON;
