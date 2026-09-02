-- Pane-less LogicalAgent identity for socket-inbox waiters, and deliveries that
-- name a waiter agent instead of a Herdr incarnation.
PRAGMA foreign_keys=OFF;

BEGIN;

ALTER TABLE logical_agents ADD COLUMN delivery_transport TEXT NOT NULL DEFAULT 'herdr_prompt'
    CHECK (delivery_transport IN ('herdr_prompt', 'socket_inbox'));
ALTER TABLE logical_agents ADD COLUMN targeting_ended_at_ms INTEGER;

CREATE TABLE socket_waiter_keys (
    idempotency_key TEXT PRIMARY KEY,
    logical_agent_id TEXT NOT NULL REFERENCES logical_agents(id)
);

CREATE TABLE deliveries_v20 (
    message_id TEXT NOT NULL REFERENCES messages(id),
    delivery_transport TEXT NOT NULL DEFAULT 'herdr_prompt'
        CHECK (delivery_transport IN ('herdr_prompt', 'socket_inbox')),
    recipient_incarnation_id TEXT REFERENCES incarnations(id),
    recipient_agent_id TEXT REFERENCES logical_agents(id),
    attempt_number INTEGER NOT NULL,
    scheduled_at_ms INTEGER NOT NULL,
    attempted_at_ms INTEGER,
    resolved_at_ms INTEGER,
    herdr_request_id TEXT,
    outcome TEXT NOT NULL CHECK (outcome IN (
        'pending','submitted','accepted','queued','unknown','rejected',
        'target_unavailable','superseded'
    )),
    operation_id TEXT REFERENCES operations(id),
    cancellation_requester_agent_id TEXT REFERENCES logical_agents(id),
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
    )
);

INSERT INTO deliveries_v20 (
    message_id, delivery_transport, recipient_incarnation_id, recipient_agent_id,
    attempt_number, scheduled_at_ms, attempted_at_ms, resolved_at_ms,
    herdr_request_id, outcome, operation_id,
    cancellation_requester_agent_id, cancellation_reason, cancelled_at_ms
)
SELECT
    message_id, 'herdr_prompt', recipient_incarnation_id, NULL,
    attempt_number, scheduled_at_ms, attempted_at_ms, resolved_at_ms,
    herdr_request_id, outcome, operation_id,
    cancellation_requester_agent_id, cancellation_reason, cancelled_at_ms
FROM deliveries;

DROP TABLE deliveries;
ALTER TABLE deliveries_v20 RENAME TO deliveries;

CREATE UNIQUE INDEX deliveries_herdr_attempt
    ON deliveries(message_id, recipient_incarnation_id, attempt_number)
    WHERE recipient_incarnation_id IS NOT NULL;
CREATE UNIQUE INDEX deliveries_socket_attempt
    ON deliveries(message_id, recipient_agent_id, attempt_number)
    WHERE recipient_agent_id IS NOT NULL;

PRAGMA user_version = 20;
COMMIT;

PRAGMA foreign_keys=ON;
