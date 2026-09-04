-- Keep failed prompt history while allowing the caller's key to name a new
-- prompt attempt. Non-prompt operations and every other prompt outcome
-- continue to reserve the key globally.
-- foreign_keys must be OFF outside the transaction so DROP of `operations`
-- succeeds while operation_attempts/deliveries still reference it by name.
PRAGMA foreign_keys=OFF;

BEGIN;

CREATE TABLE operations_v27 (
    id TEXT PRIMARY KEY,
    idempotency_key TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('start','prompt','resume','retire','notification','adopt','clear')),
    target_incarnation_id TEXT NOT NULL REFERENCES incarnations(id),
    intent_json TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL,
    resolved_at_ms INTEGER,
    outcome TEXT NOT NULL CHECK (outcome IN ('pending','accepted','succeeded','failed','superseded','unknown'))
);

INSERT INTO operations_v27 (
    id, idempotency_key, kind, target_incarnation_id, intent_json,
    created_at_ms, resolved_at_ms, outcome
)
SELECT
    id, idempotency_key, kind, target_incarnation_id, intent_json,
    created_at_ms, resolved_at_ms, outcome
FROM operations;

DROP TABLE operations;
ALTER TABLE operations_v27 RENAME TO operations;

CREATE UNIQUE INDEX operations_live_idempotency_key
    ON operations(idempotency_key)
    WHERE kind != 'prompt' OR outcome != 'failed';
CREATE INDEX operations_target_kind_outcome
    ON operations(target_incarnation_id, kind, outcome);

PRAGMA user_version = 27;
COMMIT;

PRAGMA foreign_keys=ON;
