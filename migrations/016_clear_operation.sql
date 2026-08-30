-- Give standalone backend clears their own durable operation kind.
-- foreign_keys must be OFF outside the transaction so DROP of `operations`
-- succeeds while operation_attempts/deliveries still reference it by name.
PRAGMA foreign_keys=OFF;

BEGIN;

CREATE TABLE operations_v16 (
    id TEXT PRIMARY KEY,
    idempotency_key TEXT NOT NULL UNIQUE,
    kind TEXT NOT NULL CHECK (kind IN ('start','prompt','resume','retire','notification','adopt','clear')),
    target_incarnation_id TEXT NOT NULL REFERENCES incarnations(id),
    intent_json TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL,
    resolved_at_ms INTEGER,
    outcome TEXT NOT NULL CHECK (outcome IN ('pending','accepted','succeeded','failed','superseded','unknown'))
);

INSERT INTO operations_v16 (
    id, idempotency_key, kind, target_incarnation_id, intent_json,
    created_at_ms, resolved_at_ms, outcome
)
SELECT
    id, idempotency_key, kind, target_incarnation_id, intent_json,
    created_at_ms, resolved_at_ms, outcome
FROM operations;

DROP TABLE operations;
ALTER TABLE operations_v16 RENAME TO operations;

PRAGMA user_version = 16;
COMMIT;

PRAGMA foreign_keys=ON;
