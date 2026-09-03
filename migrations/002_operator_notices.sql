BEGIN;

CREATE TABLE operator_notices (
    id TEXT PRIMARY KEY,
    body TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL,
    acknowledged_at_ms INTEGER
);

PRAGMA user_version = 2;
COMMIT;
