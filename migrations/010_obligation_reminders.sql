BEGIN;

CREATE TABLE obligation_reminders (
    ask_message_id TEXT PRIMARY KEY REFERENCES obligations(ask_message_id),
    interval_ms INTEGER NOT NULL CHECK (interval_ms > 0),
    next_due_at_ms INTEGER,
    snoozed_until_ms INTEGER,
    disabled_at_ms INTEGER,
    suspended_at_ms INTEGER,
    last_accepted_at_ms INTEGER,
    boundary_check_at_ms INTEGER,
    saw_working_at_ms INTEGER
);

CREATE TABLE reminder_attempts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ask_message_id TEXT NOT NULL REFERENCES obligation_reminders(ask_message_id),
    recipient_incarnation_id TEXT NOT NULL REFERENCES incarnations(id),
    request_id TEXT NOT NULL UNIQUE,
    started_at_ms INTEGER NOT NULL,
    resolved_at_ms INTEGER,
    phase TEXT NOT NULL CHECK (phase IN ('prepared','submitted','accepted','rejected','unknown')),
    evidence_json TEXT
);

CREATE INDEX reminder_attempts_ask_idx
ON reminder_attempts(ask_message_id, started_at_ms);

PRAGMA user_version = 10;
COMMIT;
