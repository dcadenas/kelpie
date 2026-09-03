-- Reminder eligibility excludes asks with an in-flight final reply. Without
-- these indexes SQLite scans every delivery once per due reminder.
BEGIN;

CREATE INDEX messages_reply_kind_disposition
    ON messages(reply_to_message_id, kind, disposition);
CREATE INDEX deliveries_message_outcome
    ON deliveries(message_id, outcome);

PRAGMA user_version = 25;
COMMIT;
