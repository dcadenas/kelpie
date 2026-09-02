-- A cancellation notice for the owing agent, linked so revival surfacing can
-- tell a delivered stop-notice from a recorded one (the owing agent was away).
ALTER TABLE obligations
    ADD COLUMN cancellation_owing_message_id TEXT REFERENCES messages(id);

PRAGMA user_version = 22;
