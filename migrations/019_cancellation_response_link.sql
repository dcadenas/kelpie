-- The cancellation response is linked to its obligation so revival surfacing
-- can tell a delivered response (the pane received it) from a recorded one
-- (the asker was away).
ALTER TABLE obligations
    ADD COLUMN cancellation_response_message_id TEXT REFERENCES messages(id);

PRAGMA user_version = 19;
