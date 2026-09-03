-- A cancellation is delivered like a reply: Kelpie authors a
-- 'cancellation' message, attributed to no agent, and delivers it into the
-- asker's Ready pane when one exists. Rebuild the messages table to widen the
-- kind CHECK; the 003 rebuild is the pattern. No other table changes: a
-- cancellation delivery reuses the existing prompt operation kind, and the
-- obligation row already records the requester and reason.
PRAGMA foreign_keys = OFF;
BEGIN;

CREATE TABLE messages_new (
    id TEXT PRIMARY KEY,
    sender_agent_id TEXT REFERENCES logical_agents(id),
    recipient_agent_id TEXT NOT NULL REFERENCES logical_agents(id),
    kind TEXT NOT NULL CHECK (kind IN ('tell','ask','reply','cancellation')),
    body TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL,
    reply_to_message_id TEXT REFERENCES messages_new(id),
    disposition TEXT CHECK (disposition IN ('progress','final')),
    creates_obligation INTEGER NOT NULL CHECK (creates_obligation IN (0, 1)),
    CHECK ((kind = 'reply') = (reply_to_message_id IS NOT NULL)),
    CHECK ((kind = 'reply') = (disposition IS NOT NULL))
);

INSERT INTO messages_new
SELECT id, sender_agent_id, recipient_agent_id, kind, body, created_at_ms,
       reply_to_message_id, disposition, creates_obligation
FROM messages;

DROP TABLE messages;
ALTER TABLE messages_new RENAME TO messages;

PRAGMA user_version = 18;
COMMIT;
PRAGMA foreign_keys = ON;
