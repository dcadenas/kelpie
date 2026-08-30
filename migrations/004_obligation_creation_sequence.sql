PRAGMA foreign_keys = OFF;
BEGIN;

CREATE TABLE obligations_new (
    ask_message_id TEXT PRIMARY KEY REFERENCES messages(id),
    owing_agent_id TEXT NOT NULL REFERENCES logical_agents(id),
    waiting_agent_id TEXT NOT NULL REFERENCES logical_agents(id),
    creation_sequence INTEGER NOT NULL UNIQUE,
    created_at_ms INTEGER NOT NULL,
    last_activity_at_ms INTEGER NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('open','in_progress','resolved','cancelled','orphaned')),
    resolving_message_id TEXT REFERENCES messages(id)
);

INSERT INTO obligations_new (
    ask_message_id, owing_agent_id, waiting_agent_id, creation_sequence,
    created_at_ms, last_activity_at_ms, state, resolving_message_id
)
SELECT
    ask_message_id, owing_agent_id, waiting_agent_id,
    ROW_NUMBER() OVER (ORDER BY created_at_ms, ask_message_id),
    created_at_ms, last_activity_at_ms, state, resolving_message_id
FROM obligations;

DROP TABLE obligations;
ALTER TABLE obligations_new RENAME TO obligations;

PRAGMA user_version = 4;
COMMIT;
PRAGMA foreign_keys = ON;
