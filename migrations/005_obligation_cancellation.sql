BEGIN;

ALTER TABLE obligations
ADD COLUMN cancellation_requester_agent_id TEXT REFERENCES logical_agents(id);

ALTER TABLE obligations
ADD COLUMN cancellation_reason TEXT
CHECK (cancellation_reason IS NULL OR length(trim(cancellation_reason)) > 0);

PRAGMA user_version = 5;
COMMIT;
