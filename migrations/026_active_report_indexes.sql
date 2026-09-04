-- Active reports start from the few live states, then prove each candidate is
-- the newest incarnation without scanning or sorting retired fleet history.
BEGIN;

CREATE INDEX incarnations_state_logical_created_id
    ON incarnations(state, logical_agent_id, created_at_ms DESC, id DESC);
CREATE INDEX incarnations_logical_created_id
    ON incarnations(logical_agent_id, created_at_ms DESC, id DESC);

PRAGMA user_version = 26;
COMMIT;
