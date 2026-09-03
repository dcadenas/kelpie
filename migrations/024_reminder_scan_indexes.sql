-- Reminder eligibility joins run on every daemon pass when reminders are due.
-- Index the two correlated lookups so fleet history does not make one pass
-- quadratic in obligations, incarnations, and operations.
BEGIN;

CREATE INDEX incarnations_logical_state
    ON incarnations(logical_agent_id, state);
CREATE INDEX operations_target_kind_outcome
    ON operations(target_incarnation_id, kind, outcome);

PRAGMA user_version = 24;
COMMIT;
