-- Archive dead logical identities without deleting them.
--
-- logical_agents.archived_at_ms records when the daemon's archive pass found
-- every incarnation terminal, no unresolved obligation, and nothing in flight
-- for longer than the grace. NULL means not archived. Archiving only hides an
-- identity from live-facing name checks and listings; its rows, messages,
-- obligations, and history stay exactly as they were.
--
-- A new incarnation for an archived identity (continuation, `adopt
-- --logical-id`, a start that names it) means it is alive again, so the
-- trigger clears the mark in the same statement that creates the incarnation.
BEGIN;

ALTER TABLE logical_agents ADD COLUMN archived_at_ms INTEGER;

CREATE TRIGGER logical_agents_unarchive_on_incarnation
AFTER INSERT ON incarnations
BEGIN
    UPDATE logical_agents SET archived_at_ms = NULL
    WHERE id = NEW.logical_agent_id AND archived_at_ms IS NOT NULL;
END;

PRAGMA user_version = 33;
COMMIT;
