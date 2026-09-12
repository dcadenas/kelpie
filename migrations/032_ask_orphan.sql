-- Bound obligations that outlive their counterparty, and socket waiters whose
-- claiming connection never comes back.
--
-- obligations.unavailable_since_ms records the first observed moment a party
-- required to finish the ask had no runtime and no lifecycle operation in
-- flight. NULL means present or never observed absent. The daemon sweep
-- orphans an open obligation once that clock passes the grace.
--
-- logical_agents.waiter_last_seen_at_ms records the last server-observed
-- contact with a socket waiter's claiming connection. It is NULL for Herdr
-- agents and for waiters never claimed; such waiters never expire. The daemon
-- boots it forward so a server outage cannot false-retire a client that is
-- about to reconnect.
BEGIN;

ALTER TABLE obligations ADD COLUMN unavailable_since_ms INTEGER;
ALTER TABLE logical_agents ADD COLUMN waiter_last_seen_at_ms INTEGER;

CREATE INDEX obligations_orphan_sweep
    ON obligations(unavailable_since_ms)
    WHERE state IN ('open', 'in_progress');

CREATE INDEX logical_agents_waiter_seen
    ON logical_agents(waiter_last_seen_at_ms)
    WHERE delivery_transport = 'socket_inbox';

PRAGMA user_version = 32;
COMMIT;
