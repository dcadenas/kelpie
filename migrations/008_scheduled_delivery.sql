-- Cancellation evidence for one-shot scheduled deliveries cancelled
-- before the first Herdr write. Due time remains deliveries.scheduled_at_ms.
ALTER TABLE deliveries ADD COLUMN cancellation_requester_agent_id TEXT
    REFERENCES logical_agents(id);
ALTER TABLE deliveries ADD COLUMN cancellation_reason TEXT;
ALTER TABLE deliveries ADD COLUMN cancelled_at_ms INTEGER;

PRAGMA user_version = 8;
