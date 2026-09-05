-- Repeating schedules own recurrence independently from the messages or renew
-- cycles they materialize. Tell schedules bind only a logical agent; renew
-- schedules additionally bind the exact incarnation whose context they manage.
BEGIN;

CREATE TABLE schedules (
    id TEXT PRIMARY KEY,
    kind TEXT NOT NULL CHECK (kind IN ('tell','renew')),
    logical_agent_id TEXT NOT NULL REFERENCES logical_agents(id),
    incarnation_id TEXT REFERENCES incarnations(id),
    requester_agent_id TEXT NOT NULL REFERENCES logical_agents(id),
    body TEXT,
    interval_ms INTEGER NOT NULL CHECK (interval_ms > 0),
    clock TEXT NOT NULL CHECK (clock IN ('wall','active')),
    next_fire_at_ms INTEGER NOT NULL,
    active_remaining_ms INTEGER,
    occupancy_sampled_at_ms INTEGER,
    cycle INTEGER NOT NULL CHECK (cycle >= 1),
    state TEXT NOT NULL CHECK (state IN ('active','cancelled','terminated')),
    idempotency_key TEXT UNIQUE,
    created_at_ms INTEGER NOT NULL,
    resolved_at_ms INTEGER,
    termination_reason TEXT,
    CHECK ((kind = 'tell' AND incarnation_id IS NULL AND body IS NOT NULL AND clock = 'wall'
            AND active_remaining_ms IS NULL AND occupancy_sampled_at_ms IS NULL)
        OR (kind = 'renew' AND incarnation_id IS NOT NULL AND body IS NULL AND clock = 'active'
            AND active_remaining_ms IS NOT NULL))
);

CREATE INDEX schedules_due_idx ON schedules(state, clock, next_fire_at_ms);

CREATE TABLE schedule_firings (
    schedule_id TEXT NOT NULL REFERENCES schedules(id),
    cycle INTEGER NOT NULL CHECK (cycle >= 1),
    due_at_ms INTEGER NOT NULL,
    fired_at_ms INTEGER NOT NULL,
    outcome TEXT NOT NULL CHECK (outcome IN ('materialized','target_unavailable','skipped')),
    message_id TEXT REFERENCES messages(id),
    renew_id TEXT REFERENCES renews(id),
    detail TEXT,
    PRIMARY KEY (schedule_id, cycle),
    CHECK (message_id IS NULL OR renew_id IS NULL)
);

ALTER TABLE renews ADD COLUMN schedule_id TEXT REFERENCES schedules(id);

-- A database has at most one non-terminal recurring renew per incarnation.
-- Its renew id is already a UUID and safely becomes the stable schedule id.
INSERT INTO schedules
    (id, kind, logical_agent_id, incarnation_id, requester_agent_id, body,
     interval_ms, clock, next_fire_at_ms, active_remaining_ms,
     occupancy_sampled_at_ms, cycle, state, created_at_ms)
SELECT r.id, 'renew', r.logical_agent_id, r.incarnation_id, r.requester_agent_id,
       NULL, r.every_ms, 'active', r.scheduled_at_ms,
       COALESCE(r.active_remaining_ms, r.every_ms), r.occupancy_sampled_at_ms,
       r.cycle, 'active', r.created_at_ms
  FROM renews r
 WHERE r.every_ms IS NOT NULL
   AND r.phase NOT IN ('done','aborted','terminated');

UPDATE renews SET schedule_id = id
 WHERE id IN (SELECT id FROM schedules WHERE kind = 'renew');

PRAGMA user_version = 28;
COMMIT;
