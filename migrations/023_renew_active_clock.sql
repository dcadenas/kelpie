-- `--every` accumulates only while Herdr observes working or blocked occupancy.
-- Remaining active time is durable so idle, done, and unobserved gaps cannot
-- exhaust a cycle, including time kelpied was down. One-shot renews keep a
-- wall-clock due time and leave these columns NULL.
BEGIN;

ALTER TABLE renews ADD COLUMN active_remaining_ms INTEGER
    CHECK (active_remaining_ms IS NULL OR active_remaining_ms >= 0);
ALTER TABLE renews ADD COLUMN occupancy_sampled_at_ms INTEGER;

-- Fail closed on upgrade: there is no occupancy history, so a scheduled policy
-- must earn a full interval of observed active time before it may enter
-- Preparing. An overdue wall-clock due time is not proof the agent was working.
UPDATE renews
   SET active_remaining_ms = every_ms,
       occupancy_sampled_at_ms = NULL
 WHERE every_ms IS NOT NULL
   AND phase = 'scheduled';

PRAGMA user_version = 23;
COMMIT;
