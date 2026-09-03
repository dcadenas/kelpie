-- A renew bounds one incarnation's backend-native context by clearing it and
-- re-seeding it. Like reminders, and unlike tell/ask/reply, its injections are
-- not messages between agents, so they get their own attempt records rather
-- than a new messages.kind. The prepare phase *is* an ask, so its obligation,
-- reminders and cancellation are the ask's own and are not duplicated here.
BEGIN;

CREATE TABLE renews (
    id TEXT PRIMARY KEY,
    logical_agent_id TEXT NOT NULL REFERENCES logical_agents(id),
    incarnation_id TEXT NOT NULL REFERENCES incarnations(id),
    requester_agent_id TEXT NOT NULL REFERENCES logical_agents(id),
    -- Both prompts are durable at insert, before any Herdr write. This is what
    -- lets recovery finish a renew that was interrupted after the clear.
    prepare_prompt TEXT NOT NULL,
    resume_prompt TEXT NOT NULL,
    on_timeout TEXT NOT NULL CHECK (on_timeout IN ('abort','proceed')),
    prepare_timeout_ms INTEGER NOT NULL CHECK (prepare_timeout_ms > 0),
    -- NULL means one-shot. Set means the policy re-arms after each injection
    -- and ends only when the incarnation stops being Ready.
    every_ms INTEGER CHECK (every_ms IS NULL OR every_ms > 0),
    cycle INTEGER NOT NULL CHECK (cycle >= 1),
    scheduled_at_ms INTEGER NOT NULL,
    phase TEXT NOT NULL CHECK (phase IN (
        'scheduled','preparing','ready','clearing','injected',
        'done','timed_out','aborted','terminated'
    )),
    ask_message_id TEXT REFERENCES messages(id),
    prepare_deadline_ms INTEGER,
    -- The backend-native session reference observed immediately before the
    -- clear. Clear completion is proven by observing a value different from
    -- this one, never by elapsed time or idle state.
    pre_clear_session_json TEXT,
    created_at_ms INTEGER NOT NULL,
    resolved_at_ms INTEGER,
    termination_reason TEXT,
    -- A renew that has reached the clear must carry the evidence needed to
    -- prove the clear landed and to finish the injection after a crash.
    CHECK (phase NOT IN ('clearing','injected','done')
           OR pre_clear_session_json IS NOT NULL),
    -- Preparing onward is entered by delivering the prepare ask, so the ask it
    -- is waiting on must be recorded.
    CHECK (phase IN ('scheduled','terminated') OR ask_message_id IS NOT NULL)
);

-- One incarnation cannot be renewed by two rules at once; a second would clear
-- a context the first is still preparing.
CREATE UNIQUE INDEX renews_one_active_per_incarnation
ON renews(incarnation_id)
WHERE phase NOT IN ('done','aborted','terminated');

CREATE INDEX renews_due_idx ON renews(phase, scheduled_at_ms);

CREATE TABLE renew_attempts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    renew_id TEXT NOT NULL REFERENCES renews(id),
    incarnation_id TEXT NOT NULL REFERENCES incarnations(id),
    step TEXT NOT NULL CHECK (step IN ('clear','inject')),
    request_id TEXT NOT NULL UNIQUE,
    started_at_ms INTEGER NOT NULL,
    resolved_at_ms INTEGER,
    phase TEXT NOT NULL CHECK (phase IN ('prepared','submitted','accepted','rejected','unknown')),
    evidence_json TEXT
);

CREATE INDEX renew_attempts_renew_idx
ON renew_attempts(renew_id, started_at_ms);

PRAGMA user_version = 12;
COMMIT;
