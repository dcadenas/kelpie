BEGIN;

CREATE TABLE IF NOT EXISTS logical_agents (
    id TEXT PRIMARY KEY,
    public_name TEXT NOT NULL,
    parent_agent_id TEXT REFERENCES logical_agents(id),
    explicitly_parentless INTEGER NOT NULL CHECK (explicitly_parentless IN (0, 1)),
    created_at_ms INTEGER NOT NULL,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    CHECK ((parent_agent_id IS NULL) = (explicitly_parentless = 1))
);

CREATE TABLE IF NOT EXISTS incarnations (
    id TEXT PRIMARY KEY,
    logical_agent_id TEXT NOT NULL REFERENCES logical_agents(id),
    herdr_session TEXT NOT NULL,
    intended_pane_id TEXT NOT NULL,
    expected_terminal_id TEXT NOT NULL,
    observed_pane_id TEXT,
    observed_terminal_id TEXT,
    backend_kind TEXT NOT NULL,
    backend_args_json TEXT NOT NULL,
    working_directory TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL,
    terminal_at_ms INTEGER,
    terminal_reason TEXT,
    state TEXT NOT NULL CHECK (state IN ('declared','starting','ready','failed','unknown','retiring','retired','lost','superseded'))
);

CREATE TABLE IF NOT EXISTS operations (
    id TEXT PRIMARY KEY,
    idempotency_key TEXT NOT NULL UNIQUE,
    kind TEXT NOT NULL CHECK (kind IN ('start','prompt','resume','retire','notification')),
    target_incarnation_id TEXT NOT NULL REFERENCES incarnations(id),
    intent_json TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL,
    resolved_at_ms INTEGER,
    outcome TEXT NOT NULL CHECK (outcome IN ('pending','accepted','succeeded','failed','superseded','unknown'))
);

CREATE TABLE IF NOT EXISTS operation_attempts (
    operation_id TEXT NOT NULL REFERENCES operations(id),
    attempt_number INTEGER NOT NULL,
    request_id TEXT NOT NULL,
    started_at_ms INTEGER NOT NULL,
    resolved_at_ms INTEGER,
    phase TEXT NOT NULL CHECK (phase IN ('prepared','submitted','accepted','response_committed','rejected','unknown')),
    evidence_json TEXT,
    PRIMARY KEY (operation_id, attempt_number)
);

CREATE TABLE IF NOT EXISTS messages (
    id TEXT PRIMARY KEY,
    sender_agent_id TEXT NOT NULL REFERENCES logical_agents(id),
    recipient_agent_id TEXT NOT NULL REFERENCES logical_agents(id),
    kind TEXT NOT NULL CHECK (kind IN ('tell','ask','reply')),
    body TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL,
    reply_to_message_id TEXT REFERENCES messages(id),
    disposition TEXT CHECK (disposition IN ('progress','final')),
    creates_obligation INTEGER NOT NULL CHECK (creates_obligation IN (0, 1)),
    CHECK ((kind = 'reply') = (reply_to_message_id IS NOT NULL)),
    CHECK ((kind = 'reply') = (disposition IS NOT NULL))
);

CREATE TABLE IF NOT EXISTS obligations (
    ask_message_id TEXT PRIMARY KEY REFERENCES messages(id),
    owing_agent_id TEXT NOT NULL REFERENCES logical_agents(id),
    waiting_agent_id TEXT NOT NULL REFERENCES logical_agents(id),
    created_at_ms INTEGER NOT NULL,
    last_activity_at_ms INTEGER NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('open','in_progress','resolved','cancelled','orphaned')),
    resolving_message_id TEXT REFERENCES messages(id)
);

CREATE TABLE IF NOT EXISTS deliveries (
    message_id TEXT NOT NULL REFERENCES messages(id),
    recipient_incarnation_id TEXT NOT NULL REFERENCES incarnations(id),
    attempt_number INTEGER NOT NULL,
    scheduled_at_ms INTEGER NOT NULL,
    attempted_at_ms INTEGER,
    resolved_at_ms INTEGER,
    herdr_request_id TEXT,
    outcome TEXT NOT NULL CHECK (outcome IN ('pending','submitted','accepted','queued','unknown','rejected','target_unavailable','superseded')),
    operation_id TEXT NOT NULL REFERENCES operations(id),
    PRIMARY KEY (message_id, recipient_incarnation_id, attempt_number)
);

PRAGMA user_version = 1;
COMMIT;
