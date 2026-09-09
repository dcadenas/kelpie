-- Replace per-table UUID identifiers with compact, daemon-allocated integers.
-- Foreign keys are disabled before the transaction because every ID-bearing
-- table is rebuilt together and old tables must be dropped while referenced.
PRAGMA foreign_keys=OFF;

BEGIN;

CREATE TEMP TABLE logical_agent_id_map (old_id TEXT PRIMARY KEY, new_id INTEGER UNIQUE NOT NULL);
INSERT INTO logical_agent_id_map
SELECT id, ROW_NUMBER() OVER (ORDER BY created_at_ms, id) FROM logical_agents;

CREATE TEMP TABLE incarnation_id_map (old_id TEXT PRIMARY KEY, new_id INTEGER UNIQUE NOT NULL);
INSERT INTO incarnation_id_map
SELECT id, ROW_NUMBER() OVER (ORDER BY created_at_ms, id) FROM incarnations;

CREATE TEMP TABLE operation_id_map (old_id TEXT PRIMARY KEY, new_id INTEGER UNIQUE NOT NULL);
INSERT INTO operation_id_map
SELECT id, ROW_NUMBER() OVER (ORDER BY created_at_ms, id) FROM operations;

CREATE TEMP TABLE message_id_map (old_id TEXT PRIMARY KEY, new_id INTEGER UNIQUE NOT NULL);
INSERT INTO message_id_map
SELECT id, ROW_NUMBER() OVER (ORDER BY created_at_ms, id) FROM messages;

CREATE TEMP TABLE notice_id_map (old_id TEXT PRIMARY KEY, new_id INTEGER UNIQUE NOT NULL);
INSERT INTO notice_id_map
SELECT id, ROW_NUMBER() OVER (ORDER BY created_at_ms, id) FROM operator_notices;

CREATE TEMP TABLE renew_id_map (old_id TEXT PRIMARY KEY, new_id INTEGER UNIQUE NOT NULL);
INSERT INTO renew_id_map
SELECT id, ROW_NUMBER() OVER (ORDER BY created_at_ms, id) FROM renews;

CREATE TEMP TABLE schedule_id_map (old_id TEXT PRIMARY KEY, new_id INTEGER UNIQUE NOT NULL);
INSERT INTO schedule_id_map
SELECT id, ROW_NUMBER() OVER (ORDER BY created_at_ms, id) FROM schedules;

CREATE TEMP TABLE integer_id_migration_guard (
    ok INTEGER NOT NULL CHECK (ok = 1)
);
INSERT INTO integer_id_migration_guard
SELECT CASE WHEN EXISTS(SELECT 1 FROM pragma_foreign_key_check) THEN 0 ELSE 1 END;
INSERT INTO integer_id_migration_guard
SELECT CASE WHEN EXISTS (
    SELECT 1 FROM operations o WHERE
        (json_type(o.intent_json, '$.logical_agent_id') NOT IN ('null')
         AND NOT EXISTS (SELECT 1 FROM logical_agent_id_map m
                         WHERE m.old_id = json_extract(o.intent_json, '$.logical_agent_id')))
     OR (json_type(o.intent_json, '$.parent.agent_id') NOT IN ('null')
         AND NOT EXISTS (SELECT 1 FROM logical_agent_id_map m
                         WHERE m.old_id = json_extract(o.intent_json, '$.parent.agent_id')))
     OR (json_type(o.intent_json, '$.initial_message.sender') NOT IN ('null')
         AND NOT EXISTS (SELECT 1 FROM logical_agent_id_map m
                         WHERE m.old_id = json_extract(o.intent_json, '$.initial_message.sender')))
     OR (json_type(o.intent_json, '$.supersedes') NOT IN ('null')
         AND NOT EXISTS (SELECT 1 FROM incarnation_id_map m
                         WHERE m.old_id = json_extract(o.intent_json, '$.supersedes')))
     OR (json_type(o.intent_json, '$.adopt.logical_agent_id') NOT IN ('null')
         AND NOT EXISTS (SELECT 1 FROM logical_agent_id_map m
                         WHERE m.old_id = json_extract(o.intent_json, '$.adopt.logical_agent_id')))
     OR (json_type(o.intent_json, '$.adopt.parent.agent_id') NOT IN ('null')
         AND NOT EXISTS (SELECT 1 FROM logical_agent_id_map m
                         WHERE m.old_id = json_extract(o.intent_json, '$.adopt.parent.agent_id')))
     OR (json_type(o.intent_json, '$.message_id') NOT IN ('null')
         AND NOT EXISTS (SELECT 1 FROM message_id_map m
                         WHERE m.old_id = json_extract(o.intent_json, '$.message_id')))
     OR (json_type(o.intent_json, '$.reply_to') NOT IN ('null')
         AND NOT EXISTS (SELECT 1 FROM message_id_map m
                         WHERE m.old_id = json_extract(o.intent_json, '$.reply_to')))
     OR (json_type(o.intent_json, '$.cancelled_ask') NOT IN ('null')
         AND NOT EXISTS (SELECT 1 FROM message_id_map m
                         WHERE m.old_id = json_extract(o.intent_json, '$.cancelled_ask')))
     OR (json_type(o.intent_json, '$.recipient_incarnation_id') NOT IN ('null')
         AND NOT EXISTS (SELECT 1 FROM incarnation_id_map m
                         WHERE m.old_id = json_extract(o.intent_json, '$.recipient_incarnation_id')))
     OR (json_type(o.intent_json, '$.incarnation_id') NOT IN ('null')
         AND NOT EXISTS (SELECT 1 FROM incarnation_id_map m
                         WHERE m.old_id = json_extract(o.intent_json, '$.incarnation_id')))
     OR (o.kind = 'clear' AND json_type(o.intent_json, '$.recipient') NOT IN ('null')
         AND NOT EXISTS (SELECT 1 FROM logical_agent_id_map m
                         WHERE m.old_id = json_extract(o.intent_json, '$.recipient')))
     OR (json_type(o.intent_json, '$.schedule_id') NOT IN ('null')
         AND NOT EXISTS (SELECT 1 FROM schedule_id_map m
                         WHERE m.old_id = json_extract(o.intent_json, '$.schedule_id')))
) THEN 0 ELSE 1 END;

CREATE TABLE logical_agents_v30 (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    public_name TEXT NOT NULL,
    parent_agent_id INTEGER REFERENCES logical_agents(id),
    explicitly_parentless INTEGER NOT NULL CHECK (explicitly_parentless IN (0, 1)),
    created_at_ms INTEGER NOT NULL,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    delivery_transport TEXT NOT NULL DEFAULT 'herdr_prompt'
        CHECK (delivery_transport IN ('herdr_prompt', 'socket_inbox')),
    targeting_ended_at_ms INTEGER,
    CHECK ((parent_agent_id IS NULL) = (explicitly_parentless = 1))
);

CREATE TABLE incarnations_v30 (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    logical_agent_id INTEGER NOT NULL REFERENCES logical_agents(id),
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
    state TEXT NOT NULL CHECK (state IN ('declared','starting','ready','failed','unknown','retiring','retired','lost','superseded')),
    name_authority TEXT NOT NULL DEFAULT 'observed'
        CHECK (name_authority IN ('observed', 'synthesized')),
    observed_native_session_json TEXT,
    requested_model TEXT,
    requested_provider TEXT,
    requested_effort TEXT,
    pending_rename_to TEXT,
    native_session_rotated_at_ms INTEGER
);

CREATE TABLE operations_v30 (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    idempotency_key TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('start','prompt','resume','retire','notification','adopt','clear')),
    target_incarnation_id INTEGER NOT NULL REFERENCES incarnations(id),
    intent_json TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL,
    resolved_at_ms INTEGER,
    outcome TEXT NOT NULL CHECK (outcome IN ('pending','accepted','succeeded','failed','superseded','unknown'))
);

CREATE TABLE operation_attempts_v30 (
    operation_id INTEGER NOT NULL REFERENCES operations(id),
    attempt_number INTEGER NOT NULL,
    request_id TEXT NOT NULL,
    started_at_ms INTEGER NOT NULL,
    resolved_at_ms INTEGER,
    phase TEXT NOT NULL CHECK (phase IN ('prepared','submitted','accepted','response_committed','rejected','unknown')),
    evidence_json TEXT,
    PRIMARY KEY (operation_id, attempt_number)
);

CREATE TABLE operator_notices_v30 (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    body TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL,
    acknowledged_at_ms INTEGER
);

CREATE TABLE messages_v30 (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sender_agent_id INTEGER REFERENCES logical_agents(id),
    recipient_agent_id INTEGER NOT NULL REFERENCES logical_agents(id),
    kind TEXT NOT NULL CHECK (kind IN ('tell','ask','reply','cancellation')),
    body TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL,
    reply_to_message_id INTEGER REFERENCES messages(id),
    disposition TEXT CHECK (disposition IN ('progress','final')),
    creates_obligation INTEGER NOT NULL CHECK (creates_obligation IN (0, 1)),
    CHECK ((kind = 'reply') = (reply_to_message_id IS NOT NULL)),
    CHECK ((kind = 'reply') = (disposition IS NOT NULL))
);

CREATE TABLE obligations_v30 (
    ask_message_id INTEGER PRIMARY KEY REFERENCES messages(id),
    owing_agent_id INTEGER NOT NULL REFERENCES logical_agents(id),
    waiting_agent_id INTEGER NOT NULL REFERENCES logical_agents(id),
    creation_sequence INTEGER NOT NULL UNIQUE,
    created_at_ms INTEGER NOT NULL,
    last_activity_at_ms INTEGER NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('open','in_progress','resolved','cancelled','orphaned')),
    resolving_message_id INTEGER REFERENCES messages(id),
    cancellation_requester_agent_id INTEGER REFERENCES logical_agents(id),
    cancellation_reason TEXT CHECK (cancellation_reason IS NULL OR length(trim(cancellation_reason)) > 0),
    cancellation_response_message_id INTEGER REFERENCES messages(id),
    cancellation_owing_message_id INTEGER REFERENCES messages(id)
);

CREATE TABLE deliveries_v30 (
    message_id INTEGER NOT NULL REFERENCES messages(id),
    delivery_transport TEXT NOT NULL DEFAULT 'herdr_prompt'
        CHECK (delivery_transport IN ('herdr_prompt', 'socket_inbox')),
    recipient_incarnation_id INTEGER REFERENCES incarnations(id),
    recipient_agent_id INTEGER REFERENCES logical_agents(id),
    attempt_number INTEGER NOT NULL,
    scheduled_at_ms INTEGER NOT NULL,
    attempted_at_ms INTEGER,
    resolved_at_ms INTEGER,
    herdr_request_id TEXT,
    outcome TEXT NOT NULL CHECK (outcome IN ('pending','submitted','accepted','queued','unknown','rejected','target_unavailable','superseded')),
    operation_id INTEGER REFERENCES operations(id),
    cancellation_requester_agent_id INTEGER REFERENCES logical_agents(id),
    cancellation_reason TEXT,
    cancelled_at_ms INTEGER,
    CHECK ((delivery_transport = 'herdr_prompt'
            AND recipient_incarnation_id IS NOT NULL AND operation_id IS NOT NULL)
        OR (delivery_transport = 'socket_inbox'
            AND recipient_agent_id IS NOT NULL AND recipient_incarnation_id IS NULL
            AND herdr_request_id IS NULL))
);

CREATE TABLE observed_attributions_v30 (
    id INTEGER PRIMARY KEY,
    incarnation_id INTEGER NOT NULL REFERENCES incarnations(id),
    recorded_at_ms INTEGER NOT NULL,
    adapter TEXT NOT NULL,
    native_session_json TEXT,
    model_status TEXT NOT NULL CHECK (model_status IN ('undetermined', 'reported')),
    model_value TEXT,
    provider_status TEXT NOT NULL CHECK (provider_status IN ('undetermined', 'reported')),
    provider_value TEXT,
    effort_status TEXT NOT NULL CHECK (effort_status IN ('undetermined', 'reported')),
    effort_value TEXT,
    CHECK ((model_status = 'reported') = (model_value IS NOT NULL)),
    CHECK ((provider_status = 'reported') = (provider_value IS NOT NULL)),
    CHECK ((effort_status = 'reported') = (effort_value IS NOT NULL))
);

CREATE TABLE obligation_reminders_v30 (
    ask_message_id INTEGER PRIMARY KEY REFERENCES obligations(ask_message_id),
    interval_ms INTEGER NOT NULL CHECK (interval_ms > 0),
    next_due_at_ms INTEGER,
    snoozed_until_ms INTEGER,
    disabled_at_ms INTEGER,
    suspended_at_ms INTEGER,
    last_accepted_at_ms INTEGER,
    boundary_check_at_ms INTEGER,
    saw_working_at_ms INTEGER
);

CREATE TABLE reminder_attempts_v30 (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ask_message_id INTEGER NOT NULL REFERENCES obligation_reminders(ask_message_id),
    recipient_incarnation_id INTEGER NOT NULL REFERENCES incarnations(id),
    request_id TEXT NOT NULL UNIQUE,
    started_at_ms INTEGER NOT NULL,
    resolved_at_ms INTEGER,
    phase TEXT NOT NULL CHECK (phase IN ('prepared','submitted','accepted','rejected','unknown')),
    evidence_json TEXT
);

CREATE TABLE schedules_v30 (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    kind TEXT NOT NULL CHECK (kind IN ('tell','renew')),
    logical_agent_id INTEGER NOT NULL REFERENCES logical_agents(id),
    incarnation_id INTEGER REFERENCES incarnations(id),
    requester_agent_id INTEGER NOT NULL REFERENCES logical_agents(id),
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

CREATE TABLE renews_v30 (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    logical_agent_id INTEGER NOT NULL REFERENCES logical_agents(id),
    incarnation_id INTEGER NOT NULL REFERENCES incarnations(id),
    requester_agent_id INTEGER NOT NULL REFERENCES logical_agents(id),
    prepare_prompt TEXT NOT NULL,
    resume_prompt TEXT NOT NULL,
    on_timeout TEXT NOT NULL CHECK (on_timeout IN ('abort','proceed')),
    prepare_timeout_ms INTEGER NOT NULL CHECK (prepare_timeout_ms > 0),
    every_ms INTEGER CHECK (every_ms IS NULL OR every_ms > 0),
    cycle INTEGER NOT NULL CHECK (cycle >= 1),
    scheduled_at_ms INTEGER NOT NULL,
    phase TEXT NOT NULL CHECK (phase IN ('scheduled','preparing','ready','clearing','injected','done','timed_out','aborted','terminated')),
    ask_message_id INTEGER REFERENCES messages(id),
    prepare_deadline_ms INTEGER,
    pre_clear_session_json TEXT,
    created_at_ms INTEGER NOT NULL,
    resolved_at_ms INTEGER,
    termination_reason TEXT,
    clear_deadline_ms INTEGER,
    clear_stall_notified_at_ms INTEGER,
    inject_not_before_ms INTEGER,
    active_remaining_ms INTEGER CHECK (active_remaining_ms IS NULL OR active_remaining_ms >= 0),
    occupancy_sampled_at_ms INTEGER,
    schedule_id INTEGER REFERENCES schedules(id),
    CHECK (phase NOT IN ('clearing','injected','done') OR pre_clear_session_json IS NOT NULL),
    CHECK (phase IN ('scheduled','terminated') OR ask_message_id IS NOT NULL)
);

CREATE TABLE renew_attempts_v30 (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    renew_id INTEGER NOT NULL REFERENCES renews(id),
    incarnation_id INTEGER NOT NULL REFERENCES incarnations(id),
    step TEXT NOT NULL CHECK (step IN ('clear','inject')),
    request_id TEXT NOT NULL UNIQUE,
    started_at_ms INTEGER NOT NULL,
    resolved_at_ms INTEGER,
    phase TEXT NOT NULL CHECK (phase IN ('prepared','submitted','accepted','rejected','unknown')),
    evidence_json TEXT
);

CREATE TABLE socket_waiter_keys_v30 (
    idempotency_key TEXT PRIMARY KEY,
    logical_agent_id INTEGER NOT NULL REFERENCES logical_agents(id)
);

CREATE TABLE socket_inbox_keys_v30 (
    idempotency_key TEXT PRIMARY KEY,
    message_id INTEGER NOT NULL REFERENCES messages(id)
);

CREATE TABLE schedule_firings_v30 (
    schedule_id INTEGER NOT NULL REFERENCES schedules(id),
    cycle INTEGER NOT NULL CHECK (cycle >= 1),
    due_at_ms INTEGER NOT NULL,
    fired_at_ms INTEGER NOT NULL,
    outcome TEXT NOT NULL CHECK (outcome IN ('materialized','target_unavailable','skipped')),
    message_id INTEGER REFERENCES messages(id),
    renew_id INTEGER REFERENCES renews(id),
    detail TEXT,
    PRIMARY KEY (schedule_id, cycle),
    CHECK (message_id IS NULL OR renew_id IS NULL)
);

INSERT INTO logical_agents_v30
SELECT idm.new_id, l.public_name, parent.new_id, l.explicitly_parentless,
       l.created_at_ms, l.metadata_json, l.delivery_transport, l.targeting_ended_at_ms
FROM logical_agents l
JOIN logical_agent_id_map idm ON idm.old_id = l.id
LEFT JOIN logical_agent_id_map parent ON parent.old_id = l.parent_agent_id;

INSERT INTO incarnations_v30
SELECT idm.new_id, agent.new_id, i.herdr_session, i.intended_pane_id,
       i.expected_terminal_id, i.observed_pane_id, i.observed_terminal_id,
       i.backend_kind, i.backend_args_json, i.working_directory, i.created_at_ms,
       i.terminal_at_ms, i.terminal_reason, i.state, i.name_authority,
       i.observed_native_session_json, i.requested_model, i.requested_provider,
       i.requested_effort, i.pending_rename_to, i.native_session_rotated_at_ms
FROM incarnations i
JOIN incarnation_id_map idm ON idm.old_id = i.id
JOIN logical_agent_id_map agent ON agent.old_id = i.logical_agent_id;

INSERT INTO operations_v30
SELECT idm.new_id, o.idempotency_key, o.kind, incarnation.new_id, o.intent_json,
       o.created_at_ms, o.resolved_at_ms, o.outcome
FROM operations o
JOIN operation_id_map idm ON idm.old_id = o.id
JOIN incarnation_id_map incarnation ON incarnation.old_id = o.target_incarnation_id;

-- Operation intents are typed durable records used by recovery. Rewrite only
-- known ID fields, leaving request/idempotency keys and untrusted bodies intact.
UPDATE operations_v30 SET intent_json = json_set(intent_json, '$.logical_agent_id',
    (SELECT new_id FROM logical_agent_id_map WHERE old_id = json_extract(intent_json, '$.logical_agent_id')))
WHERE json_type(intent_json, '$.logical_agent_id') IS NOT NULL;
UPDATE operations_v30 SET intent_json = json_set(intent_json, '$.parent.agent_id',
    (SELECT new_id FROM logical_agent_id_map WHERE old_id = json_extract(intent_json, '$.parent.agent_id')))
WHERE json_type(intent_json, '$.parent.agent_id') IS NOT NULL;
UPDATE operations_v30 SET intent_json = json_set(intent_json, '$.initial_message.sender',
    (SELECT new_id FROM logical_agent_id_map WHERE old_id = json_extract(intent_json, '$.initial_message.sender')))
WHERE json_type(intent_json, '$.initial_message.sender') IS NOT NULL;
UPDATE operations_v30 SET intent_json = json_set(intent_json, '$.supersedes',
    (SELECT new_id FROM incarnation_id_map WHERE old_id = json_extract(intent_json, '$.supersedes')))
WHERE json_type(intent_json, '$.supersedes') IS NOT NULL;
UPDATE operations_v30 SET intent_json = json_set(intent_json, '$.adopt.logical_agent_id',
    (SELECT new_id FROM logical_agent_id_map WHERE old_id = json_extract(intent_json, '$.adopt.logical_agent_id')))
WHERE json_type(intent_json, '$.adopt.logical_agent_id') IS NOT NULL;
UPDATE operations_v30 SET intent_json = json_set(intent_json, '$.adopt.parent.agent_id',
    (SELECT new_id FROM logical_agent_id_map WHERE old_id = json_extract(intent_json, '$.adopt.parent.agent_id')))
WHERE json_type(intent_json, '$.adopt.parent.agent_id') IS NOT NULL;
UPDATE operations_v30 SET intent_json = json_set(intent_json, '$.message_id',
    (SELECT new_id FROM message_id_map WHERE old_id = json_extract(intent_json, '$.message_id')))
WHERE json_type(intent_json, '$.message_id') IS NOT NULL;
UPDATE operations_v30 SET intent_json = json_set(intent_json, '$.reply_to',
    (SELECT new_id FROM message_id_map WHERE old_id = json_extract(intent_json, '$.reply_to')))
WHERE json_type(intent_json, '$.reply_to') IS NOT NULL;
UPDATE operations_v30 SET intent_json = json_set(intent_json, '$.cancelled_ask',
    (SELECT new_id FROM message_id_map WHERE old_id = json_extract(intent_json, '$.cancelled_ask')))
WHERE json_type(intent_json, '$.cancelled_ask') IS NOT NULL;
UPDATE operations_v30 SET intent_json = json_set(intent_json, '$.recipient_incarnation_id',
    (SELECT new_id FROM incarnation_id_map WHERE old_id = json_extract(intent_json, '$.recipient_incarnation_id')))
WHERE json_type(intent_json, '$.recipient_incarnation_id') IS NOT NULL;
UPDATE operations_v30 SET intent_json = json_set(intent_json, '$.incarnation_id',
    (SELECT new_id FROM incarnation_id_map WHERE old_id = json_extract(intent_json, '$.incarnation_id')))
WHERE json_type(intent_json, '$.incarnation_id') IS NOT NULL;
UPDATE operations_v30 SET intent_json = json_set(intent_json, '$.recipient',
    (SELECT new_id FROM logical_agent_id_map WHERE old_id = json_extract(intent_json, '$.recipient')))
WHERE kind = 'clear' AND json_type(intent_json, '$.recipient') IS NOT NULL;
UPDATE operations_v30 SET intent_json = json_set(intent_json, '$.schedule_id',
    (SELECT new_id FROM schedule_id_map WHERE old_id = json_extract(intent_json, '$.schedule_id')))
WHERE json_type(intent_json, '$.schedule_id') IS NOT NULL;

INSERT INTO operation_attempts_v30
SELECT operation.new_id, a.attempt_number, a.request_id, a.started_at_ms,
       a.resolved_at_ms, a.phase, a.evidence_json
FROM operation_attempts a
JOIN operation_id_map operation ON operation.old_id = a.operation_id;

INSERT INTO operator_notices_v30
SELECT idm.new_id, n.body, n.created_at_ms, n.acknowledged_at_ms
FROM operator_notices n JOIN notice_id_map idm ON idm.old_id = n.id;

INSERT INTO messages_v30
SELECT idm.new_id, sender.new_id, recipient.new_id, m.kind, m.body,
       m.created_at_ms, reply_to.new_id, m.disposition, m.creates_obligation
FROM messages m
JOIN message_id_map idm ON idm.old_id = m.id
LEFT JOIN logical_agent_id_map sender ON sender.old_id = m.sender_agent_id
JOIN logical_agent_id_map recipient ON recipient.old_id = m.recipient_agent_id
LEFT JOIN message_id_map reply_to ON reply_to.old_id = m.reply_to_message_id;

INSERT INTO obligations_v30
SELECT ask.new_id, owing.new_id, waiting.new_id, o.creation_sequence,
       o.created_at_ms, o.last_activity_at_ms, o.state, resolving.new_id,
       canceller.new_id, o.cancellation_reason, response.new_id, owing_message.new_id
FROM obligations o
JOIN message_id_map ask ON ask.old_id = o.ask_message_id
JOIN logical_agent_id_map owing ON owing.old_id = o.owing_agent_id
JOIN logical_agent_id_map waiting ON waiting.old_id = o.waiting_agent_id
LEFT JOIN message_id_map resolving ON resolving.old_id = o.resolving_message_id
LEFT JOIN logical_agent_id_map canceller ON canceller.old_id = o.cancellation_requester_agent_id
LEFT JOIN message_id_map response ON response.old_id = o.cancellation_response_message_id
LEFT JOIN message_id_map owing_message ON owing_message.old_id = o.cancellation_owing_message_id;

INSERT INTO deliveries_v30
SELECT message.new_id, d.delivery_transport, incarnation.new_id, recipient.new_id,
       d.attempt_number, d.scheduled_at_ms, d.attempted_at_ms, d.resolved_at_ms,
       d.herdr_request_id, d.outcome, operation.new_id, canceller.new_id,
       d.cancellation_reason, d.cancelled_at_ms
FROM deliveries d
JOIN message_id_map message ON message.old_id = d.message_id
LEFT JOIN incarnation_id_map incarnation ON incarnation.old_id = d.recipient_incarnation_id
LEFT JOIN logical_agent_id_map recipient ON recipient.old_id = d.recipient_agent_id
LEFT JOIN operation_id_map operation ON operation.old_id = d.operation_id
LEFT JOIN logical_agent_id_map canceller ON canceller.old_id = d.cancellation_requester_agent_id;

INSERT INTO observed_attributions_v30
SELECT a.id, incarnation.new_id, a.recorded_at_ms, a.adapter,
       a.native_session_json, a.model_status, a.model_value, a.provider_status,
       a.provider_value, a.effort_status, a.effort_value
FROM observed_attributions a
JOIN incarnation_id_map incarnation ON incarnation.old_id = a.incarnation_id;

INSERT INTO obligation_reminders_v30
SELECT ask.new_id, r.interval_ms, r.next_due_at_ms, r.snoozed_until_ms,
       r.disabled_at_ms, r.suspended_at_ms, r.last_accepted_at_ms,
       r.boundary_check_at_ms, r.saw_working_at_ms
FROM obligation_reminders r
JOIN message_id_map ask ON ask.old_id = r.ask_message_id;

INSERT INTO reminder_attempts_v30
SELECT r.id, ask.new_id, incarnation.new_id, r.request_id, r.started_at_ms,
       r.resolved_at_ms, r.phase, r.evidence_json
FROM reminder_attempts r
JOIN message_id_map ask ON ask.old_id = r.ask_message_id
JOIN incarnation_id_map incarnation ON incarnation.old_id = r.recipient_incarnation_id;

INSERT INTO schedules_v30
SELECT idm.new_id, s.kind, agent.new_id, incarnation.new_id, requester.new_id,
       s.body, s.interval_ms, s.clock, s.next_fire_at_ms, s.active_remaining_ms,
       s.occupancy_sampled_at_ms, s.cycle, s.state, s.idempotency_key,
       s.created_at_ms, s.resolved_at_ms, s.termination_reason
FROM schedules s
JOIN schedule_id_map idm ON idm.old_id = s.id
JOIN logical_agent_id_map agent ON agent.old_id = s.logical_agent_id
LEFT JOIN incarnation_id_map incarnation ON incarnation.old_id = s.incarnation_id
JOIN logical_agent_id_map requester ON requester.old_id = s.requester_agent_id;

INSERT INTO renews_v30
SELECT idm.new_id, agent.new_id, incarnation.new_id, requester.new_id,
       r.prepare_prompt, r.resume_prompt, r.on_timeout, r.prepare_timeout_ms,
       r.every_ms, r.cycle, r.scheduled_at_ms, r.phase, ask.new_id,
       r.prepare_deadline_ms, r.pre_clear_session_json, r.created_at_ms,
       r.resolved_at_ms, r.termination_reason, r.clear_deadline_ms,
       r.clear_stall_notified_at_ms, r.inject_not_before_ms, r.active_remaining_ms,
       r.occupancy_sampled_at_ms, schedule.new_id
FROM renews r
JOIN renew_id_map idm ON idm.old_id = r.id
JOIN logical_agent_id_map agent ON agent.old_id = r.logical_agent_id
JOIN incarnation_id_map incarnation ON incarnation.old_id = r.incarnation_id
JOIN logical_agent_id_map requester ON requester.old_id = r.requester_agent_id
LEFT JOIN message_id_map ask ON ask.old_id = r.ask_message_id
LEFT JOIN schedule_id_map schedule ON schedule.old_id = r.schedule_id;

INSERT INTO renew_attempts_v30
SELECT a.id, renew.new_id, incarnation.new_id, a.step, a.request_id,
       a.started_at_ms, a.resolved_at_ms, a.phase, a.evidence_json
FROM renew_attempts a
JOIN renew_id_map renew ON renew.old_id = a.renew_id
JOIN incarnation_id_map incarnation ON incarnation.old_id = a.incarnation_id;

INSERT INTO socket_waiter_keys_v30
SELECT k.idempotency_key, agent.new_id
FROM socket_waiter_keys k
JOIN logical_agent_id_map agent ON agent.old_id = k.logical_agent_id;

INSERT INTO socket_inbox_keys_v30
SELECT k.idempotency_key, message.new_id
FROM socket_inbox_keys k
JOIN message_id_map message ON message.old_id = k.message_id;

INSERT INTO schedule_firings_v30
SELECT schedule.new_id, f.cycle, f.due_at_ms, f.fired_at_ms, f.outcome,
       message.new_id, renew.new_id, f.detail
FROM schedule_firings f
JOIN schedule_id_map schedule ON schedule.old_id = f.schedule_id
LEFT JOIN message_id_map message ON message.old_id = f.message_id
LEFT JOIN renew_id_map renew ON renew.old_id = f.renew_id;

DROP TABLE schedule_firings;
DROP TABLE socket_inbox_keys;
DROP TABLE socket_waiter_keys;
DROP TABLE renew_attempts;
DROP TABLE renews;
DROP TABLE schedules;
DROP TABLE reminder_attempts;
DROP TABLE obligation_reminders;
DROP TABLE observed_attributions;
DROP TABLE deliveries;
DROP TABLE obligations;
DROP TABLE messages;
DROP TABLE operator_notices;
DROP TABLE operation_attempts;
DROP TABLE operations;
DROP TABLE incarnations;
DROP TABLE logical_agents;

ALTER TABLE logical_agents_v30 RENAME TO logical_agents;
ALTER TABLE incarnations_v30 RENAME TO incarnations;
ALTER TABLE operations_v30 RENAME TO operations;
ALTER TABLE operation_attempts_v30 RENAME TO operation_attempts;
ALTER TABLE operator_notices_v30 RENAME TO operator_notices;
ALTER TABLE messages_v30 RENAME TO messages;
ALTER TABLE obligations_v30 RENAME TO obligations;
ALTER TABLE deliveries_v30 RENAME TO deliveries;
ALTER TABLE observed_attributions_v30 RENAME TO observed_attributions;
ALTER TABLE obligation_reminders_v30 RENAME TO obligation_reminders;
ALTER TABLE reminder_attempts_v30 RENAME TO reminder_attempts;
ALTER TABLE schedules_v30 RENAME TO schedules;
ALTER TABLE renews_v30 RENAME TO renews;
ALTER TABLE renew_attempts_v30 RENAME TO renew_attempts;
ALTER TABLE socket_waiter_keys_v30 RENAME TO socket_waiter_keys;
ALTER TABLE socket_inbox_keys_v30 RENAME TO socket_inbox_keys;
ALTER TABLE schedule_firings_v30 RENAME TO schedule_firings;

CREATE UNIQUE INDEX operations_live_idempotency_key
    ON operations(idempotency_key) WHERE kind != 'prompt' OR outcome != 'failed';
CREATE INDEX operations_target_kind_outcome
    ON operations(target_incarnation_id, kind, outcome);
CREATE INDEX observed_attributions_incarnation
    ON observed_attributions(incarnation_id, recorded_at_ms);
CREATE INDEX reminder_attempts_ask_idx
    ON reminder_attempts(ask_message_id, started_at_ms);
CREATE UNIQUE INDEX renews_one_active_per_incarnation
    ON renews(incarnation_id) WHERE phase NOT IN ('done','aborted','terminated');
CREATE INDEX renews_due_idx ON renews(phase, scheduled_at_ms);
CREATE INDEX renew_attempts_renew_idx ON renew_attempts(renew_id, started_at_ms);
CREATE UNIQUE INDEX deliveries_herdr_attempt
    ON deliveries(message_id, recipient_incarnation_id, attempt_number)
    WHERE recipient_incarnation_id IS NOT NULL;
CREATE UNIQUE INDEX deliveries_socket_attempt
    ON deliveries(message_id, recipient_agent_id, attempt_number)
    WHERE recipient_agent_id IS NOT NULL;
CREATE INDEX incarnations_logical_state ON incarnations(logical_agent_id, state);
CREATE INDEX messages_reply_kind_disposition
    ON messages(reply_to_message_id, kind, disposition);
CREATE INDEX deliveries_message_outcome ON deliveries(message_id, outcome);
CREATE INDEX incarnations_state_logical_created_id
    ON incarnations(state, logical_agent_id, created_at_ms DESC, id DESC);
CREATE INDEX incarnations_logical_created_id
    ON incarnations(logical_agent_id, created_at_ms DESC, id DESC);
CREATE INDEX schedules_due_idx ON schedules(state, clock, next_fire_at_ms);

DROP TABLE logical_agent_id_map;
DROP TABLE incarnation_id_map;
DROP TABLE operation_id_map;
DROP TABLE message_id_map;
DROP TABLE notice_id_map;
DROP TABLE renew_id_map;
DROP TABLE schedule_id_map;
DROP TABLE integer_id_migration_guard;

PRAGMA user_version = 30;
COMMIT;

PRAGMA foreign_keys=ON;
