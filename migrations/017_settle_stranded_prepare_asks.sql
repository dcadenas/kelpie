-- Settle prepare asks left open by renew cycles that have already ended.
--
-- Terminal renew transitions now cancel an unanswered prepare ask in the same
-- transaction. Cycles that ended before that did not, so their obligations are
-- still open, still reminding, and waiting on a checkpoint nobody will confirm.
-- Nothing re-runs a terminal transition, so this is the only pass that reaches
-- them.
--
-- Cancelled, not resolved: no reply was ever delivered, and the reason records
-- why the obligation ends here rather than pretending it was answered.
BEGIN;

UPDATE obligations
SET state = 'cancelled',
    last_activity_at_ms = (
        SELECT COALESCE(r.resolved_at_ms, r.created_at_ms)
        FROM renews r WHERE r.ask_message_id = obligations.ask_message_id
    ),
    cancellation_requester_agent_id = (
        SELECT r.requester_agent_id
        FROM renews r WHERE r.ask_message_id = obligations.ask_message_id
    ),
    cancellation_reason = (
        SELECT 'renew cycle ' || r.cycle || ' ended in ' || r.phase
               || ' with the prepare ask unanswered'
        FROM renews r WHERE r.ask_message_id = obligations.ask_message_id
    )
WHERE obligations.state IN ('open', 'in_progress')
  AND EXISTS (
      SELECT 1 FROM renews r
      WHERE r.ask_message_id = obligations.ask_message_id
        AND r.phase IN ('done', 'aborted', 'terminated')
  );

PRAGMA user_version = 17;
COMMIT;
