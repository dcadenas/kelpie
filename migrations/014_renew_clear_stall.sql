-- A clear whose rotation is never observed leaves an incarnation wiped and
-- holding no instructions, and nothing inside it can notice.
--
-- The wait itself is not bounded, because abandoning the injection is the worse
-- failure: the context is already gone and the resume prompt is the only thing
-- that can re-seed it. What is bounded is the silence. A renew that
-- has been clearing longer than its deadline raises one operator notice and
-- keeps trying.
--
-- Deliberately one notice per renew, not one per pass. A stall that repeats
-- every scheduler tick trains an operator to ignore the channel that is
-- reporting the only unrecoverable state a renew has.
ALTER TABLE renews ADD COLUMN clear_deadline_ms INTEGER;
ALTER TABLE renews ADD COLUMN clear_stall_notified_at_ms INTEGER;

PRAGMA user_version = 14;
