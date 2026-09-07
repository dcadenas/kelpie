-- Move enabled five-minute policies once; retain later timing and obligations.
BEGIN;
UPDATE obligation_reminders
   SET interval_ms = 1200000,
       next_due_at_ms = CASE WHEN next_due_at_ms IS NOT NULL THEN
           MAX(next_due_at_ms, COALESCE(snoozed_until_ms, 0),
               CAST(unixepoch('subsec') * 1000 AS INTEGER) + 1200000) END
 WHERE interval_ms = 300000 AND disabled_at_ms IS NULL;
PRAGMA user_version = 29;
COMMIT;
