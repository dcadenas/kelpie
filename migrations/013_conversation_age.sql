-- When this incarnation's backend-native conversation last started.
--
-- Stamped where reconciliation already refreshes observed_native_session_json,
-- because a rotation of that reference IS a conversation boundary: clear,
-- resume, compaction, fork, or a renew all produce one while the incarnation
-- itself continues.
--
-- Deliberately NOT backfilled from created_at_ms. That column records when the
-- incarnation was bound to a runtime, which stops being the conversation start
-- the first time the conversation rotates. Copying it would make every existing
-- row look measured while reporting a number that is wrong by however long the
-- agent has been running. A conversation whose start Kelpie never observed has
-- an unknown age, and NULL says so.
ALTER TABLE incarnations ADD COLUMN native_session_rotated_at_ms INTEGER;

PRAGMA user_version = 13;
