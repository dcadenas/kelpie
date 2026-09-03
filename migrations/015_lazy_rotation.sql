-- Some backends allocate their replacement conversation on the next prompt
-- rather than on the clear, so the injection is what produces the rotation that
-- proves the clear landed. For those, the injection cannot be gated on a
-- rotation — that deadlocks — and is gated on this time instead.
--
-- The gap exists only because two prompts submitted back to back are silently
-- accepted and lost. Nothing is concluded from it having elapsed: the rotation
-- is still required, it is just observed after the injection instead of before.
--
-- NULL means the backend rotates on the clear itself and the injection waits on
-- that rotation, which is the original and still the common case.
ALTER TABLE renews ADD COLUMN inject_not_before_ms INTEGER;

PRAGMA user_version = 15;
