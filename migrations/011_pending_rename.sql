-- A rename is one intent with an external effect in Herdr, so its target name is
-- durable before Herdr is asked. While this is set, a Ready binding is still
-- exact when the live agent answers to either the committed name or this target,
-- which is what stops a crash mid-rename from stranding a live agent as lost.
ALTER TABLE incarnations ADD COLUMN pending_rename_to TEXT;

PRAGMA user_version = 11;
