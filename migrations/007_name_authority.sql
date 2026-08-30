-- Distinguish Herdr-observed public names from Kelpie synthetic adopt aliases.
-- Recovery must not treat a synthesized alias as a live Herdr name constraint.
ALTER TABLE incarnations ADD COLUMN name_authority TEXT NOT NULL DEFAULT 'observed'
    CHECK (name_authority IN ('observed', 'synthesized'));
ALTER TABLE incarnations ADD COLUMN observed_native_session_json TEXT;

PRAGMA user_version = 7;
