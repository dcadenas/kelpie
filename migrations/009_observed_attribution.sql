-- Requested launch configuration is never observed execution metadata.
-- Observed rows are append-only. status undetermined is explicit, not NULL.
ALTER TABLE incarnations ADD COLUMN requested_model TEXT;
ALTER TABLE incarnations ADD COLUMN requested_provider TEXT;
ALTER TABLE incarnations ADD COLUMN requested_effort TEXT;

CREATE TABLE IF NOT EXISTS observed_attributions (
    id INTEGER PRIMARY KEY,
    incarnation_id TEXT NOT NULL REFERENCES incarnations(id),
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

CREATE INDEX IF NOT EXISTS observed_attributions_incarnation
    ON observed_attributions (incarnation_id, recorded_at_ms);

PRAGMA user_version = 9;
