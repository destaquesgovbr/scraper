"""DDL da migration 038 do data-platform para os testes `postgres`.

Cópia manual de ``scripts/migrations/038_create_scrape_agency_state.sql`` (data-platform),
sem o seed e sem os ``COMMENT ON``, como as cópias das 008/026/037 nos outros testes. Manter
igual à migration. Usada por test_postgres_agency_state.py, test_postgres_ebc.py e
test_postgres_health_checks.py.
"""

MIGRATION_038 = """
CREATE TABLE IF NOT EXISTS scrape_agency_state (
    agency_key                      TEXT PRIMARY KEY,
    tier                            TEXT NOT NULL DEFAULT 'h2'
                                    CHECK (tier IN ('m30', 'h2', 'h6', 'd1')),
    poll_interval_minutes           INTEGER NOT NULL DEFAULT 120 CHECK (poll_interval_minutes > 0),
    rate_ewma_per_day               DOUBLE PRECISION,
    tier_updated_at                 TIMESTAMPTZ,
    next_due_at                     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    boost_until                     TIMESTAMPTZ,
    last_dispatched_at              TIMESTAMPTZ,
    last_attempt_at                 TIMESTAMPTZ,
    last_success_at                 TIMESTAMPTZ,
    last_new_article_at             TIMESTAMPTZ,
    consecutive_permanent_failures  INTEGER NOT NULL DEFAULT 0,
    consecutive_transient_failures  INTEGER NOT NULL DEFAULT 0,
    failure_streak_started_at       TIMESTAMPTZ,
    circuit_state                   TEXT NOT NULL DEFAULT 'closed'
                                    CHECK (circuit_state IN ('closed', 'open', 'half_open')),
    circuit_opened_at               TIMESTAMPTZ,
    circuit_failed_probes           INTEGER NOT NULL DEFAULT 0,
    last_error_category             TEXT,
    last_error_message              TEXT,
    created_at                      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at                      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_scrape_agency_state_next_due ON scrape_agency_state (next_due_at);

CREATE TABLE IF NOT EXISTS scrape_dispatch_meta (
    key         TEXT PRIMARY KEY,
    value       JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""
