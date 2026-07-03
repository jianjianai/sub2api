CREATE TABLE IF NOT EXISTS scheduler_rebuild_jobs (
    id BIGSERIAL PRIMARY KEY,
    scope TEXT NOT NULL DEFAULT 'full',
    reason TEXT NOT NULL,
    source TEXT NOT NULL DEFAULT 'system',
    source_outbox_id BIGINT,
    status TEXT NOT NULL DEFAULT 'pending',
    attempts INTEGER NOT NULL DEFAULT 0,
    run_after TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    locked_by TEXT,
    locked_until TIMESTAMPTZ,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    CONSTRAINT scheduler_rebuild_jobs_scope_check
        CHECK (scope IN ('full')),
    CONSTRAINT scheduler_rebuild_jobs_status_check
        CHECK (status IN ('pending', 'running', 'succeeded', 'failed'))
);

CREATE INDEX IF NOT EXISTS idx_scheduler_rebuild_jobs_claim
    ON scheduler_rebuild_jobs (status, run_after, id)
    WHERE status IN ('pending', 'running');

CREATE UNIQUE INDEX IF NOT EXISTS idx_scheduler_rebuild_jobs_active_full
    ON scheduler_rebuild_jobs (scope)
    WHERE scope = 'full' AND status IN ('pending', 'running');
