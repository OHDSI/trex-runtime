-- ── Tables ──────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS example_sales.report_config (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(100) NOT NULL,
    description TEXT,
    refresh_interval_minutes INTEGER NOT NULL DEFAULT 60,
    is_active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS example_sales.report_run (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    config_id UUID NOT NULL REFERENCES example_sales.report_config(id),
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    status VARCHAR(20) NOT NULL DEFAULT 'running',
    rows_processed INTEGER DEFAULT 0
);

-- ── Seed default config ────────────────────────────────────────────

INSERT INTO example_sales.report_config (name, description, refresh_interval_minutes)
VALUES ('Daily Sales Report', 'Auto-generated daily sales summary', 1440)
ON CONFLICT DO NOTHING;
