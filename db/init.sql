-- Database initialization for SQL Throughput Challenge
-- Schema: single table `records` optimized for read benchmarks.

-- Drop existing table if rerunning init in a dev environment.
DROP TABLE IF EXISTS public.records;

CREATE TABLE public.records (
    id            BIGSERIAL PRIMARY KEY,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    category      TEXT NOT NULL,
    payload       JSONB NOT NULL,
    amount        NUMERIC(12,2) NOT NULL,
    is_active     BOOLEAN NOT NULL DEFAULT TRUE,
    source        TEXT NOT NULL DEFAULT 'generator'
);

-- Basic indexes to support typical filters/pagination.
CREATE INDEX IF NOT EXISTS idx_records_created_at ON public.records (created_at);
CREATE INDEX IF NOT EXISTS idx_records_category    ON public.records (category);
CREATE INDEX IF NOT EXISTS idx_records_is_active   ON public.records (is_active);

-- Trigger to keep updated_at current on updates (lightweight).
CREATE OR REPLACE FUNCTION public.set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_set_updated_at ON public.records;
CREATE TRIGGER trg_set_updated_at
BEFORE UPDATE ON public.records
FOR EACH ROW
EXECUTE FUNCTION public.set_updated_at();
