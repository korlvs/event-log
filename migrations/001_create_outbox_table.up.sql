-- Явно указываем схему (по умолчанию public)
CREATE SCHEMA IF NOT EXISTS public;

SET search_path TO public;

CREATE TABLE IF NOT EXISTS outbox (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_key TEXT NOT NULL,
    payload BYTEA NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    published_at TIMESTAMPTZ NULL
);

CREATE INDEX IF NOT EXISTS idx_outbox_published ON outbox(published_at) WHERE published_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_outbox_created ON outbox(created_at);

-- Таблица для отслеживания миграций
CREATE TABLE IF NOT EXISTS outbox_schema_migrations (
    version INTEGER PRIMARY KEY,
    applied_at TIMESTAMPTZ DEFAULT NOW()
);