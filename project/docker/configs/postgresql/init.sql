-- PulseStream Database Initialization Schema
-- Supports range partitioning for events, anomaly records, and dead letter events.

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Partitioned events table
CREATE TABLE IF NOT EXISTS events (
    event_id        UUID NOT NULL,
    event_type      VARCHAR(50) NOT NULL,
    source          VARCHAR(20) NOT NULL,
    timestamp       TIMESTAMPTZ NOT NULL,
    user_id         VARCHAR(255) NOT NULL,
    community_id    VARCHAR(255),
    payload         JSONB NOT NULL DEFAULT '{}',
    ingested_at     TIMESTAMPTZ NOT NULL,
    processed_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    schema_version  VARCHAR(10) NOT NULL DEFAULT '1.0',
    PRIMARY KEY (event_id, timestamp)
) PARTITION BY RANGE (timestamp);

-- Default partition to guarantee any valid timestamp can be safely ingested
CREATE TABLE IF NOT EXISTS events_default PARTITION OF events DEFAULT;

-- Indexes on events
CREATE INDEX IF NOT EXISTS idx_events_type ON events (event_type);
CREATE INDEX IF NOT EXISTS idx_events_source ON events (source);
CREATE INDEX IF NOT EXISTS idx_events_community ON events (community_id) WHERE community_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_events_timestamp ON events (timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_events_payload_gin ON events USING gin (payload jsonb_path_ops);

-- Anomalies table
CREATE TABLE IF NOT EXISTS anomalies (
    id              SERIAL PRIMARY KEY,
    anomaly_type    VARCHAR(50) NOT NULL,
    severity        VARCHAR(10) NOT NULL,
    description     TEXT NOT NULL,
    details         JSONB NOT NULL DEFAULT '{}',
    detected_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    event_ids       UUID[] DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_anomalies_detected ON anomalies (detected_at DESC);

-- Dead letter events table
CREATE TABLE IF NOT EXISTS dead_letter_events (
    id              SERIAL PRIMARY KEY,
    raw_event       JSONB NOT NULL,
    error_message   TEXT NOT NULL,
    error_type      VARCHAR(100) NOT NULL,
    retry_count     INTEGER NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dlq_created ON dead_letter_events (created_at DESC);
