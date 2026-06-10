-- Fleet Telemetry — compact TimescaleDB schema
-- Target: PostgreSQL 16 + TimescaleDB 2.x  (database: fleet_telemetry)
--
-- Design principles (derived from measured data, see database.md):
--   * Change-only storage: a signal row is written only when the value
--     differs from the previous value for that (vehicle, signal).
--     Measured on real data this drops 71.8% of signal observations.
--   * No raw payload / decoded_payload / metadata blobs (72% of the JSONL
--     bytes) — they duplicate the decoded data.
--   * Integer surrogate keys: vehicle_id + signal_id (2+2 bytes) instead of
--     repeating a 17-char VIN and a signal name on every row.
--   * Alerts stored as episodes, not as re-sent batches: 54,034 raw alert
--     entries collapse into 893 unique (vehicle, alert, started_at) episodes.
--   * One value column per storage class; the per-signal type lives in the
--     `signals` dimension table, not on every row.

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ═══════════════════════════════════════════════════════════════
--  Dimension tables (tiny, hot in cache)
-- ═══════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS vehicles (
    vehicle_id            smallint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    vin                   text NOT NULL UNIQUE CHECK (length(vin) = 17),
    first_seen            timestamptz NOT NULL DEFAULT now(),
    device_client_version text                  -- latest seen, from metadata
);

-- signal_id = Field enum number from protos/vehicle_data.proto (1..259),
-- seeded by scripts/setup_db.py. Signals that appear in data but not in the
-- proto (future firmware) are auto-registered with ids >= 1000.
CREATE TABLE IF NOT EXISTS signals (
    signal_id   smallint PRIMARY KEY,
    name        text NOT NULL UNIQUE,
    value_kind  text   -- informational: num | long | bool | text | location | json
);

CREATE TABLE IF NOT EXISTS alert_types (
    alert_id smallint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name     text NOT NULL UNIQUE
);

-- ═══════════════════════════════════════════════════════════════
--  signal_changes — the only high-volume table (hypertable)
-- ═══════════════════════════════════════════════════════════════
-- One row per *change* of one signal of one vehicle.
-- Exactly one of the v_* columns is non-NULL, or none when invalid=true
-- (sensor temporarily unavailable). NULL columns cost ~1 bit each.
--   v_num  : floatValue / doubleValue
--   v_long : intValue / longValue (exact 64-bit, no float8 precision loss)
--   v_bool : booleanValue
--   v_text : stringValue, enum states, HH:MM:SS time values
--   v_loc  : locationValue as point(longitude, latitude)
--   v_json : doorValue / tireLocation composites

CREATE TABLE IF NOT EXISTS signal_changes (
    ts         timestamptz NOT NULL,            -- vehicle-side CreatedAt
    vehicle_id smallint    NOT NULL REFERENCES vehicles(vehicle_id),
    signal_id  smallint    NOT NULL REFERENCES signals(signal_id),
    v_num      double precision,
    v_long     bigint,
    v_bool     boolean,
    v_text     text,
    v_loc      point,
    v_json     jsonb,
    invalid    boolean NOT NULL DEFAULT false
);

SELECT create_hypertable('signal_changes', 'ts',
    chunk_time_interval => INTERVAL '7 days',
    if_not_exists       => TRUE
);

-- Covers the dominant query: history of one signal for one vehicle.
CREATE INDEX IF NOT EXISTS idx_sc_vehicle_signal_ts
    ON signal_changes (vehicle_id, signal_id, ts DESC);

-- Columnar compression: segments are (vehicle, signal) series; within a
-- segment values compress with delta/dictionary encoding.
ALTER TABLE signal_changes SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'vehicle_id, signal_id',
    timescaledb.compress_orderby   = 'ts DESC'
);
SELECT add_compression_policy('signal_changes', INTERVAL '14 days', if_not_exists => TRUE);

-- ═══════════════════════════════════════════════════════════════
--  signal_latest — current state, one row per (vehicle, signal)
-- ═══════════════════════════════════════════════════════════════
-- Upserted during ingestion. Doubles as the change-detection state and as
-- an O(1) "latest state of the car" lookup (never scan the hypertable for
-- current state).

CREATE TABLE IF NOT EXISTS signal_latest (
    vehicle_id smallint    NOT NULL REFERENCES vehicles(vehicle_id),
    signal_id  smallint    NOT NULL REFERENCES signals(signal_id),
    ts         timestamptz NOT NULL,
    v_num      double precision,
    v_long     bigint,
    v_bool     boolean,
    v_text     text,
    v_loc      point,
    v_json     jsonb,
    invalid    boolean NOT NULL DEFAULT false,
    PRIMARY KEY (vehicle_id, signal_id)
);

-- ═══════════════════════════════════════════════════════════════
--  alert_episodes — deduplicated alert lifecycle
-- ═══════════════════════════════════════════════════════════════
-- The vehicle re-sends its alert history on every alerts message (measured
-- 60x duplication). One episode = one (vehicle, alert, started_at);
-- ended_at is filled in when the closing entry arrives. Open alerts have
-- ended_at IS NULL.

CREATE TABLE IF NOT EXISTS alert_episodes (
    vehicle_id    smallint    NOT NULL REFERENCES vehicles(vehicle_id),
    alert_id      smallint    NOT NULL REFERENCES alert_types(alert_id),
    started_at    timestamptz NOT NULL,
    ended_at      timestamptz,
    audiences     text[],                 -- sorted, e.g. {Customer,Service}
    last_reported timestamptz,            -- envelope time of last re-send
    PRIMARY KEY (vehicle_id, alert_id, started_at)
);

CREATE INDEX IF NOT EXISTS idx_alerts_open
    ON alert_episodes (vehicle_id, started_at DESC)
    WHERE ended_at IS NULL;

-- ═══════════════════════════════════════════════════════════════
--  connectivity_events — connect/disconnect sessions
-- ═══════════════════════════════════════════════════════════════
-- No duplication observed in real data; natural key makes re-ingestion
-- idempotent. Low volume (~2 rows per drive/session).

CREATE TABLE IF NOT EXISTS connectivity_events (
    vehicle_id        smallint    NOT NULL REFERENCES vehicles(vehicle_id),
    connection_id     uuid        NOT NULL,
    status            text        NOT NULL CHECK (status IN ('UNKNOWN','CONNECTED','DISCONNECTED')),
    network_interface text,
    ts                timestamptz NOT NULL,    -- vehicle-side CreatedAt
    PRIMARY KEY (vehicle_id, connection_id, status)
);

CREATE INDEX IF NOT EXISTS idx_conn_vehicle_ts
    ON connectivity_events (vehicle_id, ts DESC);

-- ═══════════════════════════════════════════════════════════════
--  error_events — diagnostic errors
-- ═══════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS error_events (
    vehicle_id smallint    NOT NULL REFERENCES vehicles(vehicle_id),
    ts         timestamptz NOT NULL,           -- vehicle-side CreatedAt
    name       text        NOT NULL,
    body       text,
    tags       jsonb,
    UNIQUE NULLS NOT DISTINCT (vehicle_id, ts, name, tags)
);

CREATE INDEX IF NOT EXISTS idx_errors_vehicle_ts
    ON error_events (vehicle_id, ts DESC);

-- ═══════════════════════════════════════════════════════════════
--  Convenience views
-- ═══════════════════════════════════════════════════════════════

-- Current state of every vehicle, human-readable.
CREATE OR REPLACE VIEW v_current_state AS
SELECT
    v.vin,
    s.name AS signal,
    l.ts,
    CASE
        WHEN l.invalid THEN '<invalid>'
        ELSE COALESCE(l.v_text,
                      l.v_num::text,
                      l.v_long::text,
                      l.v_bool::text,
                      l.v_loc::text,
                      l.v_json::text)
    END AS value
FROM signal_latest l
JOIN vehicles v USING (vehicle_id)
JOIN signals  s USING (signal_id);

-- Signal history with names resolved.
CREATE OR REPLACE VIEW v_signal_history AS
SELECT
    v.vin,
    s.name AS signal,
    c.ts,
    c.v_num, c.v_long, c.v_bool, c.v_text, c.v_loc, c.v_json, c.invalid
FROM signal_changes c
JOIN vehicles v USING (vehicle_id)
JOIN signals  s USING (signal_id);

-- Open alerts per vehicle.
CREATE OR REPLACE VIEW v_open_alerts AS
SELECT v.vin, t.name AS alert, a.started_at, a.audiences, a.last_reported
FROM alert_episodes a
JOIN vehicles    v USING (vehicle_id)
JOIN alert_types t USING (alert_id)
WHERE a.ended_at IS NULL;

-- Connection sessions with duration.
CREATE OR REPLACE VIEW v_connection_sessions AS
SELECT
    v.vin,
    c.connection_id,
    c.network_interface,
    c.ts                    AS connected_at,
    d.ts                    AS disconnected_at,
    d.ts - c.ts             AS duration
FROM connectivity_events c
JOIN vehicles v USING (vehicle_id)
LEFT JOIN connectivity_events d
       ON d.vehicle_id = c.vehicle_id
      AND d.connection_id = c.connection_id
      AND d.status = 'DISCONNECTED'
WHERE c.status = 'CONNECTED';
