-- Serving-layer schema. All writers upsert (idempotent) so job restarts and
-- Airflow retries never create duplicate rows.

-- ---------- Speed layer outputs (Spark Structured Streaming) ----------
CREATE TABLE IF NOT EXISTS rt_zone_metrics (
    window_start     TIMESTAMPTZ NOT NULL,
    window_end       TIMESTAMPTZ NOT NULL,
    zone             TEXT        NOT NULL,
    sim_day          INT,
    sim_hour         INT,
    active_vehicles  INT         NOT NULL DEFAULT 0,  -- distinct vehicles reporting
    idle_vehicles    INT         NOT NULL DEFAULT 0,
    idle_ratio       DOUBLE PRECISION,
    trips_completed  INT         NOT NULL DEFAULT 0,
    earnings         NUMERIC(12,2) NOT NULL DEFAULT 0,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (window_start, zone)
);

CREATE TABLE IF NOT EXISTS rt_fleet_metrics (
    window_start     TIMESTAMPTZ PRIMARY KEY,
    window_end       TIMESTAMPTZ NOT NULL,
    sim_day          INT,
    sim_hour         INT,
    active_vehicles  INT         NOT NULL DEFAULT 0,
    idle_vehicles    INT         NOT NULL DEFAULT 0,
    idle_ratio       DOUBLE PRECISION,
    trips_completed  INT         NOT NULL DEFAULT 0,
    earnings         NUMERIC(12,2) NOT NULL DEFAULT 0,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Latest known state per vehicle (drives idle-duration alerts).
CREATE TABLE IF NOT EXISTS vehicle_state (
    vehicle_id    TEXT PRIMARY KEY,
    status        TEXT NOT NULL,
    zone          TEXT,
    last_event_ts TIMESTAMPTZ NOT NULL,
    idle_since    TIMESTAMPTZ,            -- NULL when not idle
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS alerts (
    id          BIGSERIAL PRIMARY KEY,
    alert_type  TEXT NOT NULL,            -- e.g. VEHICLE_IDLE
    vehicle_id  TEXT,
    severity    TEXT NOT NULL DEFAULT 'warning',
    message     TEXT NOT NULL,
    sim_day     INT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_alerts_created ON alerts (created_at DESC);
-- One open idle alert per vehicle per idle episode.
CREATE UNIQUE INDEX IF NOT EXISTS uq_alert_episode
    ON alerts (alert_type, vehicle_id, message);

-- ---------- Batch layer outputs (Airflow) ----------
CREATE TABLE IF NOT EXISTS expenses (
    vehicle_id        TEXT NOT NULL,
    sim_day           INT  NOT NULL,
    fuel_cost         NUMERIC(10,2) NOT NULL CHECK (fuel_cost >= 0),
    maintenance_cost  NUMERIC(10,2) NOT NULL CHECK (maintenance_cost >= 0),
    distance_covered  NUMERIC(10,2) NOT NULL CHECK (distance_covered >= 0),
    service_flag      BOOLEAN NOT NULL DEFAULT FALSE,
    loaded_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (vehicle_id, sim_day)
);

CREATE TABLE IF NOT EXISTS daily_profitability (
    vehicle_id        TEXT NOT NULL,
    sim_day           INT  NOT NULL,
    trips             INT  NOT NULL,
    earnings          NUMERIC(12,2) NOT NULL,
    fuel_cost         NUMERIC(10,2),
    maintenance_cost  NUMERIC(10,2),
    distance_covered  NUMERIC(10,2),
    profit            NUMERIC(12,2),
    profit_per_km     NUMERIC(10,4),
    cost_per_km       NUMERIC(10,4),
    unprofitable      BOOLEAN NOT NULL DEFAULT FALSE,
    service_flag      BOOLEAN NOT NULL DEFAULT FALSE,
    has_expenses      BOOLEAN NOT NULL DEFAULT TRUE,   -- FALSE => vehicle missing from expense file
    generated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (vehicle_id, sim_day)
);
CREATE INDEX IF NOT EXISTS idx_profit_day ON daily_profitability (sim_day, profit);

-- Lambda consistency check: speed layer vs batch layer totals per sim day.
CREATE TABLE IF NOT EXISTS consistency_checks (
    sim_day          INT PRIMARY KEY,
    speed_trips      INT,
    batch_trips      INT,
    speed_earnings   NUMERIC(12,2),
    batch_earnings   NUMERIC(12,2),
    earnings_delta_pct DOUBLE PRECISION,
    checked_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Feeds /health: last time each stage made progress.
CREATE TABLE IF NOT EXISTS pipeline_status (
    component        TEXT PRIMARY KEY,     -- stream_sink | batch_reconcile | ...
    last_success_at  TIMESTAMPTZ,
    detail           TEXT
);
