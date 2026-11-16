-- =====================================================================
-- init.sql  -  business tables in the `fleet` database
--
-- Naming:  rt_*    = written by the SPEED layer (Spark Streaming), approximate & fresh
--          daily_* = written by the BATCH layer (Airflow), exact & recomputed from the lake
-- Every table has a primary key so writers can use
--   INSERT ... ON CONFLICT (...) DO UPDATE   (an "upsert")
-- which makes re-running a job safe: it overwrites rows instead of duplicating them.
-- =====================================================================

-- ---------------------------------------------------------------------
-- SPEED LAYER
-- ---------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS rt_zone_metrics (
    window_start     TIMESTAMPTZ   NOT NULL,
    window_end       TIMESTAMPTZ   NOT NULL,
    zone             TEXT          NOT NULL,
    active_vehicles  INTEGER       NOT NULL DEFAULT 0,
    idle_vehicles    INTEGER       NOT NULL DEFAULT 0,
    idle_ratio       NUMERIC(5,4)  NOT NULL DEFAULT 0,
    trips_completed  INTEGER       NOT NULL DEFAULT 0,
    trips_per_hour   NUMERIC(10,2) NOT NULL DEFAULT 0,
    earnings         NUMERIC(12,2) NOT NULL DEFAULT 0,
    event_count      INTEGER       NOT NULL DEFAULT 0,
    updated_at       TIMESTAMPTZ   NOT NULL DEFAULT now(),
    PRIMARY KEY (window_start, zone)
);
CREATE INDEX IF NOT EXISTS idx_rt_zone_metrics_window_end ON rt_zone_metrics (window_end DESC);
COMMENT ON TABLE  rt_zone_metrics IS 'Speed layer: 1-minute (real time) tumbling-window KPIs per zone.';
COMMENT ON COLUMN rt_zone_metrics.idle_ratio     IS 'idle_vehicles / active_vehicles in the window (0..1).';
COMMENT ON COLUMN rt_zone_metrics.trips_per_hour IS 'Completed trips scaled to one SIMULATED hour.';
COMMENT ON COLUMN rt_zone_metrics.earnings       IS 'Sum of fares of trips completed in the window (LKR).';

CREATE TABLE IF NOT EXISTS rt_vehicle_status (
    vehicle_id   TEXT          PRIMARY KEY,
    driver_id    TEXT,
    trip_id      TEXT,
    zone         TEXT,
    lat          DOUBLE PRECISION,
    lon          DOUBLE PRECISION,
    speed        NUMERIC(6,2),
    status       TEXT          NOT NULL CHECK (status IN ('idle', 'enroute', 'on_trip')),
    idle_since   TIMESTAMPTZ,
    event_time   TIMESTAMPTZ   NOT NULL,
    sim_date     DATE,
    updated_at   TIMESTAMPTZ   NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_rt_vehicle_status_zone ON rt_vehicle_status (zone);
COMMENT ON TABLE  rt_vehicle_status IS 'Speed layer: latest known state of every vehicle (one row per vehicle).';
COMMENT ON COLUMN rt_vehicle_status.idle_since IS 'When the vehicle entered its current idle period (NULL if not idle).';

CREATE TABLE IF NOT EXISTS idle_alerts (
    alert_id          BIGSERIAL     PRIMARY KEY,
    vehicle_id        TEXT          NOT NULL,
    driver_id         TEXT,
    zone              TEXT,
    idle_since        TIMESTAMPTZ   NOT NULL,
    idle_sim_minutes  NUMERIC(10,1) NOT NULL,
    status            TEXT          NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'resolved')),
    detected_at       TIMESTAMPTZ   NOT NULL DEFAULT now(),
    resolved_at       TIMESTAMPTZ
);
-- At most ONE open alert per vehicle: the streaming job upserts against this index.
CREATE UNIQUE INDEX IF NOT EXISTS uq_idle_alerts_open_vehicle ON idle_alerts (vehicle_id) WHERE status = 'open';
CREATE INDEX IF NOT EXISTS idx_idle_alerts_detected ON idle_alerts (detected_at DESC);
COMMENT ON TABLE idle_alerts IS 'Business alert: vehicle idle longer than IDLE_ALERT_SIM_MINUTES. Resolved when it moves again.';

-- ---------------------------------------------------------------------
-- BATCH LAYER
-- ---------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS vehicle_expenses (
    vehicle_id        TEXT          NOT NULL,
    expense_date      DATE          NOT NULL,
    fuel_cost         NUMERIC(12,2) NOT NULL CHECK (fuel_cost >= 0),
    maintenance_cost  NUMERIC(12,2) NOT NULL CHECK (maintenance_cost >= 0),
    distance_covered  NUMERIC(10,2) NOT NULL CHECK (distance_covered >= 0),
    service_flag      BOOLEAN       NOT NULL DEFAULT FALSE,
    source_file       TEXT,
    loaded_at         TIMESTAMPTZ   NOT NULL DEFAULT now(),
    PRIMARY KEY (vehicle_id, expense_date)
);
COMMENT ON TABLE vehicle_expenses IS 'Batch layer: cleaned daily expense records loaded from the landing CSV files.';

CREATE TABLE IF NOT EXISTS daily_vehicle_earnings (
    vehicle_id       TEXT          NOT NULL,
    sim_date         DATE          NOT NULL,
    trips_completed  INTEGER       NOT NULL DEFAULT 0,
    total_earnings   NUMERIC(12,2) NOT NULL DEFAULT 0,
    event_count      INTEGER       NOT NULL DEFAULT 0,
    computed_at      TIMESTAMPTZ   NOT NULL DEFAULT now(),
    PRIMARY KEY (vehicle_id, sim_date)
);
COMMENT ON TABLE daily_vehicle_earnings IS 'Batch layer: earnings per vehicle per sim day, recomputed from the raw Parquet lake (source of truth).';

CREATE TABLE IF NOT EXISTS daily_vehicle_profitability (
    vehicle_id              TEXT          NOT NULL,
    sim_date                DATE          NOT NULL,
    trips_completed         INTEGER       NOT NULL DEFAULT 0,
    earnings                NUMERIC(12,2) NOT NULL DEFAULT 0,
    fuel_cost               NUMERIC(12,2) NOT NULL DEFAULT 0,
    maintenance_cost        NUMERIC(12,2) NOT NULL DEFAULT 0,
    total_cost              NUMERIC(12,2) NOT NULL DEFAULT 0,
    profit                  NUMERIC(12,2) NOT NULL DEFAULT 0,
    profit_margin           NUMERIC(8,4),
    distance_covered        NUMERIC(10,2) NOT NULL DEFAULT 0,
    cost_per_km             NUMERIC(10,2),
    is_unprofitable         BOOLEAN       NOT NULL DEFAULT FALSE,
    trending_unprofitable   BOOLEAN       NOT NULL DEFAULT FALSE,
    consecutive_profit_drops INTEGER      NOT NULL DEFAULT 0,
    computed_at             TIMESTAMPTZ   NOT NULL DEFAULT now(),
    PRIMARY KEY (vehicle_id, sim_date)
);
CREATE INDEX IF NOT EXISTS idx_profitability_date ON daily_vehicle_profitability (sim_date DESC);
COMMENT ON TABLE  daily_vehicle_profitability IS 'Batch layer: earnings joined with expenses -> profit per vehicle per sim day.';
COMMENT ON COLUMN daily_vehicle_profitability.profit_margin IS 'profit / earnings; NULL when earnings = 0.';
COMMENT ON COLUMN daily_vehicle_profitability.cost_per_km   IS 'total_cost / distance_covered; NULL when distance = 0.';
COMMENT ON COLUMN daily_vehicle_profitability.trending_unprofitable IS 'TRUE when profit dropped on 2+ consecutive sim days.';

CREATE TABLE IF NOT EXISTS batch_run_log (
    run_id        BIGSERIAL    PRIMARY KEY,
    dag_run_id    TEXT,
    sim_date      DATE         NOT NULL,
    task          TEXT         NOT NULL,
    status        TEXT         NOT NULL CHECK (status IN ('success', 'failed', 'skipped')),
    record_count  INTEGER,
    message       TEXT,
    started_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),
    finished_at   TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_batch_run_log_date ON batch_run_log (sim_date DESC, task);
COMMENT ON TABLE batch_run_log IS 'Audit trail of every batch task run (status, row counts, data-quality results).';
