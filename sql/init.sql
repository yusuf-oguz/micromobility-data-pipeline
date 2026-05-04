-- Scooter Pipeline — PostgreSQL schema initialization
-- Runs automatically on first container start via docker-entrypoint-initdb.d

-- ─── Airflow uses this DB too; keep tables in a dedicated schema ────────────
CREATE SCHEMA IF NOT EXISTS scooter;

-- ─── Raw telemetry (normal rides, routing, billing) ──────────────────────────
CREATE TABLE IF NOT EXISTS public.telemetry (
    event_id        UUID        PRIMARY KEY,
    scooter_id      VARCHAR(16) NOT NULL,
    ts              TIMESTAMPTZ NOT NULL,
    lat             DOUBLE PRECISION,
    lon             DOUBLE PRECISION,
    speed_kmh       NUMERIC(6,2),
    battery_pct     NUMERIC(5,2),
    odometer_km     NUMERIC(10,3),
    trip_id         UUID,
    gps_valid       BOOLEAN     DEFAULT TRUE,
    ingested_at     TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_telemetry_scooter ON public.telemetry (scooter_id);
CREATE INDEX IF NOT EXISTS idx_telemetry_ts      ON public.telemetry (ts DESC);
CREATE INDEX IF NOT EXISTS idx_telemetry_trip    ON public.telemetry (trip_id) WHERE trip_id IS NOT NULL;

-- ─── Trip billing summary (populated by Airflow DAG daily) ───────────────────
CREATE TABLE IF NOT EXISTS public.trips (
    trip_id         UUID        PRIMARY KEY,
    scooter_id      VARCHAR(16) NOT NULL,
    started_at      TIMESTAMPTZ,
    ended_at        TIMESTAMPTZ,
    distance_km     NUMERIC(10,3),
    duration_min    NUMERIC(8,2),
    revenue_tl      NUMERIC(8,2),
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

-- ─── Daily hotspot summary (populated by Airflow DAG daily) ──────────────────
CREATE TABLE IF NOT EXISTS public.daily_hotspots (
    report_date     DATE        NOT NULL,
    grid_lat        NUMERIC(8,5) NOT NULL,
    grid_lon        NUMERIC(8,5) NOT NULL,
    ride_count      INTEGER,
    total_km        NUMERIC(12,3),
    PRIMARY KEY (report_date, grid_lat, grid_lon)
);

-- ─── Daily revenue summary ────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS public.daily_revenue (
    report_date     DATE PRIMARY KEY,
    total_trips     INTEGER,
    total_km        NUMERIC(12,3),
    total_revenue_tl NUMERIC(12,2),
    avg_trip_km     NUMERIC(8,3),
    created_at      TIMESTAMPTZ DEFAULT NOW()
);
