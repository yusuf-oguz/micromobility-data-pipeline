"""
Airflow DAG: scooter_daily_summary
Runs every day at 02:00 UTC.
Computes:
  1. Trip summaries  → public.trips
  2. Daily hotspots  → public.daily_hotspots  (500m grid)
  3. Daily revenue   → public.daily_revenue
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator

POSTGRES_CONN_ID = "scooter_postgres"

default_args = {
    "owner": "scooter-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

UPSERT_TRIPS_SQL = """
INSERT INTO public.trips (trip_id, scooter_id, started_at, ended_at, distance_km, duration_min, revenue_tl)
SELECT
    trip_id,
    scooter_id,
    MIN(ts)                                             AS started_at,
    MAX(ts)                                             AS ended_at,
    ROUND((MAX(odometer_km) - MIN(odometer_km))::NUMERIC, 3) AS distance_km,
    ROUND(EXTRACT(EPOCH FROM (MAX(ts) - MIN(ts))) / 60.0, 2) AS duration_min,
    ROUND(
        GREATEST(0, (MAX(odometer_km) - MIN(odometer_km))) * 3.5 + 5.0
    , 2)                                                AS revenue_tl
FROM public.telemetry
WHERE trip_id IS NOT NULL
  AND ts >= DATE_TRUNC('day', NOW() - INTERVAL '1 day')
  AND ts <  DATE_TRUNC('day', NOW())
GROUP BY trip_id, scooter_id
ON CONFLICT (trip_id) DO UPDATE
    SET ended_at     = EXCLUDED.ended_at,
        distance_km  = EXCLUDED.distance_km,
        duration_min = EXCLUDED.duration_min,
        revenue_tl   = EXCLUDED.revenue_tl;
"""

UPSERT_HOTSPOTS_SQL = """
INSERT INTO public.daily_hotspots (report_date, grid_lat, grid_lon, ride_count, total_km)
SELECT
    DATE_TRUNC('day', NOW() - INTERVAL '1 day')::DATE      AS report_date,
    ROUND((lat / 0.005)::NUMERIC) * 0.005                  AS grid_lat,
    ROUND((lon / 0.005)::NUMERIC) * 0.005                  AS grid_lon,
    COUNT(DISTINCT trip_id)                                 AS ride_count,
    ROUND(SUM(speed_kmh * 1.0 / 3600)::NUMERIC, 3)        AS total_km
FROM public.telemetry
WHERE ts >= DATE_TRUNC('day', NOW() - INTERVAL '1 day')
  AND ts <  DATE_TRUNC('day', NOW())
  AND lat IS NOT NULL
  AND trip_id IS NOT NULL
GROUP BY 2, 3
ON CONFLICT (report_date, grid_lat, grid_lon) DO UPDATE
    SET ride_count = EXCLUDED.ride_count,
        total_km   = EXCLUDED.total_km;
"""

UPSERT_REVENUE_SQL = """
INSERT INTO public.daily_revenue (report_date, total_trips, total_km, total_revenue_tl, avg_trip_km)
SELECT
    started_at::DATE                   AS report_date,
    COUNT(*)                           AS total_trips,
    ROUND(SUM(distance_km)::NUMERIC, 3) AS total_km,
    ROUND(SUM(revenue_tl)::NUMERIC, 2)  AS total_revenue_tl,
    ROUND(AVG(distance_km)::NUMERIC, 3) AS avg_trip_km
FROM public.trips
WHERE started_at::DATE = (NOW() - INTERVAL '1 day')::DATE
GROUP BY 1
ON CONFLICT (report_date) DO UPDATE
    SET total_trips      = EXCLUDED.total_trips,
        total_km         = EXCLUDED.total_km,
        total_revenue_tl = EXCLUDED.total_revenue_tl,
        avg_trip_km      = EXCLUDED.avg_trip_km,
        created_at       = NOW();
"""


def create_postgres_connection():
    """Ensure Airflow connection to PostgreSQL exists (idempotent)."""
    from airflow.models import Connection
    from airflow import settings as airflow_settings

    session = airflow_settings.Session()
    existing = session.query(Connection).filter(Connection.conn_id == POSTGRES_CONN_ID).first()
    if not existing:
        conn = Connection(
            conn_id=POSTGRES_CONN_ID,
            conn_type="postgres",
            host=os.getenv("POSTGRES_HOST", "postgres"),
            schema=os.getenv("POSTGRES_DB", "scooterdb"),
            login=os.getenv("POSTGRES_USER", "scooter"),
            password=os.getenv("POSTGRES_PASSWORD", "admin12345"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
        )
        session.add(conn)
        session.commit()
    session.close()


with DAG(
    dag_id="scooter_daily_summary",
    description="Daily trip, hotspot, and revenue aggregation for scooter fleet",
    schedule="0 2 * * *",
    start_date=datetime(2026, 5, 1),
    catchup=False,
    default_args=default_args,
    tags=["scooter", "daily", "aggregation"],
) as dag:

    ensure_conn = PythonOperator(
        task_id="ensure_postgres_connection",
        python_callable=create_postgres_connection,
    )

    compute_trips = PostgresOperator(
        task_id="compute_trip_summaries",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=UPSERT_TRIPS_SQL,
    )

    compute_hotspots = PostgresOperator(
        task_id="compute_daily_hotspots",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=UPSERT_HOTSPOTS_SQL,
    )

    compute_revenue = PostgresOperator(
        task_id="compute_daily_revenue",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=UPSERT_REVENUE_SQL,
    )

    ensure_conn >> compute_trips >> [compute_hotspots, compute_revenue]
