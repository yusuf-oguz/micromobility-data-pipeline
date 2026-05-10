"""
Scooter IoT telemetry simulator.
Emits JSONL records to a rotating log file (consumed by NiFi/TailFile)
and inserts every record directly into PostgreSQL telemetry table.
"""
import json
import os
import random
import time
import uuid
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
import logging

import psycopg2
from psycopg2.extras import execute_values

LOG_FILE = os.getenv("LOG_FILE", "/data/scooter_stream.jsonl")
SCOOTER_COUNT = int(os.getenv("SCOOTER_COUNT", "50"))
EMIT_INTERVAL = float(os.getenv("EMIT_INTERVAL_SEC", "1"))

# Şişli-Beşiktaş bounding box
LAT_MIN, LAT_MAX = 41.040, 41.075
LON_MIN, LON_MAX = 28.980, 29.020

ANOMALY_RATE = 0.04  # %4 kayıt anomalili olacak

os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

handler = RotatingFileHandler(LOG_FILE, maxBytes=50 * 1024 * 1024, backupCount=3)
handler.setFormatter(logging.Formatter("%(message)s"))
logger = logging.getLogger("simulator")
logger.setLevel(logging.INFO)
logger.addHandler(handler)


def connect_postgres():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "scooterdb"),
        user=os.getenv("POSTGRES_USER", "scooter"),
        password=os.getenv("POSTGRES_PASSWORD", "admin12345"),
    )


INSERT_SQL = """
INSERT INTO public.telemetry
  (event_id, scooter_id, ts, lat, lon, speed_kmh, battery_pct,
   odometer_km, trip_id, gps_valid, fault_code, anomaly_type)
VALUES %s
ON CONFLICT (event_id) DO NOTHING
"""


class Scooter:
    def __init__(self, scooter_id: str):
        self.scooter_id = scooter_id
        self.lat = random.uniform(LAT_MIN, LAT_MAX)
        self.lon = random.uniform(LON_MIN, LON_MAX)
        self.battery = random.uniform(30, 100)
        self.speed = 0.0
        self.trip_id: str | None = None
        self.odometer_km = random.uniform(0, 5000)
        self.fault_code: str | None = None

    def _move(self):
        """Random walk with soft boundary clamp."""
        self.lat += random.gauss(0, 0.0003)
        self.lon += random.gauss(0, 0.0003)
        self.lat = max(LAT_MIN, min(LAT_MAX, self.lat))
        self.lon = max(LON_MIN, min(LON_MAX, self.lon))

    def tick(self) -> dict:
        is_anomaly = random.random() < ANOMALY_RATE
        anomaly_type = None

        if is_anomaly:
            anomaly_type = random.choice(["topple", "critical_battery", "gps_loss", "motor_fault"])

        # Trip management
        if self.trip_id is None and random.random() < 0.05:
            self.trip_id = str(uuid.uuid4())
        if self.trip_id and random.random() < 0.03:
            self.trip_id = None

        # Movement
        if self.trip_id:
            self._move()
            self.speed = random.uniform(5, 25)
            delta_km = self.speed * EMIT_INTERVAL / 3600
            self.odometer_km += delta_km
            self.battery = max(0, self.battery - random.uniform(0.01, 0.05))
        else:
            self.speed = 0.0

        # Anomaly overrides
        if anomaly_type == "critical_battery":
            self.battery = random.uniform(0, 2)
        elif anomaly_type == "gps_loss":
            return self._build_record(anomaly_type, gps_valid=False)
        elif anomaly_type == "topple":
            self.speed = 0.0
        elif anomaly_type == "motor_fault":
            self.fault_code = f"ERR_{random.randint(100, 999)}"
            self.speed = 0.0

        record = self._build_record(anomaly_type)

        # Clear one-shot fault
        if self.fault_code:
            self.fault_code = None

        return record

    def _build_record(self, anomaly_type, gps_valid=True) -> dict:
        lat = round(self.lat, 6) if gps_valid else None
        lon = round(self.lon, 6) if gps_valid else None
        return {
            "event_id": str(uuid.uuid4()),
            "scooter_id": self.scooter_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "lat": lat,
            "lon": lon,
            "location": {"lat": lat, "lon": lon} if gps_valid else None,
            "speed_kmh": round(self.speed, 2),
            "battery_pct": round(self.battery, 2),
            "odometer_km": round(self.odometer_km, 3),
            "trip_id": self.trip_id,
            "gps_valid": gps_valid,
            "fault_code": self.fault_code,
            "anomaly_type": anomaly_type,
        }


def record_to_row(r: dict):
    return (
        r["event_id"], r["scooter_id"], r["timestamp"],
        r["lat"], r["lon"], r["speed_kmh"], r["battery_pct"],
        r["odometer_km"], r["trip_id"], r["gps_valid"],
        r["fault_code"], r["anomaly_type"],
    )


def main():
    scooters = [Scooter(f"SC{str(i).zfill(4)}") for i in range(SCOOTER_COUNT)]
    record_count = 0
    print(f"[simulator] Starting: {SCOOTER_COUNT} scooters, writing to {LOG_FILE}", flush=True)

    pg = None
    while pg is None:
        try:
            pg = connect_postgres()
            print("[simulator] PostgreSQL connected.", flush=True)
        except Exception as e:
            print(f"[simulator] PG connect failed: {e}, retrying in 5s...", flush=True)
            time.sleep(5)

    cur = pg.cursor()

    while True:
        batch = []
        for sc in scooters:
            record = sc.tick()
            logger.info(json.dumps(record, ensure_ascii=False))
            batch.append(record_to_row(record))
            record_count += 1

        try:
            execute_values(cur, INSERT_SQL, batch)
            pg.commit()
        except Exception as e:
            print(f"[simulator] PG insert error: {e}", flush=True)
            pg.rollback()
            try:
                pg = connect_postgres()
                cur = pg.cursor()
            except Exception:
                pass

        if record_count % (SCOOTER_COUNT * 100) == 0:
            print(f"[simulator] Emitted {record_count} records total", flush=True)

        time.sleep(EMIT_INTERVAL)


if __name__ == "__main__":
    main()
