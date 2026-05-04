# Scooter Fleet IoT Data Pipeline

End-to-end containerized data engineering pipeline for a simulated electric scooter fleet operating in the Şişli-Beşiktaş district of Istanbul.

**YZV 322E — Applied Data Engineering · Spring 2026 · Istanbul Technical University**

## Team Members

| Name | Student No |
|------|-----------|
| Yusuf Oğuz | 150220322 |

## Architecture

```
┌─────────────────┐
│  Python         │  Simulates 50 scooters (GPS, battery,
│  Simulator      │  speed, anomalies) → JSONL log file
└────────┬────────┘
         │ /data/scooter_stream.jsonl (shared volume)
         ▼
┌─────────────────┐
│  Apache NiFi    │  TailFile → SplitText → EvaluateJsonPath
│  (port 8080)    │  → RouteOnAttribute
└────┬───────┬────┘
     │       │
     ▼       ▼
┌────────┐ ┌──────────────────┐
│Postgres│ │  Elasticsearch   │
│(5432)  │ │  (port 9200)     │
│        │ │  index:          │
│telemetry│ │  scooter-alerts  │
│trips   │ └────────┬─────────┘
│hotspots│          │
└────┬───┘          ▼
     │       ┌──────────────┐
     │       │    Kibana    │
     │       │  (port 5601) │
     │       │  Live map +  │
     │       │  dashboards  │
     │       └──────────────┘
     │
     ▼
┌─────────────────┐
│  Apache Airflow │  Daily DAG: trips → hotspots → revenue
│  (port 8081)    │  Runs at 02:00 UTC
└─────────────────┘
     │
     ▼
┌─────────────────┐
│    pgAdmin      │  DB admin UI
│  (port 5050)    │
└─────────────────┘
```

## Quick Start

```bash
git clone <repo-url>
cd scooter-pipeline
docker compose up --build
```

All services start automatically. Allow ~3-5 minutes for Elasticsearch and NiFi to initialize.

## Service URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| NiFi | http://localhost:8080/nifi | admin / admin12345 |
| Kibana | http://localhost:5601 | elastic / admin12345 |
| Airflow | http://localhost:8081 | admin / admin12345 |
| pgAdmin | http://localhost:5050 | admin@scooter.local / admin12345 |
| Elasticsearch | http://localhost:9200 | elastic / admin12345 |

## NiFi Flow Setup

After NiFi starts:
1. Open http://localhost:8080/nifi
2. Hamburger menu → Templates → Upload Template → select `nifi/scooter_flow.xml`
3. Drag template onto canvas → Instantiate
4. Configure the `DBCPConnectionPool` controller service with PostgreSQL JDBC URL
5. Start all processors

## Repository Structure

```
scooter-pipeline/
├── src/               # Python simulator + Dockerfile
├── dags/              # Airflow DAG files
├── nifi/              # NiFi flow template (XML)
│   └── templates/
├── sql/               # PostgreSQL init scripts
├── elasticsearch/     # Index mappings + setup script
├── docs/              # Kibana setup guide
├── data/              # Sample data (small subset)
├── docker-compose.yml
└── .env
```

## Data Flow

1. **Simulator** emits 50 scooter records/second as JSONL to a shared Docker volume
2. **NiFi** tails the file, splits into individual records, classifies each:
   - Anomalies / faults → **Elasticsearch** (`scooter-alerts` index)
   - Normal telemetry → **PostgreSQL** (`telemetry` table)
3. **Airflow** runs nightly at 02:00 UTC, aggregating trips, hotspots, and revenue into summary tables
4. **Kibana** shows real-time alert map and dashboards from Elasticsearch
5. **pgAdmin** provides a GUI for PostgreSQL inspection

## Known Limitations

- NiFi `PutDatabaseRecord` requires manual JDBC driver configuration in the NiFi UI (PostgreSQL JDBC jar must be added to NiFi's lib directory or via NAR)
- Airflow `scooter_daily_summary` DAG only processes yesterday's data; backfill is not triggered automatically
- Kibana dashboards must be created manually following `docs/kibana_setup.md`
- The simulator generates synthetic data only; no real scooter GPS feeds are used

## Environment Variables

All secrets are stored in `.env` (not committed to git). See `.env.example` for required variables.
