# Scooter Fleet IoT Data Pipeline

<details>
<summary>🇹🇷 Türkçe özet için tıklayın</summary>

İstanbul'un Şişli-Beşiktaş bölgesinde simüle edilmiş bir elektrikli scooter filosu için uçtan uca, konteynerize bir veri mühendisliği pipeline'ı. Takım projesi (Yusuf Oğuz, Yusuf Öksüzer).

**Mimari:** bir Python simülatörü 50 scooter için GPS/pil/hız/anomali verisi üretip hem doğrudan PostgreSQL'e hem bir JSONL dosyasına yazıyor. Apache NiFi bu dosyayı izleyip anomalileri Elasticsearch'e yönlendiriyor, Kibana canlı harita ve dashboard gösteriyor, Apache Airflow ise her gece toplu özet tabloları (yolculuklar, bölge yoğunluğu, gelir) üretiyor. Tüm sistem tek bir `docker compose up` komutuyla ayağa kalkıyor, servisler (NiFi işlemcileri, Elasticsearch index'leri, Kibana dashboard'u, Airflow) otomatik kuruluyor.

Detaylı mimari şeması, servis tablosu, veri akışı ve bilinen kısıtlamalar için aşağıdaki İngilizce bölüme bakılabilir.

</details>

---

End-to-end containerized data engineering pipeline for a simulated electric scooter fleet operating in the Şişli-Beşiktaş district of Istanbul.

## Team Members

- Yusuf Oğuz
- Yusuf Öksüzer

## Architecture

```
┌─────────────────┐
│  Python         │  Simulates 50 scooters (GPS, battery,
│  Simulator      │  speed, anomalies) → JSONL + PostgreSQL
└────────┬────────┘
         │ /data/scooter_stream.jsonl (shared volume)
         ▼
┌─────────────────┐
│  Apache NiFi    │  TailFile → SplitText → EvaluateJsonPath
│  (port 8080)    │  → RouteOnAttribute → InvokeHTTP
└─────────────────┘
         │ anomalies only
         ▼
┌──────────────────┐        ┌──────────────┐
│  Elasticsearch   │───────▶│    Kibana    │
│  (port 9200)     │        │  (port 5601) │
│  scooter-alerts  │        │  Live map +  │
└──────────────────┘        │  dashboards  │
                            └──────────────┘
┌─────────────────┐
│   PostgreSQL    │  All telemetry (direct from simulator)
│   (port 5432)   │  + daily aggregates from Airflow
└────────┬────────┘
         │
         ▼
┌─────────────────┐        ┌──────────────┐
│  Apache Airflow │        │   pgAdmin    │
│  (port 8081)    │        │  (port 5050) │
│  Daily DAG      │        │  DB admin UI │
│  02:00 UTC      │        └──────────────┘
└─────────────────┘
```

## Quick Start

```bash
git clone <repo-url>
cd scooter-pipeline
cp .env.example .env
docker compose up --build
```

Allow ~3-5 minutes for all services to initialize. Everything is fully automatic, no manual configuration needed.

## Service URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| NiFi | http://localhost:8080/nifi | admin / admin12345 |
| Kibana | http://localhost:5601 | elastic / admin12345 |
| Airflow | http://localhost:8081 | admin / admin12345 |
| pgAdmin | http://localhost:5050 | admin@scooterapp.com / admin12345 |
| Elasticsearch | http://localhost:9200 | elastic / admin12345 |
| PostgreSQL | localhost:5432 | scooter / admin12345 |

## What Starts Automatically

| Container | Role |
|-----------|------|
| `simulator` | Emits 50 scooter records/sec to JSONL and PostgreSQL |
| `nifi-setup` | Creates NiFi processors and connections via REST API |
| `es-setup` | Creates Elasticsearch indices with geo_point mapping |
| `kibana-setup` | Imports the Kibana dashboard automatically |
| `airflow-init` | Initializes Airflow DB and creates admin user |

## Data Flow

1. **Simulator** emits 50 scooter records/second:
   - All records → PostgreSQL `telemetry` table (direct JDBC)
   - All records → `/data/scooter_stream.jsonl` (shared volume)
2. **NiFi** tails the JSONL file, splits into individual records, routes:
   - Anomalies & faults → **Elasticsearch** (`scooter-alerts` index)
   - Normal telemetry → dropped (already in PostgreSQL)
3. **Airflow** runs nightly at 02:00 UTC, aggregating into:
   - `public.trips`: trip summaries with revenue
   - `public.daily_hotspots`: 500m grid ride density
   - `public.daily_revenue`: fleet-wide daily revenue
4. **Kibana** shows real-time anomaly map and dashboards (auto-imported)
5. **pgAdmin** provides a GUI for PostgreSQL inspection

## PostgreSQL Schema

| Table | Description |
|-------|-------------|
| `public.telemetry` | Raw IoT records (all scooters, all events) |
| `public.trips` | Trip summaries: distance, duration, revenue |
| `public.daily_hotspots` | 500m grid hotspot density per day |
| `public.daily_revenue` | Fleet-wide revenue aggregates per day |

## Repository Structure

```
scooter-pipeline/
├── src/                   # Python simulator + Dockerfile
├── dags/                  # Airflow DAG
├── nifi/                  # NiFi setup script + flow XML
├── sql/                   # PostgreSQL init schema
├── elasticsearch/         # Index mappings + setup script
├── kibana/                # Dashboard export + setup script
├── docker/                # NiFi Dockerfile
├── docker-compose.yml
└── .env.example
```

## Environment Variables

Secrets are stored in `.env` (not committed to git):

```bash
cp .env.example .env
```

`.env.example` already ships with working demo credentials (`admin12345` everywhere, plus a fixed Airflow Fernet key) so the whole stack runs immediately with zero setup. This is intentional, not an oversight: every service here binds only to `localhost`, and the stack is meant to run on your own machine for a demo or development session, never exposed to a network. Anyone deploying this beyond localhost should replace every credential and generate a fresh Fernet key first.

## Known Limitations

- **NiFi flow is one-directional:** Normal telemetry records are dropped after routing (they are already written directly to PostgreSQL by the simulator). Only anomalies reach Elasticsearch.
- **Simulator is CPU-bound on a single thread per scooter:** At 50 scooters × 1 record/sec the load is light, but scaling to hundreds of scooters would require async I/O or multiprocessing.
- **Airflow DAG runs at 02:00 UTC daily:** During a fresh start the aggregation tables (`trips`, `daily_hotspots`, `daily_revenue`) will be empty until the first scheduled run. They can be triggered manually from the Airflow UI.
- **Kibana dashboard is pre-imported from a static export:** If Elasticsearch index mappings change, the saved search queries inside the dashboard may need to be refreshed manually.
- **No TLS between internal services:** All inter-container communication uses plain HTTP on the `pipeline_net` bridge network. This is acceptable for a development/demo environment but not for production.
- **JSONL file grows unboundedly during a session:** The simulator uses a `RotatingFileHandler` (50 MB cap, 3 backups), but NiFi's TailFile processor tracks the byte offset, so log rotation may cause NiFi to miss records written to the new file until it picks up the rotated path.
