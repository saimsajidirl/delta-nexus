# Delta Nexus

Delta Nexus is a data engineering platform for scraping product prices and
exchange rates, publishing validated events to Kafka, persisting them to
PostgreSQL, and viewing session results in a React dashboard.

The current application flow is:

```text
React dashboard or API client
    -> FastAPI
    -> async product and FX scrapers
    -> Kafka topics
    -> Kafka consumer
    -> PostgreSQL
    -> React dashboard polling
```

Kafka is the required event path. The API fails a run if it cannot scrape,
publish to Kafka, or persist session data when using the dashboard endpoint.

## Project Status

Delta Nexus currently includes:

- async scraping for product XML feeds and FloatRates-style exchange-rate pages
- Pydantic event contracts
- Kafka publishing and consuming
- PostgreSQL persistence with idempotent processed-message tracking
- a FastAPI control layer
- a React/Vite dashboard
- Docker Compose for the app, Kafka, and Kafka UI

Remaining hardening includes managed secrets, retries, dead-letter handling,
observability, migrations automation, stronger integration tests, deployment
automation, and security review.

## Stack

| Layer | Technology |
|---|---|
| API | FastAPI, Uvicorn, Pydantic |
| Scraping | httpx, lxml, selectolax |
| Streaming | Kafka, aiokafka |
| Storage | PostgreSQL, asyncpg |
| Frontend | React, TypeScript, Vite |
| Local orchestration | Docker Compose |

## Repository Layout

```text
.
|-- docker-compose.yml
|-- Dockerfile
|-- requirements-api.txt
|-- Delta_Nexus_End_to_End_Test.ipynb
|-- a_extra/
|   |-- DATABASE_SCHEMA_DESIGN.md
|   `-- database/
|       |-- schema.sql
|       `-- migrations/
`-- sitemap_exchange_rate_processors/
    |-- backend/
    |   `-- connect_scraper_with_kafka.py
    |-- broker/
    |   |-- broker.py
    |   |-- kafka_producer.py
    |   `-- kafka_consumer.py
    |-- consumers/
    |   `-- postgres_consumer.py
    |-- frontend/
    |   |-- Dockerfile
    |   |-- package.json
    |   `-- src/
    |-- scrapers/
    `-- storage/
```

## Prerequisites

- Docker Desktop
- PostgreSQL 15 or newer
- Python 3.12 if running Python modules outside Docker
- Node.js 20 or newer if running the frontend outside Docker

Docker Compose starts Kafka, Kafka UI, the FastAPI API, and the React frontend.
It does not start PostgreSQL. Create the database locally and import the schema
before using the dashboard session flow.

## PostgreSQL Setup

Create a local database named `delta_nexus_test`, then import the schema:

```powershell
psql -U postgres -d delta_nexus_test -f .\a_extra\database\schema.sql
```

Set a connection string for local Python commands:

```powershell
$env:DATABASE_URL="postgresql://postgres:<password>@localhost:5432/delta_nexus_test"
$env:DATABASE_SCHEMA="delta_nexus"
```

For Docker Compose, set `DOCKER_DATABASE_URL` so the API container can reach the
host PostgreSQL instance:

```powershell
$env:DOCKER_DATABASE_URL="postgresql://postgres:<password>@host.docker.internal:5432/delta_nexus_test"
```

## Quick Start

Start Docker Desktop, make sure PostgreSQL is running, then start the app:

```powershell
docker-compose up --build
```

Open:

| Service | URL |
|---|---|
| React dashboard | http://localhost:5173 |
| FastAPI docs | http://localhost:8000/docs |
| API health | http://localhost:8000/health |
| API readiness | http://localhost:8000/ready |
| Kafka UI | http://localhost:8080 |
| Kafka broker | localhost:9092 |

In the dashboard, keep the default feed URL or enter another product XML feed,
then select `START SCRAPE`. The dashboard creates a browser session id, runs a
session-scoped pipeline, and polls persisted PostgreSQL rows every two seconds.

## API Usage

### Basic Kafka Publish Run

`POST /pipeline/run` scrapes product and FX data, then publishes events to Kafka.
It does not persist rows to PostgreSQL by itself.

```powershell
curl -X POST "http://localhost:8000/pipeline/run" `
  -H "Content-Type: application/json" `
  -d '{
    "price_url": "https://feeds.datafeedwatch.com/25986/cbdd197d9c7747c13f08f840f8bc76eb350292fc.xml",
    "currency_url": "https://www.floatrates.com/",
    "kafka_servers": "localhost:9092",
    "price_topic": "raw-prices",
    "rate_topic": "fx-rates",
    "base_currency": "USD"
  }'
```

### Dashboard Session Run

`POST /sessions/{session_id}/pipeline/run` uses session-specific Kafka topics and
starts a short-lived Kafka-to-Postgres consumer while the scrape runs.

```powershell
curl -X POST "http://localhost:8000/sessions/session_demo01/pipeline/run" `
  -H "Content-Type: application/json" `
  -d '{
    "price_url": "https://feeds.datafeedwatch.com/25986/cbdd197d9c7747c13f08f840f8bc76eb350292fc.xml",
    "currency_url": "https://www.floatrates.com/",
    "kafka_servers": "localhost:9092",
    "base_currency": "USD",
    "consumer_timeout_seconds": 60,
    "persist_wait_seconds": 20
  }'
```

Fetch session data:

```powershell
curl "http://localhost:8000/sessions/session_demo01/data?limit=100"
```

Session topic names are deterministic:

```text
raw-prices-{session_id}
fx-rates-{session_id}
```

## API Endpoints

| Endpoint | Purpose |
|---|---|
| `GET /` | Service index |
| `GET /health` | Confirms the API process is running |
| `GET /ready` | Confirms PostgreSQL is reachable |
| `GET /config` | Returns safe runtime defaults |
| `POST /pipeline/run` | Scrapes and publishes to Kafka |
| `POST /sessions/{session_id}/pipeline/run` | Scrapes, publishes, and persists for one dashboard session |
| `GET /sessions/{session_id}/data` | Returns persisted rows and in-memory session logs |

## Event Contract

All Kafka messages include:

```text
schema_version
event_type
emitted_at
```

Product price events include:

```text
product_id
product_name
product_url
source_url
price
currency
timestamp
```

Currency-rate events include:

```text
base_currency
target_currency
rate
timestamp
```

Current schema version: `1.0`.

## Running Locally Without Docker

Install Python dependencies:

```powershell
.\.venv\Scripts\python.exe -m pip install -r requirements-api.txt
```

Run the API:

```powershell
.\.venv\Scripts\uvicorn.exe sitemap_exchange_rate_processors.backend.connect_scraper_with_kafka:app --reload
```

Run the PostgreSQL Kafka consumer:

```powershell
.\.venv\Scripts\python.exe -m sitemap_exchange_rate_processors.consumers.postgres_consumer `
  --bootstrap-servers localhost:9092 `
  --price-topic raw-prices `
  --rate-topic fx-rates `
  --database-url "postgresql://postgres:<password>@localhost:5432/delta_nexus_test" `
  --timeout-seconds 120
```

Run the CSV Kafka consumer:

```powershell
.\.venv\Scripts\python.exe -m sitemap_exchange_rate_processors.broker.kafka_consumer
```

Run the frontend locally:

```powershell
cd .\sitemap_exchange_rate_processors\frontend
npm install
npm run dev
```

## Docker Services

| Service | Purpose | Host port |
|---|---|---|
| `zookeeper` | Kafka dependency | `2181` |
| `kafka` | Event broker | `9092` |
| `kafka-ui` | Kafka topic/message UI | `8080` |
| `api` | FastAPI scraper control service | `8000` |
| `frontend` | React dashboard served by nginx | `5173` |

Inside Docker, the API uses Kafka at `kafka:29092`. Host tools use
`localhost:9092`.

## Output Paths

The CSV consumer writes:

```text
outputs/scraped_product_prices.csv
outputs/processed_currency_rates.csv
```

The dashboard reads from PostgreSQL tables in the `delta_nexus` schema,
especially:

```text
products
product_price_observations
exchange_rate_pairs
exchange_rate_observations
kafka_processed_messages
```

## Notebook

The end-to-end notebook is available at:

```text
Delta_Nexus_End_to_End_Test.ipynb
```

Run the Docker stack first, then execute the notebook to exercise the API,
Kafka, consumers, and persistence path.

## Troubleshooting

### `/ready` returns 503

PostgreSQL is not reachable from the API process. Confirm the database exists,
the schema has been imported, and `DATABASE_URL` or `DOCKER_DATABASE_URL` uses
the right password and host.

### Dashboard shows a Postgres or pipeline error

The dashboard calls the session endpoint, which requires Kafka and PostgreSQL.
Check:

```powershell
docker-compose ps
docker-compose logs api
```

### Kafka is unavailable

Wait for the Kafka health check to pass, then verify the host broker:

```powershell
docker-compose ps
```

Host clients should use `localhost:9092`. Containers should use `kafka:29092`.

### No rows appear in the dashboard

Make sure the session endpoint completed successfully and that the API container
can connect to PostgreSQL through `host.docker.internal`.

### Frontend cannot reach the API

The Compose frontend image is built with:

```text
VITE_API_BASE_URL=http://localhost:8000
```

If you change the API port or host, rebuild the frontend container.
