# Delta Nexus

Delta Nexus is a learning-focused Python data pipeline that scrapes live product
price data and exchange-rate data, publishes validated events to Kafka, and
processes those Kafka events into CSV outputs.

The current working local flow is:

```text
FastAPI request
    |
    v
Product scraper + FX scraper
    |
    v
Kafka topics
    |
    v
Kafka consumer
    |
    v
CSV files
```

Kafka is the required message path. If Kafka cannot be reached or an event cannot
be published, the pipeline fails instead of silently skipping the message.

> Project status: Delta Nexus is a portfolio and learning project. It currently
> demonstrates async scraping, schema validation, Kafka publishing, a FastAPI
> trigger layer, Docker Compose infrastructure, and CSV persistence. It is not
> production-ready yet. Production work would still need durable database
> storage, retries, dead-letter handling, observability, security, deployment
> automation, and broader integration tests.

## Quick Start

Start Docker Desktop first, then run the full local stack:

```powershell
docker-compose up --build
```

This starts:

- ZooKeeper
- Kafka
- Kafka UI
- FastAPI

Open:

```text
FastAPI docs: http://localhost:8000/docs
Health:       http://localhost:8000/health
Kafka UI:     http://localhost:8080
Kafka broker: localhost:9092
```

## Run The Pipeline Through FastAPI

Use the Swagger UI at `http://localhost:8000/docs`, or send a request directly:

```powershell
curl -X POST "http://localhost:8000/pipeline/run" `
  -H "Content-Type: application/json" `
  -d '{
    "price_url": "https://feeds.datafeedwatch.com/25986/cbdd197d9c7747c13f08f840f8bc76eb350292fc.xml",
    "currency_url": "https://www.floatrates.com/",
    "price_topic": "raw-prices",
    "rate_topic": "fx-rates",
    "base_currency": "USD"
  }'
```

The FastAPI endpoint runs the scrapers and publishes events to Kafka. CSV writing
is handled by the Kafka consumer.

## Run The End-To-End Notebook

After `docker-compose up --build` is running, open and run:

[Delta_Nexus_End_to_End_Test.ipynb](Delta_Nexus_End_to_End_Test.ipynb)

The notebook validates the working path:

```text
Notebook
  -> FastAPI /pipeline/run
  -> scrapers
  -> Kafka
  -> KafkaConsumerWriter
  -> CSV
```

The notebook uses unique Kafka topics per run so old Kafka messages do not pollute
the test result.

## Connect To PostgreSQL

Import the schema first:

[database/schema.sql](database/schema.sql)

The code reads the database connection from `DATABASE_URL`.

For a local database named `delta_nexus_test` with user `postgres` and password
`postgres`:

```powershell
$env:DATABASE_URL="postgresql://postgres:postgres@localhost:5432/delta_nexus_test"
```

Check database readiness through FastAPI:

```text
http://localhost:8000/ready
```

Persist Kafka events into PostgreSQL:

```powershell
.\.venv\Scripts\python.exe -m sitemap_exchange_rate_processors.consumers.postgres_consumer `
  --bootstrap-servers localhost:9092 `
  --price-topic raw-prices `
  --rate-topic fx-rates `
  --database-url "postgresql://postgres:postgres@localhost:5432/delta_nexus_test" `
  --timeout-seconds 120
```

When FastAPI runs inside Docker Compose and PostgreSQL runs locally on Windows,
the API container reaches the host database through:

```text
host.docker.internal
```

Update `DATABASE_URL` in `docker-compose.yml` if your local database password or
database name is different.

## Architecture

### High-Level Flow

```text
Client / Notebook / Swagger UI
    |
    v
FastAPI
    |
    v
AsyncPriceScraper ------------------\
                                     +--> KafkaProducerClient
AsyncCurrencyFetcher ---------------/             |
                                                   |
                          +------------------------+------------------+
                          |                                           |
                          v                                           v
                    raw-prices topic                            fx-rates topic
                          |                                           |
                          +------------------------+------------------+
                                                   |
                                                   v
                                          KafkaConsumerWriter
                                                   |
                                                   v
                                             CSV output files
```

### Docker Services

| Service | Purpose | Local URL/Port |
|---|---|---|
| `zookeeper` | Kafka dependency | `localhost:2181` |
| `kafka` | Event broker | `localhost:9092` |
| `kafka-ui` | Kafka topic/message UI | `http://localhost:8080` |
| `api` | FastAPI scraper trigger service | `http://localhost:8000` |

Inside Docker, FastAPI connects to Kafka through `kafka:29092`. From your host
machine, clients connect to Kafka through `localhost:9092`.

## Project Structure

```text
sitemap_exchange_rate_processors/
  backend/
    connect_scraper_with_kafka.py      # FastAPI app
  broker/
    broker.py                          # Shared schemas and broker interface
    kafka_producer.py                  # Kafka producer implementation
    kafka_consumer.py                  # Kafka consumer + CSV writer
  scrapers/
    scrape_sitemaps.py                 # Product XML parsing
    scrape_exchange_rates.py           # FX HTML parsing
    compare_sitemap_exchange_rates.py  # Scraper orchestration helpers
  __init__.py                          # Package exports

docker-compose.yml                     # Kafka, Kafka UI, and FastAPI stack
Dockerfile                             # FastAPI container image
Delta_Nexus_End_to_End_Test.ipynb      # Working E2E notebook
```

## Components

### FastAPI Backend

Main file:

```text
sitemap_exchange_rate_processors/backend/connect_scraper_with_kafka.py
```

Endpoints:

| Endpoint | Purpose |
|---|---|
| `GET /` | Service index |
| `GET /health` | Confirms the API process is running |
| `GET /config` | Shows safe runtime defaults |
| `POST /pipeline/run` | Runs scrapers and publishes records to Kafka |

`POST /pipeline/run` accepts:

| Field | Description |
|---|---|
| `price_url` | XML product feed or sitemap URL |
| `currency_url` | FloatRates-style exchange-rate page |
| `kafka_servers` | Kafka bootstrap servers, defaults from environment |
| `price_topic` | Kafka topic for product events |
| `rate_topic` | Kafka topic for FX events |
| `base_currency` | Base currency for exchange rates, default `USD` |

### Scrapers

`AsyncPriceScraper` downloads an XML feed and parses product records into the
shared `ProductPriceRecord` schema.

`AsyncCurrencyFetcher` downloads a FloatRates-style HTML page and parses exchange
rates into the shared `CurrencyRateRecord` schema.

### Kafka Producer

`KafkaProducerClient` publishes validated events to Kafka topics:

- `raw-prices`
- `fx-rates`

Each Kafka message is wrapped in a versioned JSON event envelope.

### Kafka Consumer

`KafkaConsumerWriter` consumes product and FX events from Kafka, validates the
event schema, and writes CSV files:

- `scraped_product_prices.csv`
- `processed_currency_rates.csv`

## Event Contract

Kafka messages include shared envelope fields:

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

Current schema version:

```text
1.0
```

## Local Python Usage

Install dependencies:

```powershell
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

Run FastAPI locally without Docker:

```powershell
.\.venv\Scripts\uvicorn.exe sitemap_exchange_rate_processors.backend.connect_scraper_with_kafka:app --reload
```

Run the Kafka consumer locally:

```python
import asyncio
from sitemap_exchange_rate_processors import consume_from_kafka

asyncio.run(
    consume_from_kafka(
        bootstrap_servers="localhost:9092",
        output_dir="./outputs",
        timeout_seconds=120,
    )
)
```

## Output Format

Product CSV:

```text
schema_version,event_type,emitted_at,product_id,product_name,product_url,source_url,price,currency,timestamp
```

Currency CSV:

```text
schema_version,event_type,emitted_at,base_currency,target_currency,rate,timestamp
```

## Troubleshooting

### Docker cannot connect to the Docker API

Start Docker Desktop and wait until it says the engine is running. Then retry:

```powershell
docker-compose up --build
```

### ZooKeeper is unhealthy

The Compose file uses the `srvr` command for ZooKeeper health checks because this
image enables `srvr`. If an older container is still running, restart the stack:

```powershell
docker-compose down
docker-compose up --build
```

### Kafka is unavailable

Check that the stack is running:

```powershell
docker-compose ps
```

Kafka should be available from the host at:

```text
localhost:9092
```

FastAPI should use this Docker-internal address when running inside Compose:

```text
kafka:29092
```

### FastAPI is unavailable

Check:

```text
http://localhost:8000/health
```

If it does not respond, inspect the API logs:

```powershell
docker-compose logs api
```

### No CSV files are written

Make sure the Kafka consumer is running. The FastAPI endpoint only scrapes and
publishes to Kafka. CSV persistence happens when `KafkaConsumerWriter` consumes
events from Kafka.

The notebook starts its own consumer and writes to a temporary output directory.

## Roadmap

The next architecture step is durable storage and search:

- PostgreSQL as the source of truth
- Elasticsearch as a rebuildable search index
- FastAPI endpoints backed by PostgreSQL and Elasticsearch

See:

[POSTGRES_ELASTICSEARCH_DATABASE_ARCHITECTURE.md](POSTGRES_ELASTICSEARCH_DATABASE_ARCHITECTURE.md)
