# Real-Time E-Commerce Analytics Pipeline

A full end-to-end data engineering project that simulates an e-commerce platform, streams events in real time, stores and transforms the data through three layers, and serves business metrics on a live dashboard. Every tool in this project is open source and runs locally with Docker.

[![Python](https://img.shields.io/badge/Python-3.10+-blue?style=flat-square&logo=python)](https://python.org)
[![Apache Kafka](https://img.shields.io/badge/Kafka-7.4.0-black?style=flat-square&logo=apachekafka)](https://kafka.apache.org)
[![Apache Spark](https://img.shields.io/badge/Spark-3.5.0-orange?style=flat-square&logo=apachespark)](https://spark.apache.org)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-3.0-blue?style=flat-square)](https://delta.io)
[![dbt](https://img.shields.io/badge/dbt-1.11-red?style=flat-square&logo=dbt)](https://getdbt.com)
[![Airflow](https://img.shields.io/badge/Airflow-2.8-darkgreen?style=flat-square&logo=apacheairflow)](https://airflow.apache.org)
[![Superset](https://img.shields.io/badge/Superset-3.0-red?style=flat-square)](https://superset.apache.org)
[![Docker](https://img.shields.io/badge/Docker-Compose-blue?style=flat-square&logo=docker)](https://docker.com)

---

## Architecture

[View Interactive Architecture Diagram](https://ujusophy.github.io/ecommerce-data-pipeline/)

The pipeline follows a standard production pattern:

```
Python Simulator
      |
      | (JSON events over Kafka)
      v
Apache Kafka
      |
      | (Spark reads the stream)
      v
Spark Structured Streaming
      |
      | (writes raw events)
      v
Bronze Layer  (Delta Lake)
      |
      | (Spark batch job cleans and splits)
      v
Silver Layer  (Delta Lake)
      |
      | (dbt transforms into business metrics)
      v
Gold Layer  (DuckDB)
      |
      | (Airflow runs Silver + Gold hourly)
      v
Apache Airflow  (orchestration)
      |
      | (Superset reads Gold tables)
      v
Apache Superset Dashboard
```

---

## What Each Layer Does

**Bronze** — raw data. Every event is stored exactly as it arrived, with no changes. Think of it as your source of truth. If something breaks downstream, you can always replay from here.

**Silver** — clean data. Duplicates are removed, bad records are filtered out, and events are split into three typed tables: page views, cart events, and orders.

**Gold** — business data. dbt transforms Silver into metrics that answer real questions: What is today's revenue? Which product converts best? Where are users dropping off in the funnel?

---

## Tech Stack

| Layer | Tool | Purpose |
|---|---|---|
| Event generation | Python, Faker | Simulates user behaviour |
| Message broker | Apache Kafka | Decouples producer from consumer |
| Stream processing | Apache Spark 3.5 | Reads Kafka stream in real time |
| Storage format | Delta Lake | ACID transactions on Parquet files |
| Batch transforms | Apache Spark | Bronze to Silver processing |
| Data modelling | dbt + DuckDB | Silver to Gold SQL transforms |
| Orchestration | Apache Airflow 2.8 | Hourly pipeline scheduling |
| Dashboarding | Apache Superset 3.0 | Business metrics visualisation |
| Infrastructure | Docker + Docker Compose | All services run in containers |

---

## Project Structure

```
ecommerce-pipeline/
|
|-- simulator/
|   |-- producer.py          # generates and sends events to Kafka
|   |-- venv/                # Python virtual environment
|
|-- kafka/
|   |-- docker-compose.yml   # all Docker services live here
|
|-- spark/
|   |-- streaming_job.py     # reads Kafka, writes to Bronze Delta Lake
|   |-- silver_job.py        # cleans Bronze, writes to Silver Delta Lake
|
|-- dbt/
|   |-- ecommerce_gold/
|       |-- models/
|           |-- staging/     # thin wrappers over Silver tables
|           |-- marts/       # business metric tables (Gold)
|
|-- airflow/
|   |-- dags/
|       |-- ecommerce_pipeline.py   # the main DAG
|
|-- data/
|   |-- bronze/              # raw Delta Lake files
|   |-- silver/              # cleaned Delta Lake files
|   |-- gold/                # DuckDB database file
|
|-- docs/
    |-- index.html           # interactive architecture diagram
```

---

## Prerequisites

Before you start, make sure you have the following installed:

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) 
- [Python 3.10+](https://www.python.org/downloads/)
- Git

---

## Getting Started

### 1. Clone the repository

```bash
git clone https://github.com/Ujusophy/ecommerce-data-pipeline.git
cd ecommerce-data-pipeline
```

### 2. Start all services

This starts Kafka, Spark, Airflow, Superset, and their dependencies:

```bash
cd kafka
docker-compose up -d
```

Wait about 2 minutes for everything to initialise. Check that all containers are running:

```bash
docker ps
```

You should see: `zookeeper`, `kafka`, `kafka-ui`, `spark`, `airflow`, `airflow-postgres`, `superset`

### 3. Start the event simulator

Open a new terminal:

```bash
cd simulator
python -m venv venv
venv\Scripts\activate
pip install kafka-python faker
python producer.py
```

You will see events printing to the terminal. Leave this running.

### 4. Start the Spark streaming job

Open another terminal:

```bash
docker exec -it spark /opt/spark/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.0.0 \
  /opt/spark-jobs/streaming_job.py
```

This connects to Kafka and starts writing events to the Bronze Delta Lake layer. Leave this running.

### 5. Run the Silver job

Once you have some data in Bronze (give it a minute), open another terminal:

```bash
docker exec -it spark /opt/spark/bin/spark-submit \
  --packages io.delta:delta-spark_2.12:3.0.0 \
  /opt/spark-jobs/silver_job.py
```

This cleans the Bronze data and writes three tables to Silver: `page_views`, `cart_events`, and `orders`.

### 6. Run dbt

```bash
cd dbt/ecommerce_gold
python -m venv venv
venv\Scripts\activate
pip install dbt-core dbt-duckdb duckdb

dbt run
dbt test
```

This builds the Gold layer — three business metric tables in DuckDB.

---

## Accessing the Services

| Service | URL | Login |
|---|---|---|
| Kafka UI | http://localhost:8080 | No login |
| Spark UI | http://localhost:4040 | No login |
| Airflow | http://localhost:8082 | admin / admin123 |
| Superset | http://localhost:8083 | admin / admin123 |
| dbt Docs | http://localhost:8081 | No login |

---

## Running the Automated Pipeline

The Airflow DAG runs the Silver job and dbt every hour automatically. To trigger it manually:

```bash
docker exec -it airflow airflow dags unpause ecommerce_pipeline
docker exec -it airflow airflow dags trigger ecommerce_pipeline
```

Then open the Airflow UI at http://localhost:8082 and watch the tasks run.

The DAG runs five tasks in order:

```
check_bronze_availability
        |
  run_silver_job
        |
  run_dbt_models
        |
   run_dbt_tests
        |
  log_pipeline_summary
```

If any task fails, Airflow retries it twice with a 5-minute wait between attempts.

---

## The Dashboard

Open Superset at http://localhost:8083 and navigate to the `Ecommerce Analytics` dashboard. It shows four charts:

**Total Revenue** — a big number with a trendline showing revenue over time from successful orders only.

**Revenue by Category** — a bar chart breaking down which product categories drive the most revenue.

**User Conversion Funnel** — shows how many users viewed a product, added it to cart, and placed an order. The drop-off between steps reflects a realistic conversion rate.

**Product Performance** — a table ranking every product by revenue, with columns for total views, orders, units sold, and conversion rate.

---

## dbt Models

The Gold layer is built from six dbt models:

```
stg_page_views       (view)   -- wraps silver/page_views
stg_cart_events      (view)   -- wraps silver/cart_events
stg_orders           (view)   -- wraps silver/orders
        |
        v
fct_orders                (table) -- one row per order, revenue calculated
fct_conversion_funnel     (table) -- daily funnel metrics with conversion rates
dim_product_performance   (table) -- product-level aggregates ranked by revenue
```

Run `dbt docs serve --port 8081` to see the full lineage graph in your browser.

---

## Data Quality

dbt runs 8 automated tests every time the pipeline executes:

- `order_id` is unique and not null in `stg_orders`
- `user_id` is not null in `stg_orders`
- `total_amount` is not null in `stg_orders`
- `event_id` is unique and not null in `stg_page_views`
- `event_id` is unique and not null in `stg_cart_events`

The Spark Silver job also applies its own checks before writing:

- Drops records with null `event_id`, `user_id`, or `event_type`
- Removes duplicate events using `event_id`
- Filters out orders missing `order_id` or `total_amount`

---

## Stopping the Pipeline

To stop everything:

```bash
# stop the simulator (in its terminal)
Ctrl+C

# stop the Spark streaming job (in its terminal)
Ctrl+C

# stop all Docker containers
cd kafka
docker-compose down
```

To remove all stored data and start fresh:

```bash
docker-compose down -v
rm -rf ../data/bronze ../data/silver ../data/gold
```

---

## What I Learned Building This

Working through this project from scratch made a few things click that are hard to understand from tutorials alone.

The Medallion Architecture makes sense once you feel the pain of not having it. Before splitting into Bronze, Silver, and Gold, every time I needed to fix something I had to reprocess everything. Having layers means you can fix one layer without touching the others.

Docker networking is not the same as your local network. The bug where Spark could not find Kafka because it was looking at `localhost` instead of the container name `kafka` was a real lesson. In production, services always talk to each other by hostname, not IP.

dbt tests are more valuable than they look at first. Running `dbt test` after every build means you catch data problems before they reach the dashboard. Finding out your revenue numbers are wrong in a dashboard meeting is much worse than a failing test in a terminal.

Idempotency matters more than correctness. The Silver job uses Delta MERGE so it can be re-run without creating duplicates. This seems like extra work until you have a failed run and need to re-run it without fear.
