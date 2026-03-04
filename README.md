# 🛒 Real-Time E-Commerce Analytics Pipeline

> An end-to-end data engineering project simulating a production-grade e-commerce analytics platform — from live event streaming to a business intelligence dashboard.

[![Python](https://img.shields.io/badge/Python-3.10+-blue?style=flat-square&logo=python)](https://python.org)
[![Apache Kafka](https://img.shields.io/badge/Kafka-7.4.0-black?style=flat-square&logo=apachekafka)](https://kafka.apache.org)
[![Apache Spark](https://img.shields.io/badge/Spark-3.5.0-orange?style=flat-square&logo=apachespark)](https://spark.apache.org)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-3.0-blue?style=flat-square)](https://delta.io)
[![dbt](https://img.shields.io/badge/dbt-1.11-red?style=flat-square&logo=dbt)](https://getdbt.com)
[![Airflow](https://img.shields.io/badge/Airflow-2.8-darkgreen?style=flat-square&logo=apacheairflow)](https://airflow.apache.org)
[![Superset](https://img.shields.io/badge/Superset-3.0-red?style=flat-square)](https://superset.apache.org)
[![Docker](https://img.shields.io/badge/Docker-Compose-blue?style=flat-square&logo=docker)](https://docker.com)

---

## 📌 Table of Contents

- [Project Overview](#-project-overview)
- [Architecture](#-architecture)
- [Tech Stack](#-tech-stack)
- [Data Flow](#-data-flow)
- [Project Structure](#-project-structure)
- [Getting Started](#-getting-started)
- [Running the Pipeline](#-running-the-pipeline)
- [Data Layers](#-data-layers)
- [dbt Models](#-dbt-models)
- [Airflow DAG](#-airflow-dag)
- [Dashboard](#-dashboard)
- [Key Engineering Decisions](#-key-engineering-decisions)
- [What I Learned](#-what-i-learned)

---

## 📖 Project Overview

This project builds a complete, production-style data pipeline for an e-commerce platform. It demonstrates how modern data engineering teams handle high-volume event streams — from raw clickstream data all the way to business dashboards.

### What it simulates

A Python script acts as a live e-commerce website, generating realistic user behaviour events:

- **Page views** — users browsing products
- **Add to cart** — users adding items to their basket
- **Orders placed** — completed purchases with payment status

These events stream through Kafka into Spark, get stored in a Delta Lake medallion architecture (Bronze → Silver → Gold), get transformed by dbt into business metrics, orchestrated hourly by Airflow, and visualized in Apache Superset.

### Why this project matters

Most data engineering portfolios show batch ETL scripts. This project demonstrates:

- **Real-time streaming** with Kafka and Spark Structured Streaming
- **Lakehouse architecture** with Delta Lake ACID transactions
- **Data quality enforcement** at every layer
- **Production patterns** like idempotent upserts, checkpointing, and schema enforcement
- **End-to-end orchestration** with monitoring and retries

---

## 🏗️ Architecture

👉 [**View Interactive Architecture Diagram**](https://YOUR_USERNAME.github.io/ecommerce-data-pipeline/)

```
┌─────────────────────────────────────────────────────────────────────┐
│                         INGESTION LAYER                             │
│                                                                     │
│   ┌─────────────────┐          ┌──────────────────────────────┐    │
│   │  Python          │  events  │  Apache Kafka                │    │
│   │  Event           │ ──────►  │  topic: ecommerce_events     │    │
│   │  Simulator       │          │  port: 9092 (ext)            │    │
│   │  (Faker lib)     │          │        29092 (internal)      │    │
│   └─────────────────┘          └──────────────┬───────────────┘    │
└──────────────────────────────────────────────┼────────────────────┘
                                               │ Structured Streaming
┌──────────────────────────────────────────────▼────────────────────┐
│                        PROCESSING LAYER                            │
│                                                                    │
│   ┌────────────────────────────────────────────────────────────┐  │
│   │  Apache Spark 3.5 (Structured Streaming)                   │  │
│   │  • Reads Kafka stream → parses JSON → enforces schema      │  │
│   │  • Adds ingestion timestamps                               │  │
│   │  • Writes to Delta Lake in append mode with checkpointing  │  │
│   └───────────────────────────────┬────────────────────────────┘  │
└───────────────────────────────────┼───────────────────────────────┘
                                    │ Delta Lake writes
┌───────────────────────────────────▼───────────────────────────────┐
│                      MEDALLION STORAGE                             │
│                                                                    │
│  🥉 BRONZE              🥈 SILVER              🥇 GOLD             │
│  ┌──────────────┐      ┌──────────────┐      ┌──────────────┐    │
│  │ Raw events   │ ───► │ page_views   │ ───► │ fct_orders   │    │
│  │ all types    │      │ cart_events  │      │ fct_funnel   │    │
│  │ unmodified   │      │ orders       │      │ dim_products │    │
│  │              │      │ deduplicated │      │              │    │
│  │ append only  │      │ validated    │      │ aggregated   │    │
│  └──────────────┘      └──────────────┘      └──────────────┘    │
└───────────────────────────────────────────────────────────────────┘
                                    │
                                    │ dbt SQL transforms
┌───────────────────────────────────▼───────────────────────────────┐
│                      ORCHESTRATION                                 │
│                                                                    │
│   Apache Airflow 2.8 — runs every hour                            │
│   check_bronze ──► run_silver ──► run_dbt ──► dbt_tests ──► done  │
└───────────────────────────────────┬───────────────────────────────┘
                                    │
┌───────────────────────────────────▼───────────────────────────────┐
│                         SERVING LAYER                              │
│                                                                    │
│   Apache Superset — Business Intelligence Dashboard                │
│   • Total Revenue KPI    • Revenue by Category                    │
│   • Conversion Funnel    • Product Performance Table              │
└───────────────────────────────────────────────────────────────────┘
```

---

## 🛠️ Tech Stack

| Layer | Tool | Purpose |
|---|---|---|
| Event Generation | Python + Faker | Simulate realistic e-commerce events |
| Message Broker | Apache Kafka | Decouple producers from consumers |
| Stream Processing | Apache Spark 3.5 | Process events in real time |
| Storage | Delta Lake | ACID transactions on Parquet files |
| Transformation | dbt + DuckDB | SQL-based Gold layer modeling |
| Orchestration | Apache Airflow 2.8 | Schedule and monitor the pipeline |
| Visualization | Apache Superset | Business intelligence dashboard |
| Infrastructure | Docker Compose | Run all services locally |

---

## 🔄 Data Flow

### Event Schema

Every event produced by the simulator follows this schema:

```json
{
  "event_type": "order_placed",
  "event_id": "uuid4",
  "user_id": "uuid4",
  "timestamp": "2026-03-04T14:22:00.000Z",
  "product_id": "P001",
  "product_name": "Wireless Headphones",
  "category": "Electronics",
  "quantity": 2,
  "price": 89.99,
  "total_amount": 179.98,
  "payment_method": "credit_card",
  "status": "success"
}
```

### Conversion Funnel

The simulator replicates a realistic e-commerce funnel:

```
100% — Page View       (every user session starts here)
 60% — Add to Cart     (60% of viewers add an item)
 24% — Order Placed    (40% of cart adders complete purchase)
 18% — Successful      (75% of orders succeed, 25% fail)
```

---

## 📁 Project Structure

```
ecommerce-pipeline/
│
├── simulator/                  # Event producer
│   ├── venv/
│   └── producer.py             # Kafka producer — generates user events
│
├── kafka/                      # Infrastructure
│   └── docker-compose.yml      # All services: Kafka, Spark, Airflow, Superset
│
├── spark/                      # Spark jobs
│   ├── streaming_job.py        # Bronze layer — Kafka → Delta Lake (streaming)
│   └── silver_job.py           # Silver layer — clean, deduplicate, split
│
├── dbt/
│   └── ecommerce_gold/         # dbt project
│       ├── dbt_project.yml
│       ├── models/
│       │   ├── staging/
│       │   │   ├── stg_page_views.sql
│       │   │   ├── stg_cart_events.sql
│       │   │   ├── stg_orders.sql
│       │   │   └── schema.yml      # data quality tests
│       │   └── marts/
│       │       ├── fct_orders.sql
│       │       ├── fct_conversion_funnel.sql
│       │       └── dim_product_performance.sql
│       └── profiles.yml
│
├── airflow/
│   └── dags/
│       └── ecommerce_pipeline.py   # Hourly DAG
│
├── data/                       # Delta Lake storage (git-ignored)
│   ├── bronze/
│   ├── silver/
│   └── gold/
│
└── docs/
    └── index.html              # Interactive architecture diagram
```

---

## 🚀 Getting Started

### Prerequisites

Make sure you have these installed:

| Tool | Version | Download |
|---|---|---|
| Docker Desktop | Latest | [docker.com](https://docker.com/products/docker-desktop) |
| Python | 3.10+ | [python.org](https://python.org/downloads) |
| Git | Latest | [git-scm.com](https://git-scm.com) |

> ⚠️ On Windows, ensure Docker Desktop uses WSL 2 (not Hyper-V) and Python is added to PATH during installation.

### Clone the Repository

```bash
git clone https://github.com/YOUR_USERNAME/ecommerce-data-pipeline.git
cd ecommerce-data-pipeline
```

### Start All Services

```bash
cd kafka
docker-compose up -d
```

This spins up 6 containers:

| Container | Purpose | Port |
|---|---|---|
| `zookeeper` | Kafka cluster manager | 2181 |
| `kafka` | Message broker | 9092 |
| `kafka-ui` | Kafka visual dashboard | 8080 |
| `spark` | Stream + batch processing | 4040 |
| `airflow` | Pipeline orchestration | 8082 |
| `superset` | BI dashboard | 8083 |

Verify all containers are running:

```bash
docker ps
```

---

## ▶️ Running the Pipeline

### Step 1 — Start the Event Simulator

Opens a terminal and starts streaming fake e-commerce events to Kafka:

```bash
cd simulator
python -m venv venv
venv\Scripts\activate        # Windows
pip install kafka-python faker
python producer.py
```

You will see events streaming in real time:
```
🚀 Starting e-commerce event simulator...
[PAGE VIEW]   user=a3f92b1c...  product=Wireless Headphones
[ADD TO CART] user=a3f92b1c...  product=Wireless Headphones
[ORDER]       user=a3f92b1c...  product=Wireless Headphones  total=$179.98
```

Verify events in Kafka UI → http://localhost:8080 → Topics → `ecommerce_events`

### Step 2 — Start Spark Streaming (Bronze Layer)

In a new terminal, start consuming events from Kafka and writing to Delta Lake:

```bash
docker exec -it spark /opt/spark/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.0.0 \
  /opt/spark-jobs/streaming_job.py
```

Monitor at Spark UI → http://localhost:4040 → Streaming tab

### Step 3 — Run Silver Layer

Cleans, deduplicates, and splits Bronze into typed tables:

```bash
docker exec -it spark /opt/spark/bin/spark-submit \
  --packages io.delta:delta-spark_2.12:3.0.0 \
  /opt/spark-jobs/silver_job.py
```

### Step 4 — Run dbt Gold Layer

Builds business-level aggregations from Silver tables:

```bash
cd dbt/ecommerce_gold
venv\Scripts\activate
dbt run       # build all 6 models
dbt test      # run 8 data quality tests
dbt docs generate && dbt docs serve --port 8081
```

View lineage graph → http://localhost:8081

### Step 5 — Trigger Airflow DAG

Airflow automates Steps 3 and 4 on an hourly schedule:

```bash
# Sync DAG to database
docker exec -it airflow airflow dags reserialize

# Unpause and trigger
docker exec -it airflow airflow dags unpause ecommerce_pipeline
docker exec -it airflow airflow dags trigger ecommerce_pipeline
```

Monitor at Airflow UI → http://localhost:8082 (admin / admin123)

### Step 6 — View Dashboard

Open Superset → http://localhost:8083 (admin / admin123)

Navigate to Dashboards → **Ecommerce Analytics**

---

## 🗄️ Data Layers

### Bronze Layer
- **Path:** `data/bronze/ecommerce_events/`
- **Format:** Delta Lake (Parquet + transaction log)
- **Written by:** Spark Structured Streaming
- **Contents:** Raw, unmodified events — all types in one table
- **Rule:** Append-only. Nothing is ever deleted from Bronze.

### Silver Layer
- **Path:** `data/silver/`
- **Format:** Delta Lake
- **Written by:** `silver_job.py` via Spark batch
- **Tables:** `page_views`, `cart_events`, `orders`
- **Transformations applied:**
  - Null checks on `event_id`, `user_id`, `event_type`
  - Deduplication on `event_id`
  - Type casting (string timestamps → proper timestamps)
  - Business rule: `is_failed` flag on orders
  - Idempotent upserts via Delta MERGE

### Gold Layer
- **Path:** `data/gold/ecommerce.duckdb`
- **Format:** DuckDB database
- **Written by:** dbt
- **Models:** `fct_orders`, `fct_conversion_funnel`, `dim_product_performance`

---

## 📐 dbt Models

### Lineage Graph

```
stg_page_views ────────────────────────────────────────────┐
                                                            ▼
stg_cart_events ────────────────────────► fct_conversion_funnel
                                                            
stg_orders ──────────────► fct_orders ──► dim_product_performance
```

### Model Descriptions

| Model | Type | Description |
|---|---|---|
| `stg_page_views` | View | Wraps Silver page_views, renames columns |
| `stg_cart_events` | View | Wraps Silver cart_events, adds line_total |
| `stg_orders` | View | Wraps Silver orders, casts types |
| `fct_orders` | Table | One row per order with revenue flag |
| `fct_conversion_funnel` | Table | Daily funnel metrics and conversion rates |
| `dim_product_performance` | Table | Per-product views, orders, revenue, conversion |

### Data Quality Tests

```yaml
# 8 tests run automatically on every dbt run
stg_orders:     order_id (unique, not_null), user_id (not_null), total_amount (not_null)
stg_page_views: event_id (unique, not_null)
stg_cart_events: event_id (unique, not_null)
```

---

## 🌀 Airflow DAG

**Schedule:** `0 * * * *` (every hour at :00)

```
check_bronze_availability
         │
         ▼
    run_silver_job          ← spark-submit silver_job.py
         │
         ▼
    run_dbt_models          ← dbt run
         │
         ▼
    run_dbt_tests           ← dbt test
         │
         ▼
   log_pipeline_summary     ← logs execution stats
```

**Reliability features:**
- `retries: 2` on every task
- `retry_delay: 5 minutes`
- `execution_timeout` per task (30 min Silver, 15 min dbt)
- Bronze health check before running expensive Spark jobs
- `trigger_rule: all_success` on summary task

---

## 📊 Dashboard

The Superset dashboard (`Ecommerce Analytics`) contains four charts:

| Chart | Type | Dataset | Metric |
|---|---|---|---|
| Total Revenue | Big Number + Trendline | `fct_orders` | `SUM(revenue)` |
| Revenue by Category | Bar Chart | `fct_orders` | `SUM(revenue)` grouped by `category` |
| Conversion Funnel | Funnel Chart | `fct_conversion_funnel` | viewed → carted → ordered |
| Product Performance | Table | `dim_product_performance` | all KPIs sorted by revenue |

---

## 🧠 Key Engineering Decisions

**Why Kafka over a direct database write?**
Kafka decouples the producer (simulator) from the consumer (Spark). This means either side can go down and recover independently. In production, this handles traffic spikes without data loss.

**Why Delta Lake over plain Parquet?**
Delta Lake adds ACID transactions, schema enforcement, and time travel to plain Parquet files. The `_delta_log/` transaction log means you can audit every write and roll back bad data — critical for production pipelines.

**Why separate Bronze/Silver/Gold?**
The Medallion architecture ensures that raw data is always preserved (Bronze), cleaned data is reliable (Silver), and business logic is centralized (Gold). You can reprocess any layer without losing the original data.

**Why dbt for the Gold layer?**
dbt treats SQL transformations as code — versioned, tested, and documented. The `ref()` function builds a dependency graph so models always run in the right order, and the lineage graph makes it easy to understand data flow.

**Why DuckDB for dbt instead of Spark?**
DuckDB runs in-process with no server needed and queries Parquet files directly. For the Gold layer (small aggregated tables), it's dramatically faster and simpler than running another Spark job.

**Why Airflow over a cron job?**
Airflow gives visibility, retries, alerting, and dependency management. A cron job just runs silently — you don't know if it failed until your dashboard shows stale data.

---

## 📚 What I Learned

Building this project end-to-end taught me:

- **Docker networking** — containers can't reach each other via `localhost`. Internal services need to communicate by container name (e.g. `kafka:29092`)
- **Kafka listener configuration** — separating internal and external listeners is essential when mixing Docker and host-based clients
- **Spark Structured Streaming** — treating a live stream like a continuously growing table is a powerful mental model
- **Delta Lake MERGE** — upsert operations make batch jobs idempotent, meaning they're safe to re-run without creating duplicates
- **dbt ref()** — dependency management through `ref()` eliminates entire categories of ordering bugs
- **Airflow DAG serialization** — DAGs must be serialized to the metadata DB before they appear in the UI
- **The real value of the Medallion architecture** — having Bronze means you can always reprocess from raw data when you find a bug in Silver or Gold

---

## 🌐 Services Reference

| Service | URL | Credentials |
|---|---|---|
| Kafka UI | http://localhost:8080 | — |
| Spark UI | http://localhost:4040 | — |
| Airflow | http://localhost:8082 | admin / admin123 |
| dbt Docs | http://localhost:8081 | — |
| Superset | http://localhost:8083 | admin / admin123 |

---

## 📄 License

MIT License — feel free to use this project as a reference or starting point for your own data engineering work.

---

*Built as a data engineering portfolio project — demonstrating real-time streaming, lakehouse architecture, SQL transformation, orchestration, and business intelligence in a single end-to-end pipeline.*
