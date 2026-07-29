# 🛡️ FinGuard — Real-Time Fraud Detection Pipeline

A real-time, streaming fraud detection system built on Apache Kafka and Databricks, designed to detect and alert on suspicious credit card transactions within seconds of them occurring — not hours or days later in a batch job.

---

## 📌 Overview

Traditional fraud detection often runs in nightly batch jobs, meaning fraud can go unnoticed for hours. FinGuard solves this by continuously streaming transaction data through a fully event-driven pipeline that:

- Ingests live transaction data via **Kafka**
- Cleans and standardizes it in real time using **Spark Structured Streaming**
- Cross-references every transaction against a live **fraud watchlist**
- Flags **high-value transactions** exceeding a customer's normal spending limit
- Detects **transaction volume spikes** (potential bot attacks / card-testing fraud)
- Sends **instant email alerts** to affected customers
- Surfaces everything on a **live monitoring dashboard**

---

## 🏗️ Architecture

```
Kafka Producer (simulated transactions)
        │
        ▼
   Bronze Layer  ── raw ingestion (Kafka + Auto Loader) ──▶ Delta Lake
        │
        ▼
   Silver Layer  ── cleaning, parsing, data quality checks
        │
        ▼
    Gold Layer   ── stream-stream joins, windowed aggregations,
        │            fraud & high-value transaction detection
        ▼
  Alerting Layer ── real-time email notifications (foreach_batch_sink)
        │
        ▼
   Dashboard     ── live fraud monitoring (Databricks Lakeview)
```

Built using a **Medallion Architecture (Bronze → Silver → Gold)** on Databricks Lakeflow Declarative Pipelines (Delta Live Tables).

---

## ⚙️ Tech Stack

| Category | Tools / Technologies |
|---|---|
| **Streaming Ingestion** | Apache Kafka (Confluent Cloud), Databricks Auto Loader |
| **Stream Processing** | PySpark, Spark Structured Streaming |
| **Pipeline Framework** | Databricks Lakeflow Declarative Pipelines (Delta Live Tables) |
| **Storage** | Delta Lake, PostgreSQL |
| **Data Quality** | DLT Expectations (`expect`, `expect_or_drop`) |
| **Streaming Concepts** | Stream-Stream Joins, Event-Time Watermarking, Tumbling & Sliding Window Aggregations |
| **Alerting** | Python `smtplib`, Gmail SMTP, HTML email templating |
| **Security** | Databricks Secret Scopes, Databricks REST API |
| **Data Simulation** | Python, Faker, `python-dotenv` |
| **Dashboarding** | Databricks Lakeview |
| **Languages** | Python, SQL |

---

## 🔄 Pipeline Breakdown

### 1. Kafka Producer (Data Simulation)
A Python-based transaction simulator (`kafka_producer/`) generates realistic customer and merchant datasets and streams synthetic credit card transactions — including configurable fraud patterns (e.g., velocity fraud) — to a Confluent Cloud Kafka topic.

### 2. Bronze Layer (Raw Ingestion)
- `transactions_bronze`: Reads live transaction events directly from Kafka.
- `fraud_watchlist_bronze`: Ingests fraud watchlist updates via Databricks Auto Loader (`cloudFiles`).

### 3. Silver Layer (Cleaning & Validation)
- Parses raw JSON payloads into structured columns.
- Applies data quality rules — dropping records with null transaction/customer/card/merchant IDs and validating transaction amounts.
- Standardizes fraud watchlist entries (entity IDs, risk levels, timestamps).

### 4. Gold Layer (Business Logic & Fraud Detection)
- **`fraud_card_alert`** — Joins live transactions against the fraud watchlist (stream-stream join) using watermarked event-time to catch matches within a 5-minute window.
- **`high_value_transactions_alert`** — Flags transactions exceeding a customer's configured spending limit.
- **`transaction_count_by_minute`** — Tumbling window aggregation to monitor transaction volume.
- **`transaction_count_by_minute_sliding_window`** — Sliding window aggregation to detect sudden spikes indicative of bot/card-testing attacks.

### 5. Real-Time Alerting
A `foreach_batch_sink` streams fraud alerts directly into a Python function that sends formatted HTML fraud alert emails via Gmail SMTP, with credentials securely retrieved from Databricks Secret Scopes.

### 6. Monitoring Dashboard
A Databricks Lakeview dashboard (`FinGuard Fraud Detection Monitoring`) visualizes fraud trends, alert volumes, and transaction activity in real time.

---

## 📁 Project Structure

```
├── kafka_producer/                          # Transaction & fraud data simulator
│   ├── config.py                             # Environment-based configuration
│   ├── customer_generator.py                 # Synthetic customer data
│   ├── merchant_generator.py                 # Synthetic merchant data
│   ├── transaction_generator.py               # Synthetic transaction generation
│   ├── fraud_engine.py                        # Fraud rule/scoring logic
│   ├── producer_normal.py / producer_fraud_*  # Kafka producers
│   └── consumer.py                            # Kafka consumer utility
│
├── databricks notebooks and pipelines/
│   └── finguard_project/
│       ├── finguard_streaming/
│       │   ├── bronze/                        # Raw ingestion (Kafka + Auto Loader)
│       │   ├── silver/                        # Cleaning & validation
│       │   ├── gold/                          # Fraud detection & aggregations
│       │   └── alert/                         # Real-time email notifications
│       ├── finguard_customers_silver_ingestion/
│       ├── fraud_watchlist_file_generator/
│       └── 02_Setup_Secret_Scope.py            # Databricks secret scope setup
│
├── postgres sql/                             # Historical & incremental customer SQL
└── dashboard/                                # Lakeview fraud monitoring dashboard
```

---

## 🚀 Getting Started

### 1. Set up the Kafka producer
```bash
cd kafka_producer
python -m venv .venv
source .venv/bin/activate      # or .venv\Scripts\Activate.ps1 on Windows
pip install -r requirements.txt
cp .env.example .env           # configure Kafka credentials & simulation parameters
python producer_normal.py
```

### 2. Configure Databricks
- Run `02_Setup_Secret_Scope.py` to create the `finguard-scope` secret scope for Kafka and Gmail credentials.
- Deploy the Lakeflow Declarative Pipeline using the notebooks under `finguard_streaming/`.

### 3. Monitor
- Open the Lakeview dashboard to track live fraud alerts and transaction volume.

---

## 📈 Future Improvements

- Add a machine learning–based anomaly scoring model alongside rule-based detection
- Integrate SMS/push notifications in addition to email alerts
- Add Prometheus/Grafana-based pipeline health monitoring
- Support multi-region Kafka topics for higher throughput

---

## 🙌 Notes

This project is built for educational and portfolio purposes, simulating a real-time financial fraud detection system using industry-standard streaming and lakehouse technologies.
