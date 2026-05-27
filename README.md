# Realtime Currency Exchange Data Pipeline

A production-style data engineering project that streams live currency exchange rates through a full pipeline — from API ingestion to a structured data warehouse — using Apache Kafka, PySpark Structured Streaming, and MySQL.

---

## Architecture

```
ExchangeRate API (live rates every 10s)
          │
          ▼
    Kafka Producer
    (producer.py)
          │
          ▼
  Kafka Topic: exchange-rates
          │
          ▼
  PySpark Structured Streaming
      (consumer.py)
          │
    ┌─────┴─────┐
    ▼           ▼
Parquet      MySQL
Data Lake    Warehouse
  (raw)     (aggregated)
```

- **producer.py** — fetches live USD exchange rates for NPR, EUR, GBP, JPY, INR, AUD from the ExchangeRate API every 10 seconds and publishes each currency as a separate Kafka message
- **consumer.py** — PySpark Structured Streaming job that reads from Kafka, parses JSON messages, writes raw records to Parquet files (data lake layer) and computes rolling averages displayed on console
- **warehouse.py** — batch ETL job that reads accumulated Parquet data, computes daily summaries and latest rates using Spark SQL, and loads results into MySQL (data warehouse layer)

---

## Tech Stack

| Layer | Technology |
|-------|------------|
| Message broker | Apache Kafka 3.7.1 |
| Stream processing | Apache Spark 3.5.3 (PySpark) |
| Data lake | Parquet files (local filesystem) |
| Data warehouse | MySQL 8.4.8 |
| API integration | ExchangeRate API (REST) |
| Python libraries | Python, kafka-python, requests |
| Environment | Ubuntu (WSL), Java 17 |
| Version control | Git, GitHub |
| CI/CD | GitHub Actions |

---

## Project Structure

```
realtime-currency-exchange-data-pipeline/
├── producer.py          # Kafka producer — API ingestion
├── consumer.py          # PySpark streaming consumer — data lake
├── warehouse.py         # Batch warehouse loader — MySQL
├── .env                 # API key and DB credentials (not committed)
├── .gitignore
├── requirements.txt
└── .github/
    └── workflows/
        └── ci.yml       # GitHub Actions CI pipeline
```

---

## Data Flow

### Data Lake (Parquet)
Raw individual exchange rate readings stored as-is:

| base_currency | target_currency | exchange_rate | timestamp |
|---------------|-----------------|---------------|-----------|
| USD | NPR | 153.99 | 2026-05-22 10:30:00 |
| USD | EUR | 0.86 | 2026-05-22 10:30:10 |

### Data Warehouse (MySQL)

**daily_rates** — aggregated daily summary per currency:

| currency | avg_rate | min_rate | max_rate | rate_date |
|----------|----------|----------|----------|-----------|
| NPR | 153.97 | 153.50 | 154.20 | 2026-05-22 |
| EUR | 0.86 | 0.85 | 0.87 | 2026-05-22 |

**latest_rates** — most recent rate per currency:

| currency | rate | last_updated |
|----------|------|--------------|
| NPR | 153.99 | 2026-05-22 14:12:09 |
| EUR | 0.86 | 2026-05-22 14:12:09 |

---

## Setup & Running

### Prerequisites
- Ubuntu / WSL with Java 17
- Python 3.14.4
- Apache Kafka 3.7.1
- MySQL 8.4.8

### Installation

**1. Clone the repository:**
```bash
git clone https://github.com/Prateekmaharjan/realtime-currency-exchange-data-pipeline.git
cd realtime-currency-exchange-data-pipeline
```

**2. Create virtual environment and install dependencies:**
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

**3. Create .env file:**
```bash
cp .env.example .env
# Add your API key and MySQL credentials
```

**4. Set up MySQL database:**
```sql
CREATE DATABASE currency_warehouse;
CREATE USER 'warehouse_user'@'localhost' IDENTIFIED BY 'your_password';
GRANT ALL PRIVILEGES ON currency_warehouse.* TO 'warehouse_user'@'localhost';
```

### Running the Pipeline

**Terminal 1 — Start ZooKeeper:**
```bash
cd ~/kafka
bin/zookeeper-server-start.sh config/zookeeper.properties
```

**Terminal 2 — Start Kafka broker:**
```bash
cd ~/kafka
bin/kafka-server-start.sh config/server.properties
```

**Terminal 3 — Create topic (first time only):**
```bash
bin/kafka-topics.sh --create --topic exchange-rates --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

**Terminal 4 — Start producer:**
```bash
source venv/bin/activate
python3 producer.py
```

**Terminal 5 — Start consumer:**
```bash
source venv/bin/activate
python3 consumer.py
```

**Terminal 6 — Run warehouse loader (run once daily or as needed):**
```bash
source venv/bin/activate
python3 warehouse.py
```

---

## Key Concepts Demonstrated

- **Real-time streaming** — Kafka producer-consumer pattern with 10 second micro-batches
- **ETL/ELT pipeline** — extract from API, transform in PySpark, load to MySQL
- **Data lake vs data warehouse** — raw Parquet storage vs aggregated MySQL tables
- **Star schema design** — structured warehouse tables optimized for analytical queries
- **Spark SQL** — SQL queries on streaming DataFrames using temporary views
- **PySpark DataFrame API** — structured streaming with schema definition and JSON parsing
- **Data governance** — schema enforcement, null handling, unique constraints, checkpoint recovery
- **CI/CD** — automated syntax validation on every push via GitHub Actions
- **Batch vs streaming** — consumer.py (streaming) vs warehouse.py (batch ETL)

---

## Fintech Relevance

This pipeline mirrors real-world fintech data engineering patterns used in currency exchange and payment processing systems:

- **Exchange rate monitoring** — financial institutions track currency rates continuously for transaction pricing
- **Data reconciliation** — daily summaries in MySQL enable end-of-day reconciliation reporting
- **Audit trail** — raw Parquet files preserve complete historical record for compliance purposes
- **Lambda architecture** — speed layer (streaming consumer) + batch layer (warehouse loader) running simultaneously

---

## Author

**Pratik Maharjan**
- Email: maharjanpratik009@gmail.com
- GitHub: [Prateekmaharjan](https://github.com/Prateekmaharjan)
