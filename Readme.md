# Real-Time Cryptocurrency Market Data Engineering Pipeline

A production-style real-time data engineering pipeline that ingests cryptocurrency market data from the Kraken API, captures database changes using Debezium Change Data Capture (CDC), streams data through Apache Kafka, performs real-time transformations using Apache Spark Structured Streaming, and stores the processed data in Apache Cassandra for fast querying and analytics.

---

## Project Architecture

```text
                    +----------------------+
                    |    Kraken API        |
                    +----------+-----------+
                               |
                               |
                               v
                    +----------------------+
                    |      Airflow         |
                    | (Data Extraction)    |
                    +----------+-----------+
                               |
                               |
                               v
                    +----------------------+
                    | PostgreSQL (Staging) |
                    +----------+-----------+
                               |
                     Debezium CDC Connector
                               |
                               v
                    +----------------------+
                    |    Apache Kafka      |
                    |  (Streaming Topics)  |
                    +----------+-----------+
                               |
                +--------------+---------------+
                |                              |
                |                              |
                v                              v
        OHLC Topic                     Ticker Topic
                |                              |
                |                              |
                +--------------+---------------+
                               |
                               v
                Apache Spark Structured Streaming
                               |
                 +-------------+--------------+
                 |                            |
                 |                            |
                 v                            v
           OHLC Transformation         Ticker Transformation
                 |                            |
                 +-------------+--------------+
                               |
                               v
                     Apache Cassandra
```

---

## Features

- Real-time cryptocurrency data ingestion
- Automated scheduling using Apache Airflow
- PostgreSQL staging database
- Change Data Capture (CDC) using Debezium
- Apache Kafka event streaming
- Spark Structured Streaming transformations
- Separate processing pipelines for:
  - OHLC (candlestick) market data
  - Real-time ticker updates
- Storage in Apache Cassandra
- Modular Spark architecture
- Containerized infrastructure using Docker Compose

---

# Technology Stack

| Component | Purpose |
|-----------|---------|
| Apache Airflow | Workflow orchestration |
| PostgreSQL | Staging database |
| Debezium | PostgreSQL Change Data Capture |
| Apache Kafka | Event streaming |
| Apache Spark Structured Streaming | Real-time processing |
| Apache Cassandra | NoSQL data storage |
| Docker Compose | Infrastructure deployment |
| Python | ETL implementation |

---

# Project Structure

```
.
├── dags/
│   └── ...
│
├── connector.json
├── docker-compose.yml
├── load.py
├── ohlc.py
├── ticker.py
├── requirements.txt
├── submit.sh
├── README.md
```

---

# Data Flow

## Step 1 – Data Extraction

Apache Airflow periodically retrieves market data from the Kraken REST API.

Example datasets:

- OHLC candles
- Real-time ticker prices

The extracted data is inserted into PostgreSQL staging tables.

---

## Step 2 – Change Data Capture

Debezium monitors PostgreSQL transaction logs.

Whenever new records are inserted, updated or deleted, Debezium publishes CDC events into Kafka topics.

Example Kafka topics:

```
test.public.kraken_ohlc
test.public.kraken_ticker
```

**Note** 
### connector.json
Json doesnot support comments. Remove all the "comments" after cloning the repository, they are only ment for explanation

---

## Step 3 – Streaming

Apache Spark subscribes to Kafka topics using Structured Streaming.

Example:

```
Kafka
      ↓
Spark.readStream()
```

---

## Step 4 – Transformation

The incoming Kafka messages are Debezium envelopes.

Spark extracts only the required fields from the CDC event.

For OHLC:

```
Kafka Event
    ↓
Debezium JSON
    ↓
Extract "after"
    ↓
Extract "data"
    ↓
Parse Kraken JSON
    ↓
Flatten nested arrays
    ↓
Create structured DataFrame
```

Output schema:

| Column |
|---------|
| symbol |
| candle_time |
| open |
| high |
| low |
| close |
| volume |
| vwap |
| trades |
| ingestion_time |

---

For Ticker:

```
Kafka Event
    ↓
Debezium JSON
    ↓
Extract "after"
    ↓
Parse ticker JSON
    ↓
Extract bid, ask, last trade, volume, etc.
    ↓
Write to Cassandra
```

---

## Step 5 – Loading

Spark writes each micro-batch into Apache Cassandra using the Spark Cassandra Connector.

---

# Cassandra Schema

## OHLC Table

```sql
CREATE TABLE cryptoproject.ohlc (

    symbol text,

    candle_time timestamp,

    open double,

    high double,

    low double,

    close double,

    vwap double,

    volume double,

    trades int,

    ingestion_time timestamp,

    PRIMARY KEY ((symbol), candle_time)

) WITH CLUSTERING ORDER BY (candle_time DESC);
```

---

## Ticker Table

Example schema:

```sql
CREATE TABLE cryptoproject.ticker (

    symbol text,

    ingestion_time timestamp,

    ask_price double,

    ask_volume double,

    bid_price double,

    bid_volume double,

    last_trade_price double,

    last_trade_volume double,

    PRIMARY KEY ((symbol), ingestion_time)

) WITH CLUSTERING ORDER BY (ingestion_time DESC);
```

---

# Running the Project

Start the infrastructure

```bash
docker compose up -d
```

Verify containers

```bash
docker ps
```

Useful services:

| Service | Port |
|----------|------|
| Airflow | 8080 |
| Kafka UI | 8081 |
| Spark Master | 8090 |
| Kafka | 9092 |
| PostgreSQL | 5432 |
| Cassandra | 9042 |

---

# Spark Streaming

The Spark application automatically:

- reads Kafka topics
- parses Debezium CDC events
- transforms Kraken payloads
- writes processed records into Cassandra

---

# Example Cassandra Query

Latest candles for Bitcoin:

```sql
SELECT *
FROM cryptoproject.ohlc
WHERE symbol='BTCUSD'
LIMIT 20;
```

Latest ticker values:

```sql
SELECT *
FROM cryptoproject.ticker
WHERE symbol='BTCUSD'
LIMIT 20;
```

---

# Learning Outcomes

This project demonstrates:

- Real-time ETL design
- Event-driven architecture
- Change Data Capture (CDC)
- Kafka streaming
- Spark Structured Streaming
- NoSQL data modeling with Cassandra
- Docker-based infrastructure
- Modular Spark application development

# License

This project is licensed under the MIT License.