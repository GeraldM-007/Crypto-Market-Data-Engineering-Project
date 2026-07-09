# Real-Time Cryptocurrency Market Data Engineering Pipeline

A production-style real-time data engineering pipeline that ingests cryptocurrency market data from the Kraken API, captures database changes using Debezium Change Data Capture (CDC), streams data through Apache Kafka, performs real-time transformations using Apache Spark Structured Streaming, and stores the processed data in Apache Cassandra for fast querying and analytics.

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

# How to Run Locally

## 1. Clone the repository

```bash
https://github.com/GeraldM-007/Crypto-Market-Data-Engineering-Project.git

cd Crypto-Market-Data-Engineering-Project
```

## 2. Configure environment variables

Create a `.env` file in the project root.

Example:

```env
USER = 'postgres'
PASSWORD = 'strongpassword'
HOST = 'database IP/connection string'
PORT = '5432'
DATABASE = 'postgres'
```

## 3. Start the Docker containers

```bash
docker compose up -d
```

This starts:

- PostgreSQL
- Kafka
- Kafka UI
- Debezium Connect
- Spark Master
- Spark Worker
- Spark Submit
- Cassandra

## 4. Create the Cassandra keyspace and tables

```bash
docker exec -it cassandra cqlsh
```

Run the schema:

```sql
CREATE KEYSPACE cryptoproject
WITH replication = {
'class':'SimpleStrategy',
'replication_factor':1
};
```

Create the required tables.

## 5. Configure Debezium

Register the PostgreSQL connector.

```bash
curl -X POST http://localhost:8083/connectors \
-H "Content-Type: application/json" \
-d @connector.json
```

## 6. Start the Airflow DAG

Open Airflow:

```
http://localhost:8080
```

Enable the DAG.

Airflow will begin:

- Pulling Kraken market data
- Loading it into PostgreSQL

## 7. Spark Streaming

Spark automatically starts through `submit.sh`.

It continuously:

- Reads Kafka topics
- Parses Debezium CDC events
- Extracts the market data
- Performs transformations
- Writes the transformed records into Cassandra

## 8. Verify the results

Open Cassandra:

```bash
docker exec -it cassandra cqlsh
```

Example:

```sql
USE cryptoproject;

SELECT * FROM ohlc LIMIT 20;

SELECT * FROM ticker LIMIT 20;
```

# Data Flow

## Step 1 – Data Extraction

Apache Airflow periodically retrieves market data from the Kraken REST API.

Example datasets:

- OHLC candles
- Real-time ticker prices

The extracted data is inserted into PostgreSQL staging tables.

## Step 2 – Change Data Capture

Debezium monitors PostgreSQL transaction logs.

Whenever new records are inserted, updated or deleted, Debezium publishes CDC events into Kafka topics.

Example Kafka topics:

```
test.public.kraken_ohlc
test.public.kraken_ticker
```

## Step 3 – Streaming

Apache Spark subscribes to Kafka topics using Structured Streaming.

Example:

```
Kafka
      ↓
Spark.readStream()
```

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
Example output:

<img width="1002" height="443" alt="sparkoutput" src="https://github.com/user-attachments/assets/06ddca6e-132b-468d-9c02-c334d5712eb4" />

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

## Step 5 – Loading

Spark writes each micro-batch into Apache Cassandra using the Spark Cassandra Connector.

# Cassandra Schema

## OHLC Table
<img width="847" height="317" alt="table" src="https://github.com/user-attachments/assets/96c6a348-f08f-445e-b789-e9fe2239fff9" />

## Ticker Table

Example schema:
<img width="1163" height="333" alt="realtime_table" src="https://github.com/user-attachments/assets/e44670ae-340c-4ed3-b9d5-599668e8f583" />

# Running the Project

Start the infrastructure

```bash
docker compose up -d
```

Verify containers

```bash
docker ps -a
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


# Spark Streaming

The Spark application automatically:

- reads Kafka topics
- parses Debezium CDC events
- transforms Kraken payloads
- writes processed records into Cassandra

# Example Cassandra Query
<img width="1240" height="603" alt="ohlc_output" src="https://github.com/user-attachments/assets/be4f29f8-2057-4062-b446-d4d16316964d" />

---

# NOTE

### connector.json
Json doesnot support comments. Remove all the "comments" after cloning the repository, they are only ment for explanation


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
