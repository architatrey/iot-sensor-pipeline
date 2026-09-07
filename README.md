# Real-Time IoT Sensor Pipeline

An enterprise-grade, real-time data streaming pipeline designed to simulate, ingest, process, and store millions of IoT sensor events. 

This project utilizes **Apache Kafka** for ingestion, **Apache Flink** for real-time windowed aggregations and stream routing, and **Apache Cassandra** for high-throughput time-series storage.

## 🏗️ Architecture

```text
[Python IoT Producer] 
       │ (JSON Sensor Data)
       ▼
 [Apache Kafka] (Topic: sensor-readings)
       │
       ▼
 [Apache Flink] (Java Consumer Job)
   ├─ Route A: Filter & Map ───> [Cassandra: sensor_readings_by_device]
   ├─ Route B: Fault Detection ─> [Cassandra: alerts_by_site]
   ├─ Route C: GPS Extraction ──> [Cassandra: gps_tracking_by_vehicle]
   ├─ Route D: Industrial ──────> [Cassandra: industrial_events_by_site]
   └─ Route E: 10s Window Avg ──> [Cassandra: average_temperature_by_site]
```

## 🛠️ Prerequisites
To run this pipeline locally, you will need:
*   **Docker** & **Docker Compose**
*   **Python 3.8+**
*   **Java 11** (or modern Java with `--add-opens` flags)
*   **Maven**

---

## 🚀 How to Run the Pipeline

### Step 1: Start the Infrastructure
Spin up the Kafka broker and the Cassandra database in the background using Docker.
```bash
docker-compose up -d
```
*(Wait about 30-60 seconds for Cassandra to fully initialize).*

### Step 2: Initialize the Database Schema
Execute the setup script to create the `iot_pipeline` Keyspace and all 5 tables in Cassandra.
```bash
docker exec -i cassandra cqlsh < setup_cassandra.cql
```

### Step 3: Start the Python Producer
The producer simulates 20 distinct IoT devices sending data asynchronously. Open a new terminal tab:
```bash
cd producer
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python main.py
```
*(Keep this terminal open so data continues to stream into Kafka).*

### Step 4: Compile & Run the Flink Consumer
The Flink engine will consume the Kafka stream, perform real-time windowed averages, and route the data into the 5 Cassandra tables concurrently.

In your main terminal, compile the Java project:
```bash
cd consumer-flink
mvn clean install
```

**Run the Job:**
*(Note: If you are using Java 16 or newer, you must include the memory access flags so Flink can utilize reflection).*
```bash
java \
  --add-opens java.base/java.lang=ALL-UNNAMED \
  --add-opens java.base/java.util=ALL-UNNAMED \
  --add-opens java.base/java.net=ALL-UNNAMED \
  --add-opens java.base/java.io=ALL-UNNAMED \
  -jar target/consumer-flink-1.0-SNAPSHOT.jar
```

---

## 📊 Viewing the Results

### 1. The Flink Console
In the terminal running the Flink Job, you will see the **10-second Tumbling Window** aggregations printing live:
```text
AVG TEMP -> (warehouse-3, 2026-08-29 10:15:40, 24.80, 15)
```

### 2. The Cassandra Database
To query the processed data, jump into the Cassandra shell:
```bash
docker exec -it cassandra cqlsh
```
Run a few queries against the tables:
```cql
USE iot_pipeline;

-- View recent device alerts
SELECT * FROM alerts_by_site LIMIT 5;

-- View aggregated temperatures
SELECT * FROM average_temperature_by_site LIMIT 10;
```
