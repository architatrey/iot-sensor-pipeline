# IoT Sensor Data Producer — Requirements & Sample Data Spec

## 1. Overview

This document defines the requirements for the **Python producer script** that simulates IoT sensor devices and publishes their readings to Kafka. This is the entry point of the pipeline described in the project architecture:

```
[Python Producer] → [Kafka Topics] → [Consumer] → [Cassandra]
```

The goal is not just to push random JSON — it's to simulate realistic device behavior so that downstream Kafka partitioning and Cassandra data modeling decisions actually get exercised.

---

## 2. Functional Requirements

| ID | Requirement |
|----|-------------|
| FR1 | Simulate **N virtual devices** (configurable, default 20), each with a unique `device_id`. |
| FR2 | Each device belongs to a `region` and a `device_type` (temperature, humidity, gps, or combined multi-sensor). |
| FR3 | Each device emits one event every `interval_seconds` (configurable, default 1–5s, can be randomized per device to avoid synchronized bursts). |
| FR4 | Events must be serialized as **JSON** (Avro is a stretch goal, not required initially). |
| FR5 | Producer must publish to a Kafka topic named `sensor-readings` (configurable). |
| FR6 | Kafka message **key** must be the `device_id` — this guarantees all events from one device land on the same partition, preserving per-device ordering. |
| FR7 | Producer must occasionally inject **anomalous readings** (e.g., 2–5% of events) to simulate real-world sensor faults/spikes, for testing downstream alerting logic. |
| FR8 | Producer must occasionally inject **out-of-order or duplicate events** (small %) to simulate network retries — useful for testing idempotency downstream. |
| FR9 | Producer must log every message sent (device_id, timestamp, partition, offset) to stdout for debugging. |
| FR10 | Producer must handle Kafka broker unavailability gracefully (retry with backoff, not crash). |
| FR11 | Support **graceful shutdown** (Ctrl+C) — flush any buffered messages before exiting. |

---

## 3. Non-Functional Requirements

| ID | Requirement |
|----|-------------|
| NFR1 | Configurable via environment variables or a `config.yaml` (bootstrap servers, topic name, device count, interval). |
| NFR2 | Should be able to sustain at least **500 events/sec** on a single laptop without blocking (use `confluent-kafka-python` or `kafka-python` with async/batched sends). |
| NFR3 | Use Kafka producer **acks=all** for durability guarantees (configurable, to let you experiment with acks=0/1/all trade-offs later). |
| NFR4 | Include a `schema_version` field in every message so schema evolution can be tested later. |
| NFR5 | Code should be modular: a `Device` class generating readings, and a separate `Producer` wrapper — not one monolithic script. |

---

## 4. Suggested Dependencies

```
confluent-kafka>=2.3.0   # or kafka-python if you prefer pure Python
faker>=20.0.0            # for generating realistic device metadata
python-dotenv>=1.0.0     # for config management
```

---

## 5. Kafka Topic Design

| Topic | Partitions | Key | Purpose |
|-------|-----------|-----|---------|
| `sensor-readings` | 6 (start small, tune later) | `device_id` | Raw sensor events |
| `sensor-alerts` | 3 | `device_id` | Anomalies detected downstream (stretch goal) |

**Why partition by `device_id`:** ensures ordering per device and gives you a natural axis to reason about hot partitions if one device type generates way more traffic than others (good learning exercise later).

---

## 6. Sample Data Schemas

### 6.1 Temperature/Humidity Sensor Event

```json
{
  "schema_version": 1,
  "event_id": "3f2a9b6e-1c4d-4e2a-9f3b-7a8c1d2e3f4a",
  "device_id": "sensor-temp-014",
  "device_type": "temperature_humidity",
  "region": "us-east-1",
  "site": "warehouse-3",
  "timestamp": "2026-08-29T10:15:30.512Z",
  "readings": {
    "temperature_celsius": 24.6,
    "humidity_percent": 58.2
  },
  "battery_percent": 76,
  "status": "OK"
}
```

### 6.2 GPS Tracker Event

```json
{
  "schema_version": 1,
  "event_id": "9c1d2e3f-4a5b-6c7d-8e9f-0a1b2c3d4e5f",
  "device_id": "sensor-gps-007",
  "device_type": "gps",
  "region": "eu-west-1",
  "site": "fleet-vehicle-22",
  "timestamp": "2026-08-29T10:15:31.002Z",
  "readings": {
    "latitude": 52.5121,
    "longitude": 13.3894,
    "speed_kmh": 42.3,
    "heading_degrees": 187
  },
  "battery_percent": 91,
  "status": "OK"
}
```

### 6.3 Multi-Sensor Industrial Device

```json
{
  "schema_version": 1,
  "event_id": "5b6c7d8e-9f0a-1b2c-3d4e-5f6a7b8c9d0e",
  "device_id": "sensor-multi-031",
  "device_type": "industrial_multi",
  "region": "ap-south-1",
  "site": "factory-floor-1",
  "timestamp": "2026-08-29T10:15:31.900Z",
  "readings": {
    "temperature_celsius": 78.4,
    "vibration_hz": 12.7,
    "pressure_kpa": 101.8
  },
  "battery_percent": null,
  "power_source": "mains",
  "status": "OK"
}
```

### 6.4 Anomalous / Faulty Reading (Injected ~2–5% of the time)

```json
{
  "schema_version": 1,
  "event_id": "1a2b3c4d-5e6f-7a8b-9c0d-1e2f3a4b5c6d",
  "device_id": "sensor-temp-014",
  "device_type": "temperature_humidity",
  "region": "us-east-1",
  "site": "warehouse-3",
  "timestamp": "2026-08-29T10:15:36.512Z",
  "readings": {
    "temperature_celsius": 187.9,
    "humidity_percent": -12.0
  },
  "battery_percent": 3,
  "status": "SENSOR_FAULT"
}
```

**Purpose of anomalies:** these give you real cases to build `sensor-alerts` logic against, and stress-test Cassandra's handling of "unexpected" data shapes if you don't validate strictly.

---

## 7. Field Reference

| Field | Type | Notes |
|-------|------|-------|
| `schema_version` | int | Increment when schema changes — enables schema evolution testing later. |
| `event_id` | UUID string | Unique per event; useful for de-duplication downstream. |
| `device_id` | string | Kafka message key. Format: `sensor-<type>-<3-digit-id>`. |
| `device_type` | enum string | `temperature_humidity`, `gps`, `industrial_multi`, etc. |
| `region` | string | Simulated AWS-style region code; drives regional partitioning experiments in Cassandra. |
| `site` | string | Sub-location within a region (warehouse, vehicle, factory floor). |
| `timestamp` | ISO 8601 UTC string | Event generation time — becomes your Cassandra clustering key. |
| `readings` | object | Varies by `device_type`. |
| `battery_percent` | int or null | Null for mains-powered devices. |
| `status` | enum string | `OK`, `SENSOR_FAULT`, `LOW_BATTERY`, `OFFLINE`. |

---

## 8. Configurable Parameters (config.yaml example)

```yaml
kafka:
  bootstrap_servers: "localhost:9092"
  topic: "sensor-readings"
  acks: "all"

simulation:
  device_count: 20
  interval_seconds_min: 1
  interval_seconds_max: 5
  anomaly_rate: 0.03
  duplicate_rate: 0.01
  regions:
    - us-east-1
    - eu-west-1
    - ap-south-1
  device_types:
    - temperature_humidity
    - gps
    - industrial_multi
```

---

## 9. Acceptance Criteria

- [ ] Running `python producer.py` starts emitting events immediately for all configured devices.
- [ ] Messages appear in the `sensor-readings` topic, verifiable via `kafka-console-consumer`.
- [ ] All messages from a given `device_id` land on the same partition (verify via consumer's partition/offset logs).
- [ ] Anomalous and duplicate events appear at roughly the configured rates over a 5-minute run.
- [ ] Script exits cleanly on Ctrl+C with a "flushed N messages" confirmation.
- [ ] Throughput meets NFR2 (≥500 events/sec) when device count and interval are tuned aggressively (e.g., 500 devices, 1s interval).

---

## 10. Next Steps

Once this producer is validated (e.g., using `kafka-console-consumer` to eyeball the stream), the next document should cover:
1. The **consumer** that reads from `sensor-readings` and writes to Cassandra.
2. The **Cassandra keyspace/table schema** designed around the query patterns from the original project plan.