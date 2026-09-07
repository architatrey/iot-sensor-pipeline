# Cassandra Data Modeling & Query Patterns

This document outlines the core business queries that our IoT Sensor Pipeline must support. Because we are using Apache Cassandra, our database tables must be modeled strictly around these specific read patterns rather than relying on relational joins.

## Core Business Queries

### 1. The "Current State" Dashboard
*   **User Question:** "What is the most recent reading, battery level, and status for a specific device?"
*   **Data Requirements:** Fast key-value lookup by `device_id` to fetch the absolute latest event.
*   **Purpose:** Operational monitoring to check the immediate status of any single sensor on a dashboard.

### 2. The "Historical Trend" (Time-Series)
*   **User Question:** "What were the readings (e.g., temperature, humidity) for a specific device over the last 24 hours?"
*   **Data Requirements:** Query by `device_id` bounded by a specific time range (`timestamp`). 
*   **Purpose:** Trend analysis and generating time-series graphs to track environmental or device performance over time.

### 3. The "Asset Tracking" (GPS)
*   **User Question:** "What route did a specific vehicle (e.g., `site`: 'fleet-vehicle-22') take between 8:00 AM and 5:00 PM yesterday, and what was its top speed?"
*   **Data Requirements:** Query by `site` (or `device_id`) and time range, extracting `latitude`, `longitude`, and `speed_kmh`.
*   **Purpose:** Fleet management, plotting historical routes on a map, and monitoring speed compliance.

### 4. Site Maintenance & Alerts
*   **User Question:** "Show me all devices that reported a `SENSOR_FAULT` or `LOW_BATTERY` at a specific site (e.g., 'warehouse-3') in the last 7 days."
*   **Data Requirements:** Query by `site` and `status` over a time window.
*   **Purpose:** Preventative maintenance, allowing operators to quickly dispatch technicians to locations with failing hardware.

### 5. Industrial Auditing
*   **User Question:** "Show me all readings from `industrial_multi` sensors on `factory-floor-1` in the last hour."
*   **Data Requirements:** Query by `site` and `device_type` over a short time window.
*   **Purpose:** Aggregated monitoring of complex industrial machinery to check for anomalies like abnormal vibrations or pressure drops across a whole facility.

---

## Cassandra Table Schemas (DDL) & Queries

Because Cassandra uses Query-First Design, we create specific tables optimized for the queries above. Data duplication across these tables is expected to achieve fast read performance.

### Table 1: `sensor_readings_by_device`
Answers **Query 1** (Current State) and **Query 2** (Historical Trend).

```cql
CREATE TABLE sensor_readings_by_device (
    device_id text,
    timestamp timestamp,
    device_type text,
    region text,
    site text,
    battery_percent int,
    status text,
    readings text, -- Stored as JSON string
    PRIMARY KEY ((device_id), timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC);
```

**Sample Queries:**
*   Current State: `SELECT * FROM sensor_readings_by_device WHERE device_id = 'sensor-temp-014' LIMIT 1;`
*   Historical Trend: `SELECT * FROM sensor_readings_by_device WHERE device_id = 'sensor-temp-014' AND timestamp >= '2026-09-05T00:00:00Z';`

### Table 2: `gps_tracking_by_vehicle`
Answers **Query 3** (Asset Tracking). Extracted GPS fields for easier mapping.

```cql
CREATE TABLE gps_tracking_by_vehicle (
    site text,
    timestamp timestamp,
    device_id text,
    latitude double,
    longitude double,
    speed_kmh double,
    heading_degrees int,
    PRIMARY KEY ((site), timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC);
```

**Sample Query:**
*   Route Tracking: `SELECT * FROM gps_tracking_by_vehicle WHERE site = 'fleet-vehicle-22' AND timestamp >= '2026-09-05T08:00:00Z' AND timestamp <= '2026-09-05T17:00:00Z';`

### Table 3: `alerts_by_site`
Answers **Query 4** (Site Maintenance & Alerts). Grouped by both site and status.

```cql
CREATE TABLE alerts_by_site (
    site text,
    status text,
    timestamp timestamp,
    device_id text,
    device_type text,
    battery_percent int,
    readings text,
    PRIMARY KEY ((site, status), timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC);
```

**Sample Query:**
*   Site Faults: `SELECT * FROM alerts_by_site WHERE site = 'warehouse-3' AND status = 'SENSOR_FAULT';`

### Table 4: `industrial_events_by_site`
Answers **Query 5** (Industrial Auditing).

```cql
CREATE TABLE industrial_events_by_site (
    site text,
    device_type text,
    timestamp timestamp,
    device_id text,
    readings text,
    PRIMARY KEY ((site, device_type), timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC);
```

**Sample Query:**
*   Industrial Audit: `SELECT * FROM industrial_events_by_site WHERE site = 'factory-floor-1' AND device_type = 'industrial_multi' AND timestamp >= '2026-09-06T16:00:00Z';`
