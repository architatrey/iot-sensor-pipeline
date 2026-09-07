-- 1. Create the Keyspace (Database)
CREATE KEYSPACE IF NOT EXISTS iot_pipeline 
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

-- Switch to the new keyspace
USE iot_pipeline;

-- 2. Create the Tables (from our Query-First Design)

-- Table 1: Current State & Historical Trends
CREATE TABLE IF NOT EXISTS sensor_readings_by_device (
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

-- Table 2: Asset Tracking (GPS)
CREATE TABLE IF NOT EXISTS gps_tracking_by_vehicle (
    site text,
    timestamp timestamp,
    device_id text,
    latitude double,
    longitude double,
    speed_kmh double,
    heading_degrees int,
    PRIMARY KEY ((site), timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC);

-- Table 3: Site Maintenance & Alerts
CREATE TABLE IF NOT EXISTS alerts_by_site (
    site text,
    status text,
    timestamp timestamp,
    device_id text,
    device_type text,
    battery_percent int,
    readings text,
    PRIMARY KEY ((site, status), timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC);

-- Table 4: Industrial Auditing
CREATE TABLE IF NOT EXISTS industrial_events_by_site (
    site text,
    device_type text,
    timestamp timestamp,
    device_id text,
    readings text,
    PRIMARY KEY ((site, device_type), timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC);

-- Table 5: Windowed Aggregations (Average Temperature)
CREATE TABLE IF NOT EXISTS average_temperature_by_site (
    site text,
    window_end timestamp,
    avg_temp_celsius double,
    events_processed int,
    PRIMARY KEY ((site), window_end)
) WITH CLUSTERING ORDER BY (window_end DESC);
