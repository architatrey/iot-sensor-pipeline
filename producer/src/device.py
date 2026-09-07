"""
device.py — Simulates IoT sensor devices.

Each Device instance owns its identity (device_id, type, region, site) and
is responsible for generating readings according to its device_type, injecting
anomalies, and occasionally emitting duplicate/out-of-order events.
"""

import random
import uuid
from datetime import datetime, timezone


# ---------------------------------------------------------------------------
# Sensor reading generators
# ---------------------------------------------------------------------------

def _generate_temp_humidity_readings(anomalous):
    if anomalous:
        return {
            "temperature_celsius": round(random.uniform(150.0, 200.0), 1),
            "humidity_percent": round(random.uniform(-20.0, -1.0), 1),
        }
    return {
        "temperature_celsius": round(random.uniform(15.0, 40.0), 1),
        "humidity_percent": round(random.uniform(20.0, 95.0), 1),
    }


def _generate_gps_readings(anomalous):
    if anomalous:
        return {
            "latitude": round(random.uniform(-200.0, -100.0), 4),
            "longitude": round(random.uniform(200.0, 400.0), 4),
            "speed_kmh": round(random.uniform(500.0, 999.9), 1),
            "heading_degrees": random.randint(0, 359),
        }
    return {
        "latitude": round(random.uniform(-90.0, 90.0), 4),
        "longitude": round(random.uniform(-180.0, 180.0), 4),
        "speed_kmh": round(random.uniform(0.0, 120.0), 1),
        "heading_degrees": random.randint(0, 359),
    }


def _generate_industrial_multi_readings(anomalous):
    if anomalous:
        return {
            "temperature_celsius": round(random.uniform(200.0, 500.0), 1),
            "vibration_hz": round(random.uniform(100.0, 999.0), 1),
            "pressure_kpa": round(random.uniform(300.0, 999.9), 1),
        }
    return {
        "temperature_celsius": round(random.uniform(50.0, 120.0), 1),
        "vibration_hz": round(random.uniform(1.0, 30.0), 1),
        "pressure_kpa": round(random.uniform(95.0, 110.0), 1),
    }


_READING_GENERATORS = {
    "temperature_humidity": _generate_temp_humidity_readings,
    "gps": _generate_gps_readings,
    "industrial_multi": _generate_industrial_multi_readings,
}

# Sites per device_type
_SITES = {
    "temperature_humidity": ["warehouse-1", "warehouse-2", "warehouse-3", "server-room-A"],
    "gps": ["fleet-vehicle-01", "fleet-vehicle-22", "fleet-vehicle-99", "delivery-truck-07"],
    "industrial_multi": ["factory-floor-1", "factory-floor-2", "assembly-line-3", "cooling-unit-5"],
}


# ---------------------------------------------------------------------------
# Device class
# ---------------------------------------------------------------------------

class Device:
    """Represents a single virtual IoT sensor device."""

    SCHEMA_VERSION = 1

    def __init__(self, device_id, device_type, region, anomaly_rate=0.03, duplicate_rate=0.01):
        self.device_id = device_id
        self.device_type = device_type
        self.region = region
        self.site = random.choice(_SITES.get(device_type, ["unknown-site"]))
        self.anomaly_rate = anomaly_rate
        self.duplicate_rate = duplicate_rate
        self._is_mains_powered = (device_type == "industrial_multi")
        self._last_event = None

    def generate_event(self):
        """
        Generate one (or occasionally two) events for this device.

        Returns a list so duplicates can be emitted naturally as a second
        element — the caller simply publishes every item in the list.
        """
        event = self._build_event()
        self._last_event = event

        events = [event]

        # Inject duplicate ~duplicate_rate % of the time (FR8)
        if self._last_event and random.random() < self.duplicate_rate:
            events.append(self._last_event.copy())

        return events

    def _build_event(self):
        anomalous = random.random() < self.anomaly_rate
        readings = _READING_GENERATORS[self.device_type](anomalous)

        status = "OK"
        if anomalous:
            status = "SENSOR_FAULT"
        elif not self._is_mains_powered:
            battery = self._battery_percent()
            if battery < 10:
                status = "LOW_BATTERY"

        event = {
            "schema_version": self.SCHEMA_VERSION,
            "event_id": str(uuid.uuid4()),
            "device_id": self.device_id,
            "device_type": self.device_type,
            "region": self.region,
            "site": self.site,
            "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
            "readings": readings,
            "status": status,
        }

        if self._is_mains_powered:
            event["battery_percent"] = None
            event["power_source"] = "mains"
        else:
            event["battery_percent"] = self._battery_percent()

        return event

    def _battery_percent(self):
        """Simulate a slowly draining battery."""
        return random.randint(5, 100)

    def __repr__(self):
        return f"Device(id={self.device_id!r}, type={self.device_type!r}, region={self.region!r})"


# ---------------------------------------------------------------------------
# Factory helper
# ---------------------------------------------------------------------------

def create_devices(device_count, regions, device_types, anomaly_rate, duplicate_rate):
    """
    Instantiate `device_count` virtual devices, round-robining over device_types
    and randomly picking a region for each.
    """
    devices = []
    for i in range(device_count):
        dtype = device_types[i % len(device_types)]
        region = random.choice(regions)
        abbrev = dtype[:4]
        device_id = f"sensor-{abbrev}-{i + 1:03d}"
        devices.append(
            Device(
                device_id=device_id,
                device_type=dtype,
                region=region,
                anomaly_rate=anomaly_rate,
                duplicate_rate=duplicate_rate,
            )
        )
    return devices
