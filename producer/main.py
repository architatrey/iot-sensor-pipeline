"""
main.py — Entry point for the IoT sensor producer.

Usage:
    python main.py [--config path/to/config.yaml]

The script:
  1. Loads config from config.yaml (or a path given via --config).
  2. Creates N virtual Device instances.
  3. Connects to Kafka (with retry/back-off).
  4. Spawns one thread per device, each emitting events at its own randomized
     interval (FR3), so devices don't all fire at the same time.
  5. Runs until Ctrl+C — then flushes and exits cleanly (FR11).
"""

import argparse
import logging
import random
import sys
import threading
import time
from pathlib import Path

import yaml

# Resolve imports whether run from project root or producer/
sys.path.insert(0, str(Path(__file__).parent))

from src.device import create_devices
from src.producer import KafkaProducerWrapper


# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)-8s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("iot-producer")


# ---------------------------------------------------------------------------
# Config loader
# ---------------------------------------------------------------------------

def load_config(config_path: str) -> dict:
    path = Path(config_path)
    if not path.exists():
        logger.error("Config file not found: %s", config_path)
        sys.exit(1)
    with path.open() as f:
        cfg = yaml.safe_load(f)
    logger.info("Loaded config from %s", config_path)
    return cfg


# ---------------------------------------------------------------------------
# Per-device worker
# ---------------------------------------------------------------------------

def device_worker(device, producer: KafkaProducerWrapper, interval_min: float, interval_max: float):
    """
    Continuously emit sensor events for a single device.

    Each iteration sleeps for a randomized interval to avoid synchronized
    bursts across all devices (FR3).
    """
    logger.info("Started worker for %r", device)
    while True:
        try:
            events = device.generate_event()
            for event in events:
                producer.publish(event)
        except Exception as exc:
            logger.error("Error in worker for %s: %s", device.device_id, exc)

        sleep_s = random.uniform(interval_min, interval_max)
        time.sleep(sleep_s)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="IoT Sensor Kafka Producer")
    parser.add_argument(
        "--config",
        default=str(Path(__file__).parent / "config.yaml"),
        help="Path to config.yaml (default: ./config.yaml)",
    )
    args = parser.parse_args()

    cfg = load_config(args.config)
    sim = cfg.get("simulation", {})

    # Create devices
    devices = create_devices(
        device_count=sim.get("device_count", 20),
        regions=sim.get("regions", ["us-east-1"]),
        device_types=sim.get("device_types", ["temperature_humidity"]),
        anomaly_rate=sim.get("anomaly_rate", 0.03),
        duplicate_rate=sim.get("duplicate_rate", 0.01),
    )
    logger.info("Initialized %d virtual devices.", len(devices))

    # Connect to Kafka
    kafka_producer = KafkaProducerWrapper(cfg)
    kafka_producer.connect()

    interval_min = float(sim.get("interval_seconds_min", 1))
    interval_max = float(sim.get("interval_seconds_max", 5))

    # Launch one daemon thread per device (daemon=True so they die with main)
    threads = []
    for device in devices:
        t = threading.Thread(
            target=device_worker,
            args=(device, kafka_producer, interval_min, interval_max),
            name=f"worker-{device.device_id}",
            daemon=True,
        )
        t.start()
        threads.append(t)

    logger.info(
        "All %d device workers running. Publishing to topic '%s'. Press Ctrl+C to stop.",
        len(threads),
        cfg.get("kafka", {}).get("topic", "sensor-readings"),
    )

    # Keep main thread alive — shutdown is handled by KafkaProducerWrapper's signal handler
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass  # signal handler in KafkaProducerWrapper takes over


if __name__ == "__main__":
    main()
