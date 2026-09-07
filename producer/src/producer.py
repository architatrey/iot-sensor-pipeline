"""
producer.py — Kafka producer wrapper for the IoT sensor pipeline.

Responsibilities:
  - Connect to Kafka using config from config.yaml (NFR1)
  - Publish events with device_id as the message key (FR6)
  - Log partition/offset on delivery confirmation (FR9)
  - Handle broker unavailability with retries + exponential back-off (FR10)
  - Support graceful shutdown: flush buffered messages on SIGINT (FR11)
  - acks=all by default for durability (NFR3)
"""

import json
import logging
import signal
import sys
import time
import random
from typing import Optional

import yaml
from confluent_kafka import Producer, KafkaException

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Delivery callback
# ---------------------------------------------------------------------------

def _delivery_callback(err, msg):
    """Called once per message by librdkafka after delivery confirmation."""
    if err:
        logger.error(
            "Delivery FAILED | device_id=%s | error=%s",
            msg.key().decode() if msg.key() else "?",
            err,
        )
    else:
        logger.info(
            "Delivered | device_id=%s | topic=%s | partition=%d | offset=%d | timestamp=%s",
            msg.key().decode() if msg.key() else "?",
            msg.topic(),
            msg.partition(),
            msg.offset(),
            msg.timestamp()[1],  # (type, unix_ms)
        )


# ---------------------------------------------------------------------------
# KafkaProducerWrapper
# ---------------------------------------------------------------------------

class KafkaProducerWrapper:
    """
    Thin wrapper around confluent-kafka Producer.

    Features:
    - Connection retry with exponential back-off
    - Graceful SIGINT/SIGTERM shutdown (flush before exit)
    - Structured delivery logging
    """

    _INITIAL_RETRY_DELAY = 2      # seconds
    _MAX_RETRY_DELAY = 60         # seconds
    _MAX_CONNECT_ATTEMPTS = 10

    def __init__(self, config: dict):
        kafka_cfg = config.get("kafka", {})

        self._topic: str = kafka_cfg.get("topic", "sensor-readings")
        self._producer: Optional[Producer] = None
        self._shutting_down = False
        self._messages_flushed = 0

        # Build confluent-kafka producer config
        self._producer_conf = {
            "bootstrap.servers": kafka_cfg.get("bootstrap_servers", "localhost:9092"),
            "acks": str(kafka_cfg.get("acks", "all")),
            # Improve throughput / batching (NFR2)
            "linger.ms": 5,
            "batch.num.messages": 1000,
            "compression.type": "lz4",
            # Retries at the client level
            "retries": 5,
            "retry.backoff.ms": 300,
        }

        # Register shutdown handlers
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)

    # ------------------------------------------------------------------
    # Connection
    # ------------------------------------------------------------------

    def connect(self) -> None:
        """
        Attempt to create the Kafka Producer, retrying with exponential
        back-off if the broker is unavailable (FR10).
        """
        delay = self._INITIAL_RETRY_DELAY
        for attempt in range(1, self._MAX_CONNECT_ATTEMPTS + 1):
            try:
                logger.info(
                    "Connecting to Kafka (attempt %d/%d) — bootstrap=%s topic=%s",
                    attempt,
                    self._MAX_CONNECT_ATTEMPTS,
                    self._producer_conf["bootstrap.servers"],
                    self._topic,
                )
                self._producer = Producer(self._producer_conf)
                # Trigger a metadata fetch to validate reachability
                self._producer.list_topics(timeout=10)
                logger.info("Connected to Kafka successfully.")
                return
            except KafkaException as exc:
                logger.warning("Kafka connection attempt %d failed: %s", attempt, exc)
                if attempt == self._MAX_CONNECT_ATTEMPTS:
                    logger.error("Exhausted all Kafka connection attempts. Exiting.")
                    sys.exit(1)
                jitter = random.uniform(0, delay * 0.3)
                sleep_time = min(delay + jitter, self._MAX_RETRY_DELAY)
                logger.info("Retrying in %.1f seconds …", sleep_time)
                time.sleep(sleep_time)
                delay = min(delay * 2, self._MAX_RETRY_DELAY)

    # ------------------------------------------------------------------
    # Publishing
    # ------------------------------------------------------------------

    def publish(self, event: dict) -> None:
        """
        Serialize `event` to JSON and produce it to Kafka.

        The message key is `device_id` (bytes), ensuring all events from a
        single device land on the same partition (FR6).

        Also calls poll() to serve delivery callbacks without blocking (NFR2).
        """
        if self._shutting_down:
            return
        if self._producer is None:
            raise RuntimeError("Producer not connected. Call connect() first.")

        key = event["device_id"].encode("utf-8")
        value = json.dumps(event, ensure_ascii=False).encode("utf-8")

        while True:
            try:
                self._producer.produce(
                    topic=self._topic,
                    key=key,
                    value=value,
                    on_delivery=_delivery_callback,
                )
                # Non-blocking poll to serve delivery callbacks
                self._producer.poll(0)
                break
            except BufferError:
                # Local queue is full — poll to drain it, then retry
                logger.debug("Producer queue full; draining …")
                self._producer.poll(1)

    # ------------------------------------------------------------------
    # Shutdown
    # ------------------------------------------------------------------

    def flush(self) -> None:
        """Flush all buffered messages and wait for delivery confirmations."""
        if self._producer is None:
            return
        logger.info("Flushing producer — waiting for outstanding messages …")
        remaining = self._producer.flush(timeout=30)
        if remaining > 0:
            logger.warning("%d message(s) could not be delivered before timeout.", remaining)
        else:
            logger.info("All messages flushed successfully.")

    def _handle_shutdown(self, signum, frame):
        """Signal handler: flush and exit cleanly (FR11)."""
        logger.info("Shutdown signal received (sig=%d). Flushing …", signum)
        self._shutting_down = True
        self.flush()
        logger.info("Producer shut down cleanly.")
        sys.exit(0)
