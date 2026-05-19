import json
import logging
import os
import signal
import time

from confluent_kafka import Consumer, KafkaException
from dotenv import load_dotenv
from pymongo import MongoClient, ASCENDING

load_dotenv()

KAFKA_BOOTSTRAP = os.environ["KAFKA_BOOTSTRAP"]
KAFKA_TOPIC = os.environ["KAFKA_TOPIC"]
MONGO_URI = os.environ["MONGO_URI"]
MONGO_DB = os.getenv("MONGO_DB", "crypto_monitor")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)-8s [service=consumer1-storage] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger(__name__)

_running = True


def _handle_signal(signum, frame):
    global _running
    log.info("Shutdown signal received")
    _running = False


signal.signal(signal.SIGTERM, _handle_signal)
signal.signal(signal.SIGINT, _handle_signal)


def _setup_collection(mongo_uri: str, db_name: str):
    client = MongoClient(mongo_uri)
    col = client[db_name]["trades_raw"]
    col.create_index([("pair", ASCENDING), ("exchange", ASCENDING), ("timestamp", ASCENDING)])
    log.info("MongoDB index ensured on trades_raw")
    return col


def main():
    col = _setup_collection(MONGO_URI, MONGO_DB)

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": "group-storage",
        "auto.offset.reset": "latest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([KAFKA_TOPIC])
    log.info("Subscribed to %s", KAFKA_TOPIC)

    msg_count = 0
    window_start = time.monotonic()

    try:
        while _running:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                pass
            elif msg.error():
                raise KafkaException(msg.error())
            else:
                try:
                    trade = json.loads(msg.value().decode())
                    col.insert_one(trade)
                    consumer.commit(message=msg)
                    msg_count += 1
                except Exception as exc:
                    log.error("Failed to store trade: %s", exc)

            elapsed = time.monotonic() - window_start
            if elapsed >= 10:
                log.info("Throughput: %.1f msg/s", msg_count / elapsed)
                msg_count = 0
                window_start = time.monotonic()
    finally:
        consumer.close()
        log.info("Consumer1 shutdown complete")


if __name__ == "__main__":
    main()
