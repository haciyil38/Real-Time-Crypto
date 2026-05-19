import json
import logging
import math
import os
import signal
import threading
import time
from collections import deque
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaException
from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne

load_dotenv()

KAFKA_BOOTSTRAP = os.environ["KAFKA_BOOTSTRAP"]
KAFKA_TOPIC = os.environ["KAFKA_TOPIC"]
MONGO_URI = os.environ["MONGO_URI"]
MONGO_DB = os.getenv("MONGO_DB", "crypto_monitor")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)-8s [service=consumer2-aggregator] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger(__name__)

WINDOWS = {"1m": 60, "5m": 300, "15m": 900, "1h": 3600}
MAX_WINDOW_SECS = 3600
FLUSH_INTERVAL = 5

_running = True
_lock = threading.Lock()
# key: (exchange, pair) → deque of {"ts": float, "price": float, "qty": float}
_buffers: dict[tuple, deque] = {}


def _handle_signal(signum, frame):
    global _running
    log.info("Shutdown signal received")
    _running = False


signal.signal(signal.SIGTERM, _handle_signal)
signal.signal(signal.SIGINT, _handle_signal)


def _compute_window(trades: list[dict], cutoff: float) -> dict | None:
    window_trades = [t for t in trades if t["ts"] >= cutoff]
    if not window_trades:
        return None
    prices = [t["price"] for t in window_trades]
    qtys = [t["qty"] for t in window_trades]
    vol_base = math.fsum(qtys)
    vol_quote = math.fsum(p * q for p, q in zip(prices, qtys))
    vwap = vol_quote / vol_base if vol_base else 0.0
    price_open = prices[0]
    price_close = prices[-1]
    change_pct = ((price_close - price_open) / price_open * 100) if price_open else 0.0
    return {
        "vwap": vwap,
        "volume_base": vol_base,
        "volume_quote": vol_quote,
        "trade_count": len(window_trades),
        "price_open": price_open,
        "price_high": max(prices),
        "price_low": min(prices),
        "price_close": price_close,
        "price_change_pct": change_pct,
    }


def _flush(col):
    now = time.time()
    ops = []
    with _lock:
        for (exchange, pair), buf in _buffers.items():
            trades = list(buf)
            for label, secs in WINDOWS.items():
                result = _compute_window(trades, now - secs)
                if result is None:
                    continue
                doc = {
                    "pair": pair,
                    "exchange": exchange,
                    "window": label,
                    "computed_at": datetime.now(timezone.utc).isoformat(),
                    **result,
                }
                ops.append(UpdateOne(
                    {"pair": pair, "exchange": exchange, "window": label},
                    {"$set": doc},
                    upsert=True,
                ))
    if ops:
        try:
            col.bulk_write(ops)
            log.info("Flushed %d aggregate documents", len(ops))
        except Exception as exc:
            log.error("Flush error: %s", exc)


def _flush_loop(col):
    while _running:
        time.sleep(FLUSH_INTERVAL)
        _flush(col)


def main():
    client = MongoClient(MONGO_URI)
    col = client[MONGO_DB]["aggregates"]
    col.create_index([("pair", 1), ("exchange", 1), ("window", 1)], unique=True)

    flusher = threading.Thread(target=_flush_loop, args=(col,), daemon=True)
    flusher.start()

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": "group-aggregator",
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
    })
    consumer.subscribe([KAFKA_TOPIC])
    log.info("Subscribed to %s", KAFKA_TOPIC)

    try:
        while _running:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())
            try:
                trade = json.loads(msg.value().decode())
                k = (trade["exchange"], trade["pair"])
                entry = {"ts": time.time(), "price": trade["price"], "qty": trade["quantity"]}
                with _lock:
                    if k not in _buffers:
                        _buffers[k] = deque()
                    buf = _buffers[k]
                    buf.append(entry)
                    cutoff = time.time() - MAX_WINDOW_SECS
                    while buf and buf[0]["ts"] < cutoff:
                        buf.popleft()
            except Exception as exc:
                log.error("Processing error: %s", exc)
    finally:
        consumer.close()
        log.info("Consumer2 shutdown complete")


if __name__ == "__main__":
    main()
