import json
import logging
import math
import os
import signal
import time
from collections import deque
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaException
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()

KAFKA_BOOTSTRAP = os.environ["KAFKA_BOOTSTRAP"]
KAFKA_TOPIC = os.environ["KAFKA_TOPIC"]
MONGO_URI = os.environ["MONGO_URI"]
MONGO_DB = os.getenv("MONGO_DB", "crypto_monitor")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)-8s [service=consumer3-anomaly] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger(__name__)

QTY_BUFFER_SIZE = 500
FREQ_WINDOW_SECS = 10
FREQ_THRESHOLD = 50
SPREAD_THRESHOLD = 0.0005  # 0.05% spread between Binance BTC/USDT and Coinbase BTC/USD

_running = True
_qty_buffers: dict[str, deque] = {}
_last_prices: dict[str, float] = {}
_freq_buffers: dict[str, deque] = {}
_btc_prices: dict[str, float] = {}  # "binance" → last BTC price, "coinbase" → last BTC price


def _handle_signal(signum, frame):
    global _running
    log.info("Shutdown signal received")
    _running = False


signal.signal(signal.SIGTERM, _handle_signal)
signal.signal(signal.SIGINT, _handle_signal)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _mean_std(values: list[float]) -> tuple[float, float]:
    n = len(values)
    if n < 2:
        return (values[0] if values else 0.0, 0.0)
    mean = math.fsum(values) / n
    variance = math.fsum((v - mean) ** 2 for v in values) / (n - 1)
    return mean, math.sqrt(variance)


def _detect_and_insert(trade: dict, col) -> None:
    k = f"{trade['exchange']}:{trade['pair']}"
    price = trade["price"]
    qty = trade["quantity"]
    now = time.time()

    if k not in _qty_buffers:
        _qty_buffers[k] = deque(maxlen=QTY_BUFFER_SIZE)
    if k not in _freq_buffers:
        _freq_buffers[k] = deque()

    qty_buf = _qty_buffers[k]
    freq_buf = _freq_buffers[k]

    qty_buf.append(qty)

    freq_buf.append(now)
    cutoff = now - FREQ_WINDOW_SECS
    while freq_buf and freq_buf[0] < cutoff:
        freq_buf.popleft()

    alerts = []

    if len(qty_buf) >= 30:
        mean, std = _mean_std(list(qty_buf))
        if std > 0:
            sigma = (qty - mean) / std
            if sigma > 3:
                severity = "HIGH" if sigma > 5 else "MEDIUM"
                alerts.append({
                    "alert_type": "LARGE_TRADE",
                    "exchange": trade["exchange"],
                    "pair": trade["pair"],
                    "triggered_at": _now_iso(),
                    "trade_price": price,
                    "trade_quantity": qty,
                    "threshold_value": mean + 3 * std,
                    "actual_value": qty,
                    "severity": severity,
                })

    prev = _last_prices.get(k)
    if prev is not None and prev > 0:
        pct = abs(price - prev) / prev
        if pct > 0.005:
            severity = "HIGH" if pct > 0.01 else "MEDIUM"
            alerts.append({
                "alert_type": "PRICE_SPIKE",
                "exchange": trade["exchange"],
                "pair": trade["pair"],
                "triggered_at": _now_iso(),
                "trade_price": price,
                "trade_quantity": qty,
                "threshold_value": 0.005,
                "actual_value": pct,
                "severity": severity,
            })
    _last_prices[k] = price

    if len(freq_buf) > FREQ_THRESHOLD:
        alerts.append({
            "alert_type": "HIGH_FREQUENCY",
            "exchange": trade["exchange"],
            "pair": trade["pair"],
            "triggered_at": _now_iso(),
            "trade_price": price,
            "trade_quantity": qty,
            "threshold_value": float(FREQ_THRESHOLD),
            "actual_value": float(len(freq_buf)),
            "severity": "LOW",
        })

    # Spread Binance/Coinbase on BTC
    if trade["pair"] in ("BTC/USDT", "BTC/USD"):
        exchange_key = "binance" if trade["exchange"] == "binance" else "coinbase"
        _btc_prices[exchange_key] = price
        if "binance" in _btc_prices and "coinbase" in _btc_prices:
            p_b = _btc_prices["binance"]
            p_c = _btc_prices["coinbase"]
            spread_pct = abs(p_b - p_c) / ((p_b + p_c) / 2)
            if spread_pct > SPREAD_THRESHOLD:
                severity = "HIGH" if spread_pct > 0.005 else "MEDIUM"
                alerts.append({
                    "alert_type": "SPREAD_BINANCE_COINBASE",
                    "exchange": "binance/coinbase",
                    "pair": "BTC",
                    "triggered_at": _now_iso(),
                    "trade_price": price,
                    "trade_quantity": qty,
                    "threshold_value": SPREAD_THRESHOLD,
                    "actual_value": round(spread_pct, 6),
                    "severity": severity,
                    "binance_price": p_b,
                    "coinbase_price": p_c,
                })

    for alert in alerts:
        try:
            col.insert_one(alert)
            log.info(
                "Alert %s | %s %s | severity=%s",
                alert["alert_type"], alert["exchange"], alert["pair"], alert["severity"],
            )
        except Exception as exc:
            log.error("Failed to insert alert: %s", exc)


def main():
    client = MongoClient(MONGO_URI)
    col = client[MONGO_DB]["alerts"]
    col.create_index([("triggered_at", -1)])
    col.create_index([("pair", 1), ("exchange", 1)])

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": "group-anomaly",
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
                _detect_and_insert(trade, col)
            except Exception as exc:
                log.error("Processing error: %s", exc)
    finally:
        consumer.close()
        log.info("Consumer3 shutdown complete")


if __name__ == "__main__":
    main()
