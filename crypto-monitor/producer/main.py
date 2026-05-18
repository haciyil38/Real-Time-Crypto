import asyncio
import json
import logging
import os
import signal
from datetime import datetime, timezone
from typing import TypedDict

import websockets
from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP = os.environ["KAFKA_BOOTSTRAP"]
KAFKA_TOPIC = os.environ["KAFKA_TOPIC"]
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)-8s [service=producer] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger(__name__)

BINANCE_WS_URL = (
    "wss://stream.binance.com:9443/stream"
    "?streams=btcusdt@trade/ethusdt@trade"
)
COINBASE_WS_URL = "wss://advanced-trade-ws.coinbase.com"
COINBASE_SUBSCRIBE = json.dumps({
    "type": "subscribe",
    "product_ids": ["BTC-USD"],
    "channel": "market_trades",
})

BINANCE_PAIR_MAP = {
    "BTCUSDT": "BTC/USDT",
    "ETHUSDT": "ETH/USDT",
}

_shutdown = asyncio.Event()
_trade_counts: dict[str, int] = {"binance": 0, "coinbase": 0}


class Trade(TypedDict):
    exchange: str
    pair: str
    price: float
    quantity: float
    trade_id: str
    timestamp: str
    received_at: str


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _delivery_cb(err, _msg):
    if err:
        log.error("Kafka delivery error: %s", err)


def _build_producer() -> Producer:
    return Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "queue.buffering.max.messages": 100_000,
        "queue.buffering.max.ms": 50,
    })


_producer = _build_producer()


def _publish(trade: Trade) -> None:
    payload = json.dumps(trade).encode()
    _producer.produce(
        KAFKA_TOPIC,
        key=trade["pair"].encode(),
        value=payload,
        callback=_delivery_cb,
    )
    _producer.poll(0)


def _parse_binance(raw: dict) -> Trade | None:
    data = raw.get("data", {})
    if data.get("e") != "trade":
        return None
    symbol = data.get("s", "")
    pair = BINANCE_PAIR_MAP.get(symbol)
    if not pair:
        return None
    ts = datetime.fromtimestamp(data["T"] / 1000.0, tz=timezone.utc).isoformat()
    return Trade(
        exchange="binance",
        pair=pair,
        price=float(data["p"]),
        quantity=float(data["q"]),
        trade_id=str(data["t"]),
        timestamp=ts,
        received_at=_now_iso(),
    )


def _parse_coinbase(raw: dict) -> list[Trade]:
    if raw.get("channel") != "market_trades":
        return []
    events = raw.get("events", [])
    if not events:
        return []
    trades = []
    for t in events[0].get("trades", []):
        trades.append(Trade(
            exchange="coinbase",
            pair="BTC/USD",
            price=float(t["price"]),
            quantity=float(t["size"]),
            trade_id=str(t["trade_id"]),
            timestamp=t["time"],
            received_at=_now_iso(),
        ))
    return trades


async def _binance_handler() -> None:
    attempt = 0
    while not _shutdown.is_set():
        try:
            async with websockets.connect(BINANCE_WS_URL, ping_interval=20) as ws:
                log.info("Connected to binance")
                attempt = 0
                async for raw_msg in ws:
                    if _shutdown.is_set():
                        return
                    try:
                        trade = _parse_binance(json.loads(raw_msg))
                        if trade:
                            _publish(trade)
                            _trade_counts["binance"] += 1
                    except Exception as exc:
                        log.error("Binance parse error: %s", exc)
        except Exception as exc:
            if _shutdown.is_set():
                return
            delay = min(2 ** attempt, 30)
            log.warning(
                "Binance disconnected (attempt %d): %s — retrying in %ds",
                attempt + 1, exc, delay,
            )
            attempt += 1
            await asyncio.sleep(delay)


async def _coinbase_handler() -> None:
    attempt = 0
    while not _shutdown.is_set():
        try:
            async with websockets.connect(COINBASE_WS_URL, ping_interval=20) as ws:
                await ws.send(COINBASE_SUBSCRIBE)
                log.info("Connected to coinbase")
                attempt = 0
                async for raw_msg in ws:
                    if _shutdown.is_set():
                        return
                    try:
                        for trade in _parse_coinbase(json.loads(raw_msg)):
                            _publish(trade)
                            _trade_counts["coinbase"] += 1
                    except Exception as exc:
                        log.error("Coinbase parse error: %s", exc)
        except Exception as exc:
            if _shutdown.is_set():
                return
            delay = min(2 ** attempt, 30)
            log.warning(
                "Coinbase disconnected (attempt %d): %s — retrying in %ds",
                attempt + 1, exc, delay,
            )
            attempt += 1
            await asyncio.sleep(delay)


async def _stats_reporter() -> None:
    while not _shutdown.is_set():
        await asyncio.sleep(30)
        b = _trade_counts["binance"]
        c = _trade_counts["coinbase"]
        _trade_counts["binance"] = 0
        _trade_counts["coinbase"] = 0
        log.info("Stats — binance: %d trades/30s | coinbase: %d trades/30s", b, c)


async def _main() -> None:
    loop = asyncio.get_running_loop()

    def _handle_signal():
        log.info("Shutdown signal received")
        _shutdown.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _handle_signal)

    log.info("Producer starting — broker=%s topic=%s", KAFKA_BOOTSTRAP, KAFKA_TOPIC)

    tasks = await asyncio.gather(
        _binance_handler(),
        _coinbase_handler(),
        _stats_reporter(),
        return_exceptions=True,
    )

    for result in tasks:
        if isinstance(result, Exception):
            log.error("Task exited with error: %s", result)

    _producer.flush(timeout=10)
    log.info("Producer shutdown complete")


if __name__ == "__main__":
    asyncio.run(_main())
