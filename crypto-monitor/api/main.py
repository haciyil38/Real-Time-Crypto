import asyncio
import json
import logging
import os
import threading
from datetime import datetime, timezone
from typing import Optional

from dotenv import load_dotenv
from fastapi import FastAPI, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient

from routes.alerts import get_alerts
from routes.stats import get_stats
from routes.trades import get_trades

load_dotenv()

MONGO_URI = os.environ["MONGO_URI"]
MONGO_DB = os.getenv("MONGO_DB", "crypto_monitor")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)-8s [service=api] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger(__name__)

app = FastAPI(title="Crypto Monitor API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

_mongo_client: AsyncIOMotorClient = None
_db = None


_trade_queue: asyncio.Queue = None
_ingest_rate: float = 0.0


@app.on_event("startup")
async def _startup():
    global _mongo_client, _db, _trade_queue
    _mongo_client = AsyncIOMotorClient(MONGO_URI)
    _db = _mongo_client[MONGO_DB]
    asyncio.create_task(_broadcaster())

    # Start Kafka consumer in a daemon thread; bridge to async via Queue
    _trade_queue = asyncio.Queue(maxsize=2000)
    loop = asyncio.get_running_loop()
    t = threading.Thread(
        target=_trade_consumer_thread, args=(loop, _trade_queue), daemon=True, name="trade-consumer"
    )
    t.start()
    asyncio.create_task(_trade_queue_drainer())

    log.info("API started")


@app.on_event("shutdown")
async def _shutdown():
    if _mongo_client:
        _mongo_client.close()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ── REST endpoints ────────────────────────────────────────────────────────────

@app.get("/api/trades")
async def api_trades(
    pair: Optional[str] = Query(None),
    exchange: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=500),
):
    return await get_trades(_db, pair=pair, exchange=exchange, limit=limit)


@app.get("/api/stats")
async def api_stats(
    pair: Optional[str] = Query(None),
    window: str = Query("5m"),
):
    return await get_stats(_db, pair=pair, window=window)


@app.get("/api/alerts")
async def api_alerts(
    limit: int = Query(20, ge=1, le=200),
    severity: Optional[str] = Query(None),
):
    return await get_alerts(_db, limit=limit, severity=severity)


@app.get("/api/alerts/count")
async def api_alerts_count(minutes: int = Query(10, ge=1, le=1440)):
    since = datetime.now(timezone.utc).replace(microsecond=0)
    from datetime import timedelta
    since = since - timedelta(minutes=minutes)
    count = await _db["alerts"].count_documents({"triggered_at": {"$gte": since.isoformat()}})
    return {"count": count, "minutes": minutes}


@app.get("/api/alerts/since")
async def api_alerts_since(
    since: str = Query(""),
    limit: int = Query(20),
):
    query: dict = {}
    if since:
        query["triggered_at"] = {"$gt": since}
    cursor = _db["alerts"].find(query, {"_id": 0}).sort("triggered_at", 1).limit(limit)
    return await cursor.to_list(length=limit)


@app.get("/api/snapshot")
async def api_snapshot():
    return await _build_snapshot()


@app.get("/api/health")
async def api_health():
    mongo_ok = False
    try:
        await _mongo_client.admin.command("ping")
        mongo_ok = True
    except Exception:
        pass

    kafka_ok = False
    consumer_count = 0
    try:
        from confluent_kafka.admin import AdminClient
        admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
        result = admin.list_consumer_groups()
        groups = result.result(timeout=3)
        known = {"group-storage", "group-aggregator", "group-anomaly"}
        consumer_count = len({g.group_id for g in groups.valid if g.group_id in known})
        kafka_ok = True
    except Exception:
        pass

    return {
        "status": "ok",
        "kafka": kafka_ok,
        "mongodb": mongo_ok,
        "consumers_active": consumer_count,
        "ingest_rate": _ingest_rate,
    }


# ── Snapshot builder ──────────────────────────────────────────────────────────

async def _build_snapshot() -> dict:
    pairs = [
        ("BTC/USDT", "binance"),
        ("ETH/USDT", "binance"),
        ("BTC/USD",  "coinbase"),
    ]
    prices: dict = {}
    for pair, exchange in pairs:
        doc = await _db["trades_raw"].find_one(
            {"pair": pair, "exchange": exchange},
            {"price": 1, "_id": 0},
            sort=[("timestamp", -1)],
        )
        agg = await _db["aggregates"].find_one(
            {"pair": pair, "exchange": exchange, "window": "5m"},
            {"price_change_pct": 1, "_id": 0},
        )
        prices[pair] = {
            exchange: doc["price"] if doc else None,
            "last_change_pct": agg["price_change_pct"] if agg else 0.0,
        }

    stats_5m: dict = {}
    stats_all: dict = {}
    async for doc in _db["aggregates"].find({}, {"_id": 0}):
        key = f"{doc['pair']}:{doc['exchange']}"
        if doc["window"] == "5m":
            stats_5m[key] = doc
        stats_all.setdefault(key, {})[doc["window"]] = doc

    recent_trades = await get_trades(_db, pair=None, exchange=None, limit=5)
    recent_alerts = await get_alerts(_db, limit=3, severity=None)

    return {
        "type": "snapshot",
        "timestamp": _now_iso(),
        "prices": prices,
        "stats_5m": stats_5m,
        "stats_all": stats_all,
        "recent_trades": recent_trades,
        "recent_alerts": recent_alerts,
    }


# ── Real-time trade streamer (Kafka → WebSocket) ─────────────────────────────

_trade_queue: asyncio.Queue = None


def _queue_put_or_drop(queue: asyncio.Queue, item: dict):
    """Called from the event loop via call_soon_threadsafe; drops silently if full."""
    if not queue.full():
        queue.put_nowait(item)


def _trade_consumer_thread(loop: asyncio.AbstractEventLoop, queue: asyncio.Queue):
    """Blocking Kafka consumer in a thread; pushes trades into an asyncio Queue."""
    log.info("Trade consumer thread started")
    try:
        from confluent_kafka import Consumer
        consumer = Consumer({
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": "group-api-ws",
            "auto.offset.reset": "latest",
            "enable.auto.commit": "true",
        })
        consumer.subscribe(["crypto.trades.raw"])
        log.info("Trade streamer subscribed to crypto.trades.raw")
        try:
            while True:
                msg = consumer.poll(0.05)
                if msg is None or msg.error():
                    continue
                try:
                    trade = json.loads(msg.value())
                    payload = {"type": "trade", **trade}
                    loop.call_soon_threadsafe(_queue_put_or_drop, queue, payload)
                except Exception as exc:
                    log.warning("Trade streamer parse error: %s", exc)
        finally:
            consumer.close()
    except Exception as exc:
        log.error("Trade consumer thread FATAL: %s", exc, exc_info=True)


async def _trade_queue_drainer():
    """Drain trades from the thread-safe queue and broadcast to WS clients."""
    global _ingest_rate
    log.info("Trade queue drainer started")
    count = 0
    window_start = asyncio.get_event_loop().time()
    while True:
        try:
            trade = await _trade_queue.get()
            count += 1
            now = asyncio.get_event_loop().time()
            if now - window_start >= 5.0:
                _ingest_rate = round(count / (now - window_start), 1)
                count = 0
                window_start = now
            if _manager._clients:
                await _manager.broadcast(trade)
        except Exception as exc:
            log.warning("Trade broadcast error: %s", exc)


# ── WebSocket broadcast (kept for direct-network clients) ────────────────────

class _Manager:
    def __init__(self):
        self._clients: list[WebSocket] = []

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self._clients.append(ws)
        log.info("WS client connected — total: %d", len(self._clients))

    def disconnect(self, ws: WebSocket):
        self._clients = [c for c in self._clients if c is not ws]
        log.info("WS client disconnected — total: %d", len(self._clients))

    async def broadcast(self, payload: dict):
        text = json.dumps(payload)
        dead: list[WebSocket] = []
        for ws in list(self._clients):
            try:
                await ws.send_text(text)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.disconnect(ws)


_manager = _Manager()
_last_alert_ts: str = ""
# Dedup: track last broadcast time per (alert_type, pair) for noisy alert types
_alert_dedup: dict = {}
_DEDUP_SECONDS = 30  # HIGH_FREQUENCY: at most one push per pair every 30s


def _should_broadcast_alert(doc: dict) -> bool:
    """Return True if this alert should be pushed to clients."""
    if doc.get("alert_type") in ("LARGE_TRADE", "PRICE_SPIKE", "SPREAD_BINANCE_COINBASE"):
        return True
    key = f"{doc.get('alert_type')}:{doc.get('pair')}"
    import time
    now = time.monotonic()
    if now - _alert_dedup.get(key, 0) >= _DEDUP_SECONDS:
        _alert_dedup[key] = now
        return True
    return False


async def _broadcaster():
    global _last_alert_ts
    while True:
        await asyncio.sleep(1)
        if not _manager._clients:
            continue
        try:
            snapshot = await _build_snapshot()
            await _manager.broadcast(snapshot)
        except Exception as exc:
            log.error("Snapshot error: %s", exc)
        try:
            query: dict = {}
            if _last_alert_ts:
                query["triggered_at"] = {"$gt": _last_alert_ts}
            async for doc in _db["alerts"].find(query, {"_id": 0}).sort("triggered_at", 1):
                _last_alert_ts = doc["triggered_at"]
                if _should_broadcast_alert(doc):
                    await _manager.broadcast({"type": "alert", **doc})
        except Exception as exc:
            log.error("Alert poll error: %s", exc)


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await _manager.connect(ws)
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        _manager.disconnect(ws)
