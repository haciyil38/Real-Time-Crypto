# Crypto Market Monitor — Real-Time Pipeline

Pipeline de streaming temps réel qui ingère les trades live depuis Binance et Coinbase via WebSocket, les traite via Apache Kafka, les stocke dans MongoDB et les expose via FastAPI avec un dashboard HTML/JS temps réel.

Projet EFREI — Real-Time Data. Soutenance le 30 juin.

## Architecture

```
Binance WS (BTC/USDT, ETH/USDT) ─┐
                                   ├─► Producer ──► Kafka (crypto.trades.raw)
Coinbase WS (BTC-USD) ────────────┘                        │
                                          ┌────────────────┼────────────────┐
                                          ▼                ▼                ▼
                                     Consumer 1       Consumer 2       Consumer 3
                                     (stockage)      (agrégation)     (anomalies)
                                          │                │                │
                                          └────────────────┴────────────────┘
                                                           │
                                                       MongoDB
                                                           │
                                               FastAPI (REST + WebSocket)
                                                           │
                                                   Nginx → Dashboard
```

## Prérequis

- [Docker](https://docs.docker.com/get-docker/) 24+
- [Docker Compose](https://docs.docker.com/compose/) v2+

## Démarrage

```bash
cp .env.example .env
docker compose up -d --build
```

Dashboard : **http://localhost**

## Services

| Service | Rôle | Port |
|---------|------|------|
| `kafka` | Message broker (KRaft, sans Zookeeper) | 9092 |
| `mongodb` | Stockage (trades, agrégats, alertes) | 27017 |
| `producer` | WebSocket Binance + Coinbase → Kafka | — |
| `consumer1` | Kafka → MongoDB `trades_raw` | — |
| `consumer2` | Kafka → VWAP/OHLCV fenêtres 1m/5m/15m/1h → MongoDB `aggregates` | — |
| `consumer3` | Kafka → détection anomalies → MongoDB `alerts` | — |
| `api` | FastAPI REST + WebSocket push | 8000 |
| `dashboard` | Nginx static files + reverse proxy | 80 |

## MongoDB — collections

| Collection | Contenu |
|------------|---------|
| `trades_raw` | Tous les trades bruts (exchange, pair, price, quantity, trade_id, timestamp) |
| `aggregates` | VWAP + volume par fenêtre (1m/5m/15m/1h) par pair/exchange |
| `alerts` | Anomalies détectées en temps réel |

## API — endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /api/trades` | Derniers trades (params : pair, exchange, limit) |
| `GET /api/stats` | Agrégats VWAP/volume (params : pair, window) |
| `GET /api/alerts` | Dernières alertes (params : limit, severity) |
| `GET /api/alerts/count?minutes=10` | Nombre d'alertes sur N minutes |
| `GET /api/health` | État Kafka, MongoDB, consumers, débit ingestion |
| `GET /api/snapshot` | Snapshot complet : prix + stats + alertes |
| `WS /ws` | Push temps réel : snapshots (1/s) + trades Kafka + alertes |

## Transport temps réel

**WebSocket uniquement** — pas de fallback HTTP polling.

Le dashboard reçoit 3 types de messages via WebSocket :
- `{ type: "snapshot" }` — prix, KPIs, stats toutes les secondes
- `{ type: "trade" }` — chaque trade individuel depuis Kafka (~50/s)
- `{ type: "alert" }` — anomalies détectées en temps réel

En cas de déconnexion, le dashboard reconnecte automatiquement toutes les 3 secondes.

## Détection d'anomalies

| Type | Condition | Sévérité |
|------|-----------|----------|
| `LARGE_TRADE` | Quantité > moyenne + 3×écart-type (fenêtre 500 trades) | HIGH / MEDIUM |
| `PRICE_SPIKE` | Variation prix > 0.5% entre deux trades consécutifs | HIGH / MEDIUM |
| `HIGH_FREQUENCY` | > 50 trades en 10 secondes sur un même pair | LOW |
| `SPREAD_BINANCE_COINBASE` | Écart prix BTC Binance/Coinbase > 0.05% | HIGH / MEDIUM |

## Accès

| Interface | URL |
|-----------|-----|
| Dashboard | http://localhost |
| API docs (Swagger) | http://localhost:8000/docs |
| API health | http://localhost:8000/api/health |

## Commandes utiles

```bash
# Voir les logs d'un service
docker compose logs -f producer
docker compose logs -f api

# Vérifier que tout tourne
docker compose ps

# Arrêter sans perdre les données MongoDB
docker compose down

# Arrêter et supprimer les volumes
docker compose down -v
```
