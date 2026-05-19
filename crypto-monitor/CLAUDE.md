# Crypto Market Monitoring System — CLAUDE.md

Projet EFREI Real-Time Data. Soutenance le 30 juin (10-15 min, groupes de 4-5).
Sujet : pipeline de streaming temps réel Binance + Coinbase → Kafka → 3 consumers → MongoDB → FastAPI → Dashboard HTML/JS.

## Architecture

```
Binance WS (BTC/USDT, ETH/USDT)  ┐
                                   ├─► Producer ──► Kafka (crypto.trades.raw) ──► Consumer 1 ──► MongoDB (trades_raw)
Coinbase WS (BTC-USD)             ┘                                            ├─► Consumer 2 ──► MongoDB (aggregates)
                                                                                └─► Consumer 3 ──► MongoDB (alerts)
                                                                                         ↓
                                                                               FastAPI (REST + WebSocket)
                                                                                         ↓
                                                                               Nginx ──► Dashboard HTML/JS
```

**8 containers Docker** (docker-compose.yml) :
| Container | Rôle |
|-----------|------|
| `kafka` | Kafka KRaft (sans Zookeeper), topic `crypto.trades.raw` |
| `mongodb` | MongoDB 7, base `crypto_monitor` |
| `producer` | Se connecte aux WS Binance + Coinbase, publie dans Kafka |
| `consumer1` | Stockage brut → collection `trades_raw` |
| `consumer2` | Agrégation VWAP/OHLCV fenêtres 1m/5m/15m/1h → `aggregates` |
| `consumer3` | Détection anomalies (LARGE_TRADE, PRICE_SPIKE, HIGH_FREQUENCY) → `alerts` |
| `api` | FastAPI REST + WebSocket, streame les trades depuis Kafka via thread |
| `dashboard` | Nginx servant les fichiers statiques + reverse proxy vers l'API |

## Démarrer le projet

```bash
cd crypto-monitor
docker compose up -d --build   # premier démarrage
docker compose up -d           # démarrages suivants (si pas de modif code)
```

Dashboard accessible sur **http://localhost** (port 80).
API accessible sur **http://localhost:8000**.

Arrêter :
```bash
docker compose down            # arrête les containers (données MongoDB conservées)
docker compose down -v         # arrête + supprime les volumes (efface MongoDB)
```

## Modifier et redéployer

**Dashboard** (HTML/CSS/JS) : fichiers montés en volume → changements immédiats, juste Ctrl+Shift+R dans le navigateur. Bumper `?v=N` dans index.html pour vider le cache navigateur.

**API** (main.py, routes/) : PAS de volume mount → rebuild obligatoire :
```bash
docker compose build api && docker compose up -d api
```

**Consumers / Producer** : idem, rebuild nécessaire.

## Structure des fichiers

```
crypto-monitor/
├── docker-compose.yml
├── producer/
│   └── main.py              # WebSocket Binance + Coinbase → Kafka
├── consumer1_storage/
│   └── main.py              # Kafka → MongoDB trades_raw
├── consumer2_aggregator/
│   └── main.py              # Kafka → VWAP/OHLCV fenêtres glissantes → MongoDB aggregates
├── consumer3_anomaly/
│   └── main.py              # Kafka → détection anomalies → MongoDB alerts
├── api/
│   ├── main.py              # FastAPI : REST + WebSocket + trade streamer Kafka
│   └── routes/              # trades.py, stats.py, alerts.py
└── dashboard/
    ├── index.html
    ├── app.js               # Transport WS (préféré) avec fallback HTTP polling
    ├── style.css
    └── nginx.conf           # Proxy /api/ et /ws vers le container api
```

## MongoDB — collections

| Collection | Contenu |
|------------|---------|
| `trades_raw` | Chaque trade individuel (exchange, pair, price, quantity, trade_id, timestamp) |
| `aggregates` | VWAP/volume par fenêtre (1m/5m/15m/1h) par pair/exchange, mis à jour en continu |
| `alerts` | Anomalies détectées (LARGE_TRADE >3σ, PRICE_SPIKE >0.5%, HIGH_FREQUENCY >50 trades/10s) |

Pour voir les données dans MongoDB Compass : **stopper d'abord le MongoDB Homebrew local** qui prend le port 27017 avant Docker :
```bash
brew services stop mongodb-community
```
Puis connecter Compass sur `mongodb://localhost:27017`.

## API — endpoints REST

| Endpoint | Description |
|----------|-------------|
| `GET /api/trades` | Derniers trades (params: pair, exchange, limit) |
| `GET /api/stats` | Agrégats (params: pair, window) |
| `GET /api/alerts` | Dernières alertes (params: limit, severity) |
| `GET /api/alerts/count?minutes=10` | Nombre d'alertes sur N minutes (côté serveur) |
| `GET /api/alerts/since?since=<iso>` | Alertes après un timestamp (pour polling incrémental) |
| `GET /api/snapshot` | Snapshot complet : prix + stats + trades + alertes |
| `GET /api/health` | État Kafka + MongoDB + consumers actifs |
| `WS /ws` | WebSocket : pousse snapshots (1/s) + trades individuels (Kafka stream) + alertes |

## Transport Dashboard → API

L'API pousse les données de deux façons :
1. **Snapshot** (toutes les secondes) : prix, KPIs, stats, alertes récentes
2. **Trade stream** (temps réel) : chaque trade individuellement depuis Kafka via un thread dédié + asyncio.Queue → broadcast WS immédiat

Le dashboard essaie d'abord le WebSocket. Si bloqué (réseau école avec Cloudflare WARP), bascule automatiquement sur HTTP polling après 3 tentatives. Affiché dans Pipeline Health : "WebSocket ✓" ou "Polling (WS blocked)".

## Anomaly detection — logique

Dans `consumer3_anomaly/main.py` :
- **LARGE_TRADE** : quantité > moyenne + 3×écart-type sur fenêtre glissante 100 trades
- **PRICE_SPIKE** : variation de prix > 0.5% entre deux trades consécutifs
- **HIGH_FREQUENCY** : > 50 trades en 10 secondes sur un même pair/exchange

HIGH_FREQUENCY se déclenche très souvent (marché actif). Côté API, déduplication : une alerte HIGH_FREQUENCY max toutes les 30s par pair dans les pushes WebSocket. Le compteur KPI "Anomalies (10 min)" compte tout (requête MongoDB count directe).

## Conformité avec le cahier des charges

| Exigence PDF | Statut |
|--------------|--------|
| WebSocket ingestion Binance + Coinbase | ✅ |
| Kafka comme buffer/découplage | ✅ |
| 3 consumer groups distincts | ✅ group-storage, group-aggregator, group-anomaly |
| Fenêtres glissantes 1m/5m/15m/1h | ✅ consumer2 |
| Détection anomalies temps réel | ✅ consumer3 |
| API REST | ✅ FastAPI |
| API WebSocket push | ✅ /ws endpoint |
| Dashboard HTML/CSS/JS | ✅ vanilla, pas de framework |
| Pas de consumer Kafka direct dans le dashboard | ✅ dashboard → API → Kafka (API est l'intermédiaire) |
| Pas de batch processing | ✅ tout en streaming |
| Affichage live obligatoire | ✅ trades en temps réel depuis Kafka stream |

## Ce qui manque vs le dashboard exemple du PDF

- Marqueurs d'anomalies sur le graphique de prix (points sur la courbe)
- Spread Binance/Coinbase comme type d'alerte dédié
- Ingestion rate en msg/s dans le health panel (affiché "WS push" à la place)

## Points à expliquer en soutenance

1. **Pourquoi Kafka** : découplage ingestion/traitement, buffer si consumer lent, fanout vers plusieurs consumers sans impacter le producer
2. **Pourquoi 3 consumer groups** : chaque groupe reçoit tous les messages indépendamment (fanout), pas de partage de charge entre eux
3. **WebSocket vs polling** : le serveur pousse, le client ne tire pas → latence quasi-nulle, moins de charge réseau
4. **Pourquoi pas consumer Kafka direct dans le dashboard** : le navigateur ne peut pas parler à Kafka, et ça créerait un couplage fort — l'API est la couche d'abstraction
5. **Fenêtres glissantes** : consumer2 maintient un état en mémoire (deque par pair), recalcule à chaque trade sans relire toute la base

## Commandes utiles

```bash
# Voir les logs en direct
docker compose logs -f api
docker compose logs -f consumer3

# Vérifier que tout tourne
docker compose ps

# Compter les documents MongoDB
docker exec mongodb mongosh crypto_monitor --eval 'db.trades_raw.countDocuments()'

# Tester l'API
curl http://localhost:8000/api/health
curl http://localhost:8000/api/snapshot | python3 -m json.tool
```
