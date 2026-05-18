# Crypto Market Monitor — Real-Time Pipeline

A production-ready streaming pipeline that ingests live trade data from Binance
and Coinbase via WebSocket, processes it through Apache Kafka, stores results in
MongoDB, and exposes them via a FastAPI backend with a real-time dashboard.

## Architecture

```
Binance WS ──┐
             ├──► Producer ──► Kafka (crypto.trades.raw) ──► Consumer 1 (storage)   ──► MongoDB
Coinbase WS ─┘                                           ├──► Consumer 2 (aggregator) ──► MongoDB
                                                         └──► Consumer 3 (anomaly)    ──► MongoDB
                                                                                           │
                                                                                     FastAPI (REST + WS)
                                                                                           │
                                                                                       Dashboard
```

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) 24+
- [Docker Compose](https://docs.docker.com/compose/) v2+

## Setup

```bash
cp .env.example .env
docker compose up --build
```

## Services

| Service    | Role                              | Port  |
|------------|-----------------------------------|-------|
| kafka      | Message broker (KRaft mode)       | 9092  |
| mongodb    | Document store                    | 27017 |
| producer   | Binance + Coinbase WS → Kafka     | —     |
| consumer1  | Raw trade storage                 | —     |
| consumer2  | VWAP / OHLCV aggregation          | —     |
| consumer3  | Anomaly detection + alerts        | —     |
| api        | FastAPI REST + WebSocket          | 8000  |
| dashboard  | Nginx serving HTML/JS frontend    | 3000  |

## Kafka Topic

`crypto.trades.raw` — all normalised trade events from both exchanges.

## Access

| Interface       | URL                          |
|-----------------|------------------------------|
| Dashboard       | http://localhost:3000        |
| API docs        | http://localhost:8000/docs   |
| API health      | http://localhost:8000/api/health |

## Status

> Work in progress — ingestion layer complete.
> Consumers, API, and dashboard are in active development.
