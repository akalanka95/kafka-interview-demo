# Kafka Interview Demo

A secured 3-broker Kafka cluster with two client apps:
- a **Spring Boot + React** web app that demonstrates delivery semantics
- a **Python** pipeline that demonstrates exactly-once processing with retries and a DLQ

Design docs: [docs/architecture.md](docs/architecture.md) → [infra](docs/infra.md) · [backend](docs/backend.md) · [frontend](docs/frontend.md) · [python-app](docs/python-app.md)

## Status

| Layer | State |
|---|---|
| Infra (Kafka cluster, topics, ACLs) | ✅ done, smoke test passing |
| Backend (Spring Boot) | ⏳ next |
| Frontend (React, UI_v1) | ⏳ |
| Python pipeline | ⏳ (needs Python 3.11/3.12 installed) |

## Quick start: infra

Prereqs: Docker Desktop running (≥ 4 GB RAM, 6 GB recommended).

```powershell
docker compose -f infra/docker-compose.yml up -d     # ~60 s first time
powershell -ExecutionPolicy Bypass -File .\smoke-test.ps1
```

| What | Where |
|---|---|
| Brokers (SASL_PLAINTEXT, SCRAM-SHA-512) | `localhost:19092`, `localhost:29092`, `localhost:39092` |
| Kafka UI | http://localhost:8081 |
| Client credentials | `infra/client-configs/<user>.properties` (demo-only passwords `<user>-secret`) |

Full reset (wipes data and users): `docker compose -f infra/docker-compose.yml down -v`

More commands: [docs/infra.md §11](docs/infra.md#11-operations-cheatsheet-powershell-from-repo-root).

## Branching

| Branch | Purpose |
|---|---|
| `main` | Released, demo-ready state |
| `develop` | Integration branch; features merge here via pull request |
| `feature/*` | One branch per layer or change, branched from `develop` |
