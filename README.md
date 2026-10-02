# Kafka Interview Demo

A secured 3-broker Kafka cluster with two client apps:
- a **Spring Boot + React** web app that demonstrates delivery semantics
- a **Python** pipeline that demonstrates exactly-once processing with retries and a DLQ

## Branching

| Branch | Purpose |
|---|---|
| `main` | Released, demo-ready state |
| `develop` | Integration branch; features merge here via pull request |
| `feature/*` | One branch per layer or change, branched from `develop` |
