# Python pipeline (Task 3)

Idempotent producer → `orders` → consumer with a dedup table (exactly-once processing) →
in-place retries → `orders.retry` (delayed hops) → `orders.dlq` → inspector with replay.
Design: [docs/python-app.md](../docs/python-app.md).

## Setup (PowerShell, from `python-app/`)

Needs Python 3.11/3.12 and the cluster from `infra/` running.

```powershell
py -3.12 -m venv .venv; .\.venv\Scripts\Activate.ps1; pip install -r requirements.txt
python -m unittest discover -s tests      # unit tests, no Kafka needed
```

## Run (one terminal each)

```powershell
python consumer.py           # group orders-processor
python retry_consumer.py     # group orders-retry-processor
python producer.py --count 20
python db_stats.py           # row counts + retry/DLQ end offsets
```

## Demo scenarios

| # | Do | Expect |
|---|---|---|
| F3 | `python producer.py --user intruder --count 1` | Authorization error, exit code 1 |
| F7 | `python producer.py --duplicate 5` | 5 × `DUPLICATE SKIPPED`, `orders` rows unchanged |
| F8 | `python producer.py --count 200 --rate 20`, then `Stop-Process -Id <consumer pid> -Force`, restart | rows = previous + 200 |
| F9 | `New-Item .fail_downstream` (downstream down), `producer.py --count 1`, wait for `→ orders.retry hop 1`, then `Remove-Item .fail_downstream` within 5 s | consumer: `RETRY 1/3`, `RETRY 2/3` (~1.5 s) → `orders.retry hop 1`; retry consumer: `WAITING` → `PROCESSED (hop 1)`. Missed the 5 s? It succeeds on hop 2 (+10 s) or hop 3 (+20 s) instead |
| F10 | Same as F9 but never remove the flag | hop 1 (+5 s) → hop 2 (+10 s) → hop 3 (+20 s) → `orders.dlq` after ~40 s, `error-class=TransientError`, `retry-count=3`. Then remove the flag and `--replay` it (F12) |
| F11 | `python producer.py --poison 2` | 2 records in `orders.dlq`, good records keep flowing |
| F12 | `python dlq_inspector.py --replay <offset>` twice (flag removed) | 1st `PROCESSED`, 2nd `DUPLICATE SKIPPED` |
| F13 | start a second `consumer.py` | `assigned [...]` split across both, no double rows |

DLQ: `python dlq_inspector.py --list`, `--show <offset>`, `--replay <offset>`.

Each app prints its PID at startup (for `Stop-Process`). Ctrl+C stops cleanly and leaves the group.
Reset local state: delete `data/` (keep `orders.db` and `last_sent.jsonl` in step, or new order IDs
may collide with old rows and go to the DLQ as conflicting orders).
