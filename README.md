# miniqueue

A tiny Celery-style **distributed task queue built from scratch in Python**: decorators, worker
processes, retries with exponential backoff, timeouts, crash recovery, pluggable storage
backends and a live web dashboard. About 1,200 lines (many of them explanatory comments) and 36 tests.

```python
from miniqueue import task

@task(retries=3, timeout=5)
def send_report(user_id: int) -> str: ...

send_report(42)          # runs right here, like a normal function
job = send_report.delay(42)   # queued; a worker process runs it later
```

## Run it (2 commands)

```bash
cd ~/Desktop/workspace/personal/miniqueue
source .venv/bin/activate

miniqueue up        # starts 3 worker processes + dashboard, opens http://127.0.0.1:8000
```

On the dashboard press **Send demo burst**: 18 jobs appear and move through
`pending -> running -> success/failed` live. Ctrl-C stops everything cleanly.

| Command | What it does |
|---|---|
| `miniqueue up` | workers + dashboard together (`-c 5` for 5 workers, `--port 9000`) |
| `miniqueue worker -c 4` | only workers (run on several terminals/machines sharing one DB) |
| `miniqueue dashboard` | only the dashboard/API |
| `miniqueue demo` | scripted run in the terminal, throwaway database |
| `miniqueue demo --crash` | same, but **kill -9s a worker mid-job** to show recovery |
| `pytest` | the 36 tests |

Use your own tasks: put `@task` functions in `mytasks.py`, then `miniqueue up --tasks mytasks`.

## Architecture

```
 your code / dashboard / REST API
        │  task.delay(...)  →  Job(PENDING)
        ▼
 ┌─────────────────────────┐        ┌───────────────────────────────┐
 │ Backend (storage)       │◄──────►│ Worker process ×N             │
 │  MemoryBackend (tests)  │ claim  │  loop: claim → run → complete │
 │  SQLiteBackend (shared) │ (atomic│  retries, backoff, timeout    │
 └─────────────────────────┘        │  sweeps jobs of dead workers  │
        ▲                            └───────────────────────────────┘
        │ stats / jobs (SSE, every 1s)
 FastAPI dashboard
```

Job life cycle:

```
PENDING --claim--> RUNNING --ok-------------------------> SUCCESS
                      ├── error, retries left ----------> PENDING (run_at = now + backoff)
                      ├── error, none left -------------> FAILED
                      └── worker died (visibility timeout) -> PENDING, or FAILED if out of retries
```

## Where each advanced Python concept lives

| Concept | File | What to look for |
|---|---|---|
| Decorators with *and* without arguments | `task.py` | `task(fn=None, /, *, ...)` |
| Class-based decorator, `functools.update_wrapper` | `task.py` | `Task.__init__`, `__call__` |
| `Generic` + `ParamSpec` + `@overload` | `task.py` | `delay()` has the same signature as the task |
| Plugin registry via `__init_subclass__` | `backends/base.py` | `class SQLiteBackend(Backend, scheme="sqlite")` |
| Abstract base classes | `backends/base.py` | the `Backend` contract |
| Dataclasses with `slots`, `Enum`, state machine | `job.py` | `Job`, `Status`, `mark_*` |
| Context managers (`@contextmanager`, `__enter__/__exit__`) | `worker.py`, `backends/sqlite.py` | `time_limit`, `_tx`, `WorkerPool` |
| Generators | `worker.py`, `backends/base.py` | `Worker.jobs()`, `Backend.stream()` |
| `multiprocessing` (`spawn`), GIL reasoning | `worker.py` | `WorkerPool`, `_worker_main` |
| Signals (`SIGALRM`, `SIGTERM`) | `worker.py` | timeouts, graceful shutdown |
| Atomic claim / concurrency control | `backends/sqlite.py` | `BEGIN IMMEDIATE` in `claim` |
| `asyncio`, async generator, `to_thread`, SSE | `api.py` | `/api/stream` |
| FastAPI + pydantic validation | `api.py` | `EnqueueRequest` |
| Packaging, console scripts, `src` layout | `pyproject.toml` | `[project.scripts]` |
| Tests: parametrised fixtures, threads, real processes | `tests/` | `test_backends.py`, `test_pool.py` |

See **[STUDY_GUIDE.md](STUDY_GUIDE.md)** for a reading order and likely viva questions.

## REST API

`GET /api/tasks` · `POST /api/jobs` `{task, args, kwargs, delay, count}` · `GET /api/jobs?status=`
· `GET /api/jobs/{id}` · `GET /api/stats` · `GET /api/stream` (SSE) · OpenAPI docs at `/docs`.

## Limitations (honest ones)

- SQLite is great for one machine; a multi-machine deployment would want Redis/Postgres. The
  `Backend` interface exists precisely so that is one new class, not a rewrite.
- Timeouts use `SIGALRM`: Unix only (macOS/Linux), and can't interrupt a C extension that never
  returns to Python. A stricter design runs each task in a child process and kills it.
- Delivery is **at-least-once**: a job whose worker dies is run again, so tasks should be idempotent.
- No priorities, periodic schedules or task chaining yet (good extensions).

## Ideas to extend

Redis backend (`BLPOP`/Lua for atomic claim), priorities, cron-style periodic tasks, a heartbeat
so long jobs aren't mistaken for dead ones, a `Job.wait()` future, Docker compose, Prometheus metrics.
