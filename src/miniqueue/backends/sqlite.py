"""SQLite backend: durable, and safely shared between worker *processes*.

CONCEPTS: context managers (``@contextmanager``) used for connections and
transactions, atomic claim via ``BEGIN IMMEDIATE``, WAL mode for concurrency.

Why ``BEGIN IMMEDIATE``?  It takes the database write lock *up front*. So the
sequence "SELECT oldest pending job, then UPDATE it to running" happens as one
indivisible step. Two workers can never both SELECT the same row, because the
second one has to wait for the first to COMMIT.
"""

from __future__ import annotations

import json
import sqlite3
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from ..job import Job, Status
from .base import Backend

_SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id         TEXT PRIMARY KEY,
    status     TEXT NOT NULL,
    run_at     REAL NOT NULL,
    created_at REAL NOT NULL,
    started_at REAL,
    data       TEXT NOT NULL            -- the whole Job as JSON
);
CREATE INDEX IF NOT EXISTS idx_jobs_claim ON jobs (status, run_at);
"""


class SQLiteBackend(Backend, scheme="sqlite"):
    def __init__(self, path: str | Path = "miniqueue.db") -> None:
        self.path = str(path)
        Path(self.path).expanduser().parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as conn:
            conn.execute("PRAGMA journal_mode=WAL")  # readers don't block the writer
            conn.executescript(_SCHEMA)

    @classmethod
    def _from_url_rest(cls, rest: str) -> SQLiteBackend:
        # "sqlite:///jobs.db" -> rest="/jobs.db"  (relative path)
        # "sqlite:////tmp/jobs.db" -> rest="//tmp/jobs.db" (absolute path)
        return cls(rest[1:] if rest.startswith("/") else rest)

    # ---- connection helpers (context managers) -----------------------------

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        # One short-lived connection per operation: simple and thread/process safe.
        conn = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        try:
            yield conn
        finally:
            conn.close()

    @contextmanager
    def _tx(self) -> Iterator[sqlite3.Connection]:
        """``with self._tx() as conn:`` => BEGIN IMMEDIATE ... COMMIT (or ROLLBACK)."""
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                yield conn
            except BaseException:
                conn.execute("ROLLBACK")
                raise
            else:
                conn.execute("COMMIT")

    # ---- row <-> Job -------------------------------------------------------

    @staticmethod
    def _dump(job: Job) -> str:
        return json.dumps(job.to_dict(), default=repr)  # unserialisable results -> repr()

    @staticmethod
    def _load(data: str) -> Job:
        return Job.from_dict(json.loads(data))

    def _save(self, conn: sqlite3.Connection, job: Job) -> None:
        conn.execute(
            "UPDATE jobs SET status=?, run_at=?, started_at=?, data=? WHERE id=?",
            (job.status.value, job.run_at, job.started_at, self._dump(job), job.id),
        )

    def _load_for_update(self, conn: sqlite3.Connection, job_id: str) -> Job:
        row = conn.execute("SELECT data FROM jobs WHERE id=?", (job_id,)).fetchone()
        if row is None:
            raise KeyError(f"no such job: {job_id}")
        return self._load(row[0])

    # ---- Backend contract --------------------------------------------------

    def enqueue(self, job: Job) -> None:
        with self._tx() as conn:
            conn.execute(
                "INSERT INTO jobs (id, status, run_at, created_at, started_at, data)"
                " VALUES (?, ?, ?, ?, ?, ?)",
                (
                    job.id,
                    job.status.value,
                    job.run_at,
                    job.created_at,
                    job.started_at,
                    self._dump(job),
                ),
            )

    def claim(self, worker_id: str, now: float | None = None) -> Job | None:
        now = time.time() if now is None else now
        with self._tx() as conn:
            row = conn.execute(
                "SELECT data FROM jobs WHERE status='pending' AND run_at<=?"
                " ORDER BY run_at, created_at LIMIT 1",
                (now,),
            ).fetchone()
            if row is None:
                return None
            job = self._load(row[0])
            job.mark_running(worker_id, now)
            self._save(conn, job)
            return job

    def complete(self, job_id: str, result: object) -> None:
        with self._tx() as conn:
            job = self._load_for_update(conn, job_id)
            job.mark_success(result, time.time())
            self._save(conn, job)

    def fail(self, job_id: str, error: str, retry_at: float | None = None) -> None:
        with self._tx() as conn:
            job = self._load_for_update(conn, job_id)
            job.mark_failed(error, retry_at, time.time())
            self._save(conn, job)

    def get(self, job_id: str) -> Job | None:
        with self._connect() as conn:
            row = conn.execute("SELECT data FROM jobs WHERE id=?", (job_id,)).fetchone()
        return self._load(row[0]) if row else None

    def list_jobs(
        self, *, status: Status | None = None, limit: int = 50, offset: int = 0
    ) -> list[Job]:
        sql = "SELECT data FROM jobs"
        params: list[object] = []
        if status is not None:
            sql += " WHERE status=?"
            params.append(status.value)
        sql += " ORDER BY created_at DESC, id LIMIT ? OFFSET ?"
        params += [limit, offset]
        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [self._load(r[0]) for r in rows]

    def stats(self) -> dict[str, int]:
        counts = {s.value: 0 for s in Status}
        with self._connect() as conn:
            for status, n in conn.execute("SELECT status, COUNT(*) FROM jobs GROUP BY status"):
                counts[status] = n
        counts["total"] = sum(counts.values())
        return counts

    def requeue_stale(self, older_than: float, now: float | None = None) -> int:
        now = time.time() if now is None else now
        with self._tx() as conn:
            rows = conn.execute(
                "SELECT data FROM jobs WHERE status='running' AND started_at<?",
                (now - older_than,),
            ).fetchall()
            for (data,) in rows:
                job = self._load(data)
                job.mark_lost(now)
                self._save(conn, job)
            return len(rows)

    def clear(self) -> None:
        with self._tx() as conn:
            conn.execute("DELETE FROM jobs")
