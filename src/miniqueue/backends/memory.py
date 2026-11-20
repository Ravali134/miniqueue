"""In-memory backend: great for tests and single-process use.

NOT shared between processes (each process gets its own dict), so the worker
pool uses SQLite instead. Thread-safe thanks to one lock.
"""

from __future__ import annotations

import copy
import threading
import time

from ..job import Job, Status
from .base import Backend


class MemoryBackend(Backend, scheme="memory"):
    def __init__(self) -> None:
        self._jobs: dict[str, Job] = {}
        self._lock = threading.RLock()

    # We store and return *copies*, so callers can't mutate our state by
    # accident. That mimics a real database, where you always get fresh rows.

    def enqueue(self, job: Job) -> None:
        with self._lock:
            self._jobs[job.id] = copy.deepcopy(job)

    def claim(self, worker_id: str, now: float | None = None) -> Job | None:
        now = time.time() if now is None else now
        with self._lock:
            runnable = [
                j for j in self._jobs.values() if j.status is Status.PENDING and j.run_at <= now
            ]
            if not runnable:
                return None
            job = min(runnable, key=lambda j: (j.run_at, j.created_at))
            job.mark_running(worker_id, now)
            return copy.deepcopy(job)

    def complete(self, job_id: str, result: object) -> None:
        with self._lock:
            self._jobs[job_id].mark_success(result, time.time())

    def fail(self, job_id: str, error: str, retry_at: float | None = None) -> None:
        with self._lock:
            self._jobs[job_id].mark_failed(error, retry_at, time.time())

    def get(self, job_id: str) -> Job | None:
        with self._lock:
            job = self._jobs.get(job_id)
            return copy.deepcopy(job) if job else None

    def list_jobs(
        self, *, status: Status | None = None, limit: int = 50, offset: int = 0
    ) -> list[Job]:
        with self._lock:
            jobs = [j for j in self._jobs.values() if status is None or j.status is status]
            jobs.sort(key=lambda j: j.created_at, reverse=True)
            return copy.deepcopy(jobs[offset : offset + limit])

    def stats(self) -> dict[str, int]:
        with self._lock:
            counts = {s.value: 0 for s in Status}
            for job in self._jobs.values():
                counts[job.status.value] += 1
            counts["total"] = len(self._jobs)
            return counts

    def requeue_stale(self, older_than: float, now: float | None = None) -> int:
        now = time.time() if now is None else now
        cutoff = now - older_than
        n = 0
        with self._lock:
            for job in self._jobs.values():
                if job.status is Status.RUNNING and job.started_at is not None:
                    if job.started_at < cutoff:
                        job.mark_lost(now)
                        n += 1
        return n

    def clear(self) -> None:
        with self._lock:
            self._jobs.clear()
