"""The Job: a plain data record that describes one unit of work.

CONCEPTS: dataclasses (+ ``slots=True``), ``Enum`` with ``str`` mixin,
``field(default_factory=...)``, a tiny state machine expressed as methods.

A job moves through these states::

    PENDING --claim--> RUNNING --ok-------------------> SUCCESS
                          |
                          +--error, retries left------> PENDING (run_at in the future)
                          +--error, no retries left---> FAILED
                          +--worker died (stale)-------> PENDING or FAILED

All state transitions live *here*, not in the backends. Backends only store
jobs; that keeps the rules identical for memory, SQLite, Redis, ...
"""

from __future__ import annotations

import time
import uuid
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any


class Status(str, Enum):
    """``str`` mixin => ``Status.PENDING == "pending"`` and it JSON-encodes cleanly."""

    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"


TERMINAL = frozenset({Status.SUCCESS, Status.FAILED})


@dataclass(slots=True)
class Job:
    # What to run
    task_name: str
    args: list[Any] = field(default_factory=list)
    kwargs: dict[str, Any] = field(default_factory=dict)
    max_retries: int = 0
    timeout: float | None = None

    # Bookkeeping
    id: str = field(default_factory=lambda: uuid.uuid4().hex)
    status: Status = Status.PENDING
    attempts: int = 0
    result: Any = None
    error: str | None = None
    worker: str | None = None
    created_at: float = field(default_factory=time.time)
    run_at: float = field(default_factory=time.time)  # earliest time it may run
    started_at: float | None = None
    finished_at: float | None = None

    # ---- state transitions -------------------------------------------------

    def mark_running(self, worker_id: str, now: float) -> None:
        self.status = Status.RUNNING
        self.attempts += 1
        self.worker = worker_id
        self.started_at = now

    def mark_success(self, result: Any, now: float) -> None:
        self.status = Status.SUCCESS
        self.result = result
        self.error = None
        self.finished_at = now

    def mark_failed(self, error: str, retry_at: float | None, now: float) -> None:
        """Record a failed attempt. ``retry_at`` set => go back to the queue."""
        self.error = error
        if retry_at is not None:
            self.status = Status.PENDING
            self.run_at = retry_at
            self.worker = None
            self.started_at = None
        else:
            self.status = Status.FAILED
            self.finished_at = now

    def mark_lost(self, now: float) -> None:
        """A worker claimed this job and then vanished (crash, kill -9, power cut).

        If retries remain we re-queue it. If not, we fail it for good: a job that
        keeps killing its worker (a "poison pill") must not loop forever.
        """
        if self.attempts <= self.max_retries:
            self.mark_failed("worker lost (visibility timeout)", retry_at=now, now=now)
        else:
            self.mark_failed("worker lost (visibility timeout), no retries left", None, now)

    # ---- (de)serialisation -------------------------------------------------

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["status"] = self.status.value
        return d

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Job:
        data = dict(data)
        data["status"] = Status(data["status"])
        return cls(**data)
