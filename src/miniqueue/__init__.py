"""miniqueue: a tiny Celery-style task queue, built from scratch."""

from .backends import Backend, MemoryBackend, SQLiteBackend
from .config import configure, get_backend
from .job import Job, Status
from .task import Task, task
from .worker import Worker, WorkerPool

__all__ = [
    "Backend",
    "Job",
    "MemoryBackend",
    "SQLiteBackend",
    "Status",
    "Task",
    "Worker",
    "WorkerPool",
    "configure",
    "get_backend",
    "task",
]
__version__ = "0.1.0"
