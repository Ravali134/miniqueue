"""Which backend does ``my_task.delay(...)`` talk to? Configured here."""

from __future__ import annotations

import os
import threading

from .backends import Backend

DEFAULT_URL = "sqlite:///miniqueue.db"

_backend: Backend | None = None
_lock = threading.Lock()


def configure(target: str | Backend) -> Backend:
    """Choose the backend: a URL like ``"sqlite:///jobs.db"`` or an instance."""
    global _backend
    with _lock:
        _backend = Backend.from_url(target) if isinstance(target, str) else target
        return _backend


def get_backend() -> Backend:
    """The configured backend; lazily created from ``$MINIQUEUE_URL`` on first use."""
    if _backend is None:
        configure(os.environ.get("MINIQUEUE_URL", DEFAULT_URL))
    assert _backend is not None
    return _backend
