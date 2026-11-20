"""The storage interface every backend implements.

CONCEPTS: abstract base classes, ``__init_subclass__`` (a lightweight
alternative to metaclasses) for automatic plugin registration, generators
(``stream``), class methods as alternative constructors.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterator
from typing import ClassVar

from ..job import Job, Status


class Backend(ABC):
    #: scheme -> class, filled in automatically as subclasses are *defined*
    registry: ClassVar[dict[str, type[Backend]]] = {}

    def __init_subclass__(cls, *, scheme: str | None = None, **kwargs) -> None:
        """Runs once for every ``class Foo(Backend, scheme="foo")`` statement.

        This is the "plugin registry" pattern. No metaclass needed: writing
        ``scheme="sqlite"`` in the class header is enough to make
        ``Backend.from_url("sqlite://...")`` find the class.
        """
        super().__init_subclass__(**kwargs)
        if scheme:
            Backend.registry[scheme] = cls

    # ---- construction ------------------------------------------------------

    @classmethod
    def from_url(cls, url: str) -> Backend:
        """``"sqlite:///jobs.db"`` -> ``SQLiteBackend("jobs.db")``."""
        scheme, sep, rest = url.partition("://")
        if not sep or scheme not in Backend.registry:
            known = ", ".join(sorted(Backend.registry)) or "none"
            raise ValueError(f"Unknown backend URL {url!r} (known schemes: {known})")
        return Backend.registry[scheme]._from_url_rest(rest)

    @classmethod
    def _from_url_rest(cls, rest: str) -> Backend:
        return cls()

    # ---- the contract ------------------------------------------------------

    @abstractmethod
    def enqueue(self, job: Job) -> None: ...

    @abstractmethod
    def claim(self, worker_id: str, now: float | None = None) -> Job | None:
        """Atomically take the oldest runnable PENDING job and mark it RUNNING.

        *Atomic* is the whole point: two workers calling this at the same time
        must never receive the same job.
        """

    @abstractmethod
    def complete(self, job_id: str, result: object) -> None: ...

    @abstractmethod
    def fail(self, job_id: str, error: str, retry_at: float | None = None) -> None: ...

    @abstractmethod
    def get(self, job_id: str) -> Job | None: ...

    @abstractmethod
    def list_jobs(
        self, *, status: Status | None = None, limit: int = 50, offset: int = 0
    ) -> list[Job]:
        """Newest first."""

    @abstractmethod
    def stats(self) -> dict[str, int]: ...

    @abstractmethod
    def requeue_stale(self, older_than: float, now: float | None = None) -> int:
        """Recover RUNNING jobs whose worker vanished. Returns how many."""

    @abstractmethod
    def clear(self) -> None: ...

    # ---- shared behaviour built on the contract ----------------------------

    def stream(self, status: Status | None = None, page_size: int = 100) -> Iterator[Job]:
        """Lazily yield *every* job, one page at a time (a generator).

        Memory stays O(page_size) however many jobs exist, because the next
        page is only fetched when the consumer asks for more.
        """
        offset = 0
        while True:
            page = self.list_jobs(status=status, limit=page_size, offset=offset)
            if not page:
                return
            yield from page
            offset += len(page)
