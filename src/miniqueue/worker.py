"""Workers and the worker pool.

CONCEPTS: generators (``Worker.jobs``), context managers (``time_limit``,
``WorkerPool``), ``multiprocessing`` with the ``spawn`` start method, signals
(SIGALRM for timeouts, SIGTERM for graceful shutdown), exponential backoff.

Why *processes* and not threads? Because of the GIL: CPU-bound tasks in threads
run one at a time. Separate processes each have their own interpreter + GIL, so
N workers genuinely use N cores. It also means a task that crashes hard
(segfault, ``kill -9``) takes down only its own worker, not the whole system.
"""

from __future__ import annotations

import importlib
import logging
import multiprocessing as mp
import os
import signal
import threading
import time
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from typing import Any

from .backends import Backend
from .config import configure
from .job import Job
from .task import Task

log = logging.getLogger("miniqueue.worker")


class TaskTimeout(Exception):
    """Raised inside a task when it exceeds its time limit."""


@contextmanager
def time_limit(seconds: float | None) -> Iterator[None]:
    """``with time_limit(5): slow()`` raises ``TaskTimeout`` after 5 seconds.

    Uses SIGALRM, so it works on Unix (macOS/Linux) in the main thread, which is
    where each worker process runs its tasks. Elsewhere it quietly does nothing.
    """
    if not seconds or threading.current_thread() is not threading.main_thread():
        yield
        return

    def on_alarm(signum: int, frame: Any) -> None:
        raise TaskTimeout(f"timed out after {seconds}s")

    previous = signal.signal(signal.SIGALRM, on_alarm)
    signal.setitimer(signal.ITIMER_REAL, seconds)
    try:
        yield
    finally:  # always disarm the timer and restore the old handler
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)


class Worker:
    """Pulls jobs from a backend one at a time and runs them."""

    def __init__(
        self,
        backend: Backend,
        *,
        worker_id: str | None = None,
        poll_interval: float = 0.5,
        visibility_timeout: float = 60.0,
        backoff_base: float = 1.0,
        backoff_cap: float = 60.0,
        stop_event: Any = None,  # threading.Event or multiprocessing.Event
    ) -> None:
        self.backend = backend
        self.worker_id = worker_id or f"worker-{os.getpid()}"
        self.poll_interval = poll_interval
        self.visibility_timeout = visibility_timeout
        self.backoff_base = backoff_base
        self.backoff_cap = backoff_cap
        self.stop_event = stop_event or threading.Event()

    # ---- the polling loop, written as a generator --------------------------

    def jobs(self) -> Iterator[Job]:
        """Yield claimed jobs forever (until stopped).

        A generator keeps the "how do I find work" logic separate from the "what
        do I do with it" logic: ``for job in worker.jobs(): handle(job)``.
        """
        sweep_every = max(0.5, self.visibility_timeout / 4)
        last_sweep = 0.0
        while not self.stop_event.is_set():
            now = time.time()
            if now - last_sweep >= sweep_every:  # rescue jobs from dead workers
                recovered = self.backend.requeue_stale(self.visibility_timeout)
                if recovered:
                    log.warning("%s recovered %d stale job(s)", self.worker_id, recovered)
                last_sweep = now
            job = self.backend.claim(self.worker_id)
            if job is None:
                self.stop_event.wait(self.poll_interval)  # sleep, but wake early on stop
                continue
            yield job

    def run(self, max_jobs: int | None = None) -> int:
        done = 0
        for job in self.jobs():
            self.execute(job)
            done += 1
            if max_jobs is not None and done >= max_jobs:
                break
        return done

    def run_once(self) -> bool:
        """Claim and run at most one job. Handy in tests. True if one ran."""
        job = self.backend.claim(self.worker_id)
        if job is None:
            return False
        self.execute(job)
        return True

    # ---- running one job ---------------------------------------------------

    def backoff(self, attempts: int) -> float:
        """1s, 2s, 4s, 8s ... capped. Don't hammer something that's already failing."""
        return min(self.backoff_base * 2 ** (attempts - 1), self.backoff_cap)

    def execute(self, job: Job) -> None:
        task = Task.registry.get(job.task_name)
        if task is None:
            self.backend.fail(job.id, f"unknown task {job.task_name!r}", retry_at=None)
            log.error("job %s: unknown task %r", job.id[:8], job.task_name)
            return

        log.info("job %s: %s started (attempt %d)", job.id[:8], job.task_name, job.attempts)
        try:
            with time_limit(job.timeout):
                result = task(*job.args, **job.kwargs)
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"
            will_retry = job.attempts <= job.max_retries
            retry_at = time.time() + self.backoff(job.attempts) if will_retry else None
            self.backend.fail(job.id, error, retry_at)
            log.warning(
                "job %s: %s failed (%s) -> %s",
                job.id[:8],
                job.task_name,
                error,
                "retry" if will_retry else "giving up",
            )
        else:
            self.backend.complete(job.id, result)
            log.info("job %s: %s succeeded", job.id[:8], job.task_name)


# ---- multi-process pool ----------------------------------------------------


def _worker_main(url: str, modules: tuple[str, ...], stop_event: Any, opts: dict) -> None:
    """Entry point of each worker *process*. Must be a top-level function so
    ``spawn`` can pickle a reference to it."""
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(processName)s %(message)s", datefmt="%H:%M:%S"
    )
    for module in modules:  # importing runs the @task decorators => fills Task.registry
        importlib.import_module(module)
    backend = configure(url)  # so tasks can call other tasks' .delay() too
    worker = Worker(backend, stop_event=stop_event, **opts)

    # Graceful shutdown: on SIGTERM/SIGINT finish the current job, then exit.
    def request_stop(signum: int, frame: Any) -> None:
        stop_event.set()

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)
    worker.run()


class WorkerPool:
    """``with WorkerPool(url, size=4): ...`` starts 4 worker processes and
    guarantees they are stopped again afterwards (a context manager)."""

    def __init__(
        self,
        url: str,
        modules: Iterable[str] = ("miniqueue.examples",),
        size: int = 2,
        **worker_opts: Any,
    ) -> None:
        self.url = url
        self.modules = tuple(modules)
        self.size = size
        self.worker_opts = worker_opts
        self.processes: list[mp.process.BaseProcess] = []
        self._ctx = mp.get_context("spawn")  # fresh interpreters: no inherited state/locks
        self._stop = self._ctx.Event()

    def start(self) -> None:
        for i in range(self.size):
            p = self._ctx.Process(
                target=_worker_main,
                args=(self.url, self.modules, self._stop, self.worker_opts),
                name=f"worker-{i + 1}",
                daemon=True,
            )
            p.start()
            self.processes.append(p)

    def stop(self, timeout: float = 10.0) -> None:
        self._stop.set()
        deadline = time.time() + timeout
        for p in self.processes:
            p.join(max(0.0, deadline - time.time()))
            if p.is_alive():
                p.terminate()
        self.processes.clear()

    @property
    def pids(self) -> list[int]:
        return [p.pid for p in self.processes if p.pid is not None]

    def __enter__(self) -> WorkerPool:
        self.start()
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.stop()
