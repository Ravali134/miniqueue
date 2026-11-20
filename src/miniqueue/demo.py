"""``miniqueue demo``: a self-contained run in a temporary database, printed to the terminal."""

from __future__ import annotations

import os
import signal
import tempfile
import time
from pathlib import Path

from .config import configure
from .examples import add, fib, flaky, hang, send_email, slow_square
from .job import TERMINAL, Status
from .worker import WorkerPool


def run_demo(workers: int = 3, crash: bool = False) -> None:
    db = Path(tempfile.mkdtemp(prefix="miniqueue-demo-")) / "demo.db"
    url = f"sqlite:///{db}" if not db.is_absolute() else f"sqlite:////{str(db).lstrip('/')}"
    backend = configure(url)

    print(f"Demo database: {db}")
    print(f"Starting {workers} worker processes...\n")
    opts = dict(poll_interval=0.2, visibility_timeout=5.0, backoff_base=0.5)
    with WorkerPool(url, size=workers, **opts) as pool:
        for _ in range(8):
            add.delay(2, 3)
        for _ in range(4):
            fib.delay(30)
        for _ in range(3):
            flaky.delay()
        for _ in range(2):
            send_email.delay("ada@example.com")
        hang.delay()  # will exceed its 2s limit
        victim = slow_square.delay(7, seconds=4) if crash else None

        if victim:
            print("Waiting for a worker to pick up the slow job, then killing it (kill -9)...")
            while (job := backend.get(victim.id)) and job.status is not Status.RUNNING:
                time.sleep(0.1)
            pid = int(job.worker.split("-")[1])
            os.kill(pid, signal.SIGKILL)
            print(f"Killed {job.worker}. Another worker should rescue the job in ~5s.\n")

        while True:
            stats = backend.stats()
            print(
                f"\r pending={stats['pending']:<3} running={stats['running']:<3} "
                f"success={stats['success']:<3} failed={stats['failed']:<3}",
                end="",
                flush=True,
            )
            if stats["pending"] == 0 and stats["running"] == 0:
                break
            time.sleep(0.3)
        print("\n")

        for job in reversed(backend.list_jobs(limit=100)):
            assert job.status in TERMINAL
            detail = job.error or job.result
            print(f" {job.status.value:<8} {job.task_name:<12} tries={job.attempts}  {detail}")
    print("\nDone. Run `miniqueue up` for the live dashboard.")
