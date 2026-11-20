"""End-to-end: real worker *processes* sharing one SQLite file."""

import time

from miniqueue import SQLiteBackend, Status, WorkerPool
from miniqueue.config import configure
from miniqueue.examples import add, hang


def wait_until_idle(backend, timeout=30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        s = backend.stats()
        if s["pending"] == 0 and s["running"] == 0:
            return
        time.sleep(0.1)
    raise AssertionError(f"jobs still busy: {backend.stats()}")


def test_pool_processes_jobs_in_parallel_processes(tmp_path):
    path = tmp_path / "pool.db"
    backend = configure(SQLiteBackend(path))
    jobs = [add.delay(i, i) for i in range(12)] + [hang.delay()]

    with WorkerPool(f"sqlite:///{path}", size=3, poll_interval=0.05):
        wait_until_idle(backend)

    results = {j.id: backend.get(j.id) for j in jobs}
    assert all(results[j.id].status is Status.SUCCESS for j in jobs[:12])
    assert [results[j.id].result for j in jobs[:12]] == [i * 2 for i in range(12)]
    assert results[jobs[12].id].status is Status.FAILED
    assert "TaskTimeout" in results[jobs[12].id].error
    workers_used = {r.worker for r in results.values()}
    assert len(workers_used) >= 2  # work really was spread over several processes
