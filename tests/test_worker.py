import time

import pytest

from miniqueue import MemoryBackend, Status, Worker, task
from miniqueue.worker import TaskTimeout, time_limit

CALLS = {"flaky_ok_on_3": 0}


@task
def w_add(a, b):
    return a + b


@task(retries=2)
def w_flaky_ok_on_3():
    CALLS["flaky_ok_on_3"] += 1
    if CALLS["flaky_ok_on_3"] < 3:
        raise RuntimeError("not yet")
    return "finally"


@task(retries=1)
def w_always_fails():
    raise ValueError("nope")


@task(timeout=0.2)
def w_sleepy():
    time.sleep(2)


@pytest.fixture
def setup():
    backend = MemoryBackend()
    worker = Worker(backend, worker_id="test", backoff_base=0)  # retry immediately
    return backend, worker


def run(backend, worker, t, *args):
    job = t.make_job(args)
    backend.enqueue(job)
    while worker.run_once():
        pass
    return backend.get(job.id)


def test_success(setup):
    job = run(*setup, w_add, 2, 3)
    assert job.status is Status.SUCCESS and job.result == 5 and job.attempts == 1


def test_retries_until_success(setup):
    CALLS["flaky_ok_on_3"] = 0
    job = run(*setup, w_flaky_ok_on_3)
    assert job.status is Status.SUCCESS and job.attempts == 3 and job.result == "finally"


def test_gives_up_after_retries_exhausted(setup):
    job = run(*setup, w_always_fails)
    assert job.status is Status.FAILED
    assert job.attempts == 2  # first try + 1 retry
    assert job.error == "ValueError: nope"


def test_timeout_fails_the_job(setup):
    start = time.time()
    job = run(*setup, w_sleepy)
    assert time.time() - start < 1.5
    assert job.status is Status.FAILED and "TaskTimeout" in job.error


def test_unknown_task_fails_cleanly(setup):
    backend, worker = setup
    from miniqueue import Job

    j = Job(task_name="does_not_exist")
    backend.enqueue(j)
    worker.run_once()
    assert backend.get(j.id).status is Status.FAILED


def test_backoff_doubles_and_caps():
    w = Worker(MemoryBackend(), backoff_base=1, backoff_cap=10)
    assert [w.backoff(n) for n in (1, 2, 3, 4, 5)] == [1, 2, 4, 8, 10]


def test_time_limit_context_manager():
    with pytest.raises(TaskTimeout):
        with time_limit(0.1):
            time.sleep(1)
    with time_limit(None):  # no-op
        pass


def test_jobs_generator_stops_when_stop_event_is_set(setup):
    backend, worker = setup
    backend.enqueue(w_add.make_job((1, 1)))
    seen = []
    for job in worker.jobs():
        seen.append(job)
        worker.stop_event.set()
    assert len(seen) == 1
