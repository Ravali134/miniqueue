import threading

import pytest

from miniqueue import Backend, Job, MemoryBackend, SQLiteBackend, Status


@pytest.fixture(params=["memory", "sqlite"])
def backend(request, tmp_path):
    if request.param == "memory":
        return MemoryBackend()
    return SQLiteBackend(tmp_path / "q.db")


def make(**kw):
    # Tests drive time with explicit `now=` values, so jobs are due from t=0 by default.
    kw.setdefault("run_at", 0)
    kw.setdefault("created_at", 0)
    return Job(task_name="t", **kw)


def test_enqueue_get_roundtrip(backend):
    job = make(args=[1, 2], kwargs={"k": "v"})
    backend.enqueue(job)
    got = backend.get(job.id)
    assert got == job
    assert backend.get("missing") is None


def test_claim_is_fifo_and_marks_running(backend):
    a, b = make(created_at=1, run_at=1), make(created_at=2, run_at=2)
    backend.enqueue(b)
    backend.enqueue(a)
    first = backend.claim("w1", now=10)
    assert first.id == a.id
    assert first.status is Status.RUNNING and first.attempts == 1 and first.worker == "w1"
    assert backend.claim("w2", now=10).id == b.id
    assert backend.claim("w3", now=10) is None


def test_delayed_job_is_not_claimable_until_due(backend):
    job = make(run_at=100)
    backend.enqueue(job)
    assert backend.claim("w", now=50) is None
    assert backend.claim("w", now=100).id == job.id


def test_complete_and_fail_without_retry(backend):
    ok, bad = make(), make()
    backend.enqueue(ok)
    backend.enqueue(bad)
    backend.claim("w")
    backend.claim("w")
    backend.complete(ok.id, {"answer": 42})
    backend.fail(bad.id, "boom", retry_at=None)
    assert backend.get(ok.id).status is Status.SUCCESS
    assert backend.get(ok.id).result == {"answer": 42}
    failed = backend.get(bad.id)
    assert failed.status is Status.FAILED and failed.error == "boom"


def test_fail_with_retry_goes_back_to_pending(backend):
    job = make()
    backend.enqueue(job)
    backend.claim("w", now=1)
    backend.fail(job.id, "oops", retry_at=500)
    again = backend.get(job.id)
    assert again.status is Status.PENDING and again.run_at == 500 and again.worker is None
    assert backend.claim("w", now=499) is None
    assert backend.claim("w", now=500).attempts == 2


def test_requeue_stale_recovers_or_fails_lost_jobs(backend):
    retryable, poison = make(max_retries=1), make(max_retries=0)
    backend.enqueue(retryable)
    backend.enqueue(poison)
    backend.claim("dead", now=100)
    backend.claim("dead", now=100)
    assert backend.requeue_stale(older_than=60, now=130) == 0  # not stale yet
    assert backend.requeue_stale(older_than=60, now=200) == 2
    assert backend.get(retryable.id).status is Status.PENDING
    assert backend.get(poison.id).status is Status.FAILED


def test_stats_and_list_and_stream(backend):
    for i in range(5):
        backend.enqueue(make(created_at=i, run_at=i))
    backend.claim("w", now=100)
    stats = backend.stats()
    assert (stats["pending"], stats["running"], stats["total"]) == (4, 1, 5)
    listed = backend.list_jobs(limit=3)
    assert [j.created_at for j in listed] == [4, 3, 2]  # newest first
    assert len(list(backend.stream(page_size=2))) == 5  # generator pages through everything
    assert len(backend.list_jobs(status=Status.RUNNING)) == 1


def test_concurrent_claims_never_hand_out_the_same_job(backend):
    for _ in range(40):
        backend.enqueue(make())
    claimed, lock = [], threading.Lock()

    def drain(name):
        while (job := backend.claim(name)) is not None:
            with lock:
                claimed.append(job.id)

    threads = [threading.Thread(target=drain, args=(f"w{i}",)) for i in range(6)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    assert len(claimed) == 40 and len(set(claimed)) == 40


def test_backend_registry_and_from_url(tmp_path):
    assert set(Backend.registry) >= {"memory", "sqlite"}
    assert isinstance(Backend.from_url("memory://"), MemoryBackend)
    b = Backend.from_url(f"sqlite:///{tmp_path}/rel.db")
    assert isinstance(b, SQLiteBackend)
    with pytest.raises(ValueError):
        Backend.from_url("nope://x")
