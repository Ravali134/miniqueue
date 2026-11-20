import pytest

from miniqueue import MemoryBackend, Status, Task, configure, get_backend, task


@pytest.fixture(autouse=True)
def backend():
    return configure(MemoryBackend())


@task
def t_plain(a, b):
    """docstring kept"""
    return a + b


@task(name="custom_name", retries=3, timeout=5)
def t_configured(x):
    return x


def test_bare_and_parameterised_decorator_forms():
    assert isinstance(t_plain, Task) and isinstance(t_configured, Task)
    assert (t_plain.retries, t_plain.timeout) == (0, None)
    assert (t_configured.retries, t_configured.timeout) == (3, 5)


def test_task_is_still_a_normal_callable_and_keeps_metadata():
    assert t_plain(2, 3) == 5
    assert t_plain.__name__ == "t_plain"
    assert t_plain.__doc__ == "docstring kept"


def test_registry_uses_custom_name():
    assert Task.registry["custom_name"] is t_configured
    assert Task.registry["t_plain"] is t_plain


def test_delay_enqueues_a_pending_job_copying_task_settings():
    job = t_configured.delay(7)
    stored = get_backend().get(job.id)
    assert stored.status is Status.PENDING
    assert (stored.task_name, stored.args, stored.max_retries, stored.timeout) == (
        "custom_name",
        [7],
        3,
        5,
    )


def test_delay_in_sets_future_run_at():
    job = t_plain.delay_in(60, 1, 2)
    assert job.run_at > job.created_at + 59
    assert get_backend().claim("w") is None  # not runnable yet


def test_negative_retries_rejected():
    with pytest.raises(ValueError):
        task(retries=-1)(lambda: None)
