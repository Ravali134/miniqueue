"""The ``@task`` decorator.

CONCEPTS: decorators *with* and *without* arguments, ``functools.update_wrapper``,
class-based decorators (``Task`` is a callable object), ``Generic`` + ``ParamSpec``
so type checkers know ``add.delay(1, 2)`` takes the same arguments as ``add``,
``typing.overload``, a class-level registry.

    @task(retries=3, timeout=5)
    def send_report(user_id: int) -> str: ...

    send_report(42)         # runs right here, like a normal function
    send_report.delay(42)   # puts a job on the queue; a worker runs it later
"""

from __future__ import annotations

import functools
import time
from collections.abc import Callable
from typing import Any, ClassVar, Generic, ParamSpec, TypeVar, overload

from .config import get_backend
from .job import Job

P = ParamSpec("P")  # the decorated function's parameters
R = TypeVar("R")  # ... and its return type


class Task(Generic[P, R]):
    #: name -> Task. Workers look tasks up here by the name stored in the job.
    registry: ClassVar[dict[str, Task[..., Any]]] = {}

    def __init__(
        self,
        fn: Callable[P, R],
        *,
        name: str | None = None,
        retries: int = 0,
        timeout: float | None = None,
    ) -> None:
        if retries < 0:
            raise ValueError("retries must be >= 0")
        functools.update_wrapper(self, fn)  # copy __name__, __doc__, ... onto this object
        self.fn = fn
        self.name = name or fn.__name__
        self.retries = retries
        self.timeout = timeout
        Task.registry[self.name] = self  # auto-register

    # Calling the task directly just calls the function.
    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R:
        return self.fn(*args, **kwargs)

    def make_job(
        self, args: tuple | list = (), kwargs: dict | None = None, delay: float = 0.0
    ) -> Job:
        return Job(
            task_name=self.name,
            args=list(args),
            kwargs=dict(kwargs or {}),
            max_retries=self.retries,
            timeout=self.timeout,
            run_at=time.time() + delay,
        )

    def delay(self, *args: P.args, **kwargs: P.kwargs) -> Job:
        """Enqueue now. Returns the Job so you can look at ``job.id`` / poll it."""
        job = self.make_job(args, kwargs)
        get_backend().enqueue(job)
        return job

    def delay_in(self, seconds: float, /, *args: P.args, **kwargs: P.kwargs) -> Job:
        """Enqueue, but don't let a worker start it for ``seconds`` seconds."""
        job = self.make_job(args, kwargs, delay=seconds)
        get_backend().enqueue(job)
        return job

    def __repr__(self) -> str:
        return f"<Task {self.name} retries={self.retries} timeout={self.timeout}>"


# Two @overload signatures describe the two ways to use the decorator; only the
# last, un-decorated ``def`` below actually exists at runtime.
@overload
def task(fn: Callable[P, R], /) -> Task[P, R]: ...
@overload
def task(
    *, name: str | None = ..., retries: int = ..., timeout: float | None = ...
) -> Callable[[Callable[P, R]], Task[P, R]]: ...
def task(
    fn: Callable[P, R] | None = None,
    /,
    *,
    name: str | None = None,
    retries: int = 0,
    timeout: float | None = None,
):
    """Use as ``@task`` or ``@task(retries=3, timeout=5)``."""

    def decorate(f: Callable[P, R]) -> Task[P, R]:
        return Task(f, name=name, retries=retries, timeout=timeout)

    # Bare ``@task``  -> Python passes the function straight in.
    # ``@task(...)``  -> fn is None, so return the real decorator to be applied next.
    return decorate if fn is None else decorate(fn)
