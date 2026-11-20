"""Example tasks used by the dashboard, the demo and the tests.

Workers import this module (``--tasks miniqueue.examples``), which runs every
``@task`` decorator below and so fills ``Task.registry``. Write your own module
the same way and pass its dotted name to ``--tasks``.
"""

from __future__ import annotations

import random
import time

from .task import task


@task
def add(a: float, b: float) -> float:
    """The simplest possible task."""
    return a + b


@task
def word_count(text: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for word in text.lower().split():
        counts[word] = counts.get(word, 0) + 1
    return counts


@task
def fib(n: int) -> int:
    """Deliberately slow, CPU-bound recursion: shows why we use processes."""

    def _fib(k: int) -> int:
        return k if k < 2 else _fib(k - 1) + _fib(k - 2)

    return _fib(n)


@task(retries=1)
def slow_square(n: int, seconds: float = 3.0) -> int:
    """Takes a while. Used by the crash-recovery demo (kill its worker mid-run)."""
    time.sleep(seconds)
    return n * n


@task(retries=5)
def flaky(fail_rate: float = 0.6) -> str:
    """Fails at random, so you can watch retries + exponential backoff."""
    if random.random() < fail_rate:
        raise RuntimeError("random failure")
    return "worked eventually"


@task(retries=2)
def send_email(to: str, subject: str = "Hello") -> str:
    """Pretend to send an email."""
    time.sleep(1)
    return f"sent {subject!r} to {to}"


@task(timeout=2)
def hang(seconds: float = 30) -> str:
    """Sleeps longer than its 2s limit, so it ends as FAILED with a timeout."""
    time.sleep(seconds)
    return "never reached"
