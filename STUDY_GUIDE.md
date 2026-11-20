# Study guide

## 1. Reading order (about 90 minutes)

Read in this order. Each file builds on the previous one.

1. **`job.py`** (10 min): the data model and the state machine. Everything else moves `Job`s around.
2. **`task.py`** (15 min): what `@task` really returns (a `Task` *object*, not a function) and what `.delay()` does.
3. **`backends/base.py`**, then **`memory.py`** (15 min): the contract, then its simplest implementation.
4. **`backends/sqlite.py`** (15 min): the same contract with real concurrency. Focus on `_tx` and `claim`.
5. **`worker.py`** (25 min): the polling generator, `execute`, `time_limit`, then the process pool.
6. **`api.py`** + `static/dashboard.html` (10 min): how the dashboard gets live data.
7. **`tests/`**: read `test_backends.py` and `test_worker.py`; tests are the best documentation.

Then **run it and break it**: `miniqueue demo --crash`, then `miniqueue up` and send a burst.
Change `retries`, `timeout` and `backoff_base` and watch what changes.

## 2. Key ideas in one paragraph each

**`@task` returns an object.** `Task` stores the function and defines `__call__`, so `add(1, 2)` still
works. `update_wrapper` copies `__name__` and `__doc__` so introspection and tracebacks look normal.
`task` supports `@task` and `@task(retries=3)` because it checks whether it received the function
(`fn is not None`) or only keyword options (then it returns the real decorator).

**Registry pattern, twice.** `Task.registry` maps a name to a task, because a job on disk can only store the
*name*. `Backend.__init_subclass__` registers backend classes by `scheme=`, so
`Backend.from_url("sqlite:///x.db")` can find the class without a big `if/elif`.

**Why a claim must be atomic.** If two workers `SELECT` the same pending row before either `UPDATE`s it, both run
the job. `BEGIN IMMEDIATE` takes SQLite's write lock before the `SELECT`, so the second worker waits
until the first has committed. `test_concurrent_claims_never_hand_out_the_same_job` proves it with 6 threads.

**Processes, not threads.** The GIL lets only one thread run Python bytecode at a time, so CPU-bound tasks (like
`fib(30)`) gain nothing from threads. Each worker process has its own interpreter and GIL. We use the
`spawn` start method (the macOS default): each worker starts fresh and re-imports the task modules,
which is why `_worker_main` is a top-level function (it must be picklable).

**Retries and backoff.** On failure, if `attempts <= max_retries` the job goes back to `PENDING` with
`run_at = now + base * 2**(attempts-1)` (capped). Backoff stops us hammering a service that is
already failing.

**Crash recovery (visibility timeout).** A worker claims a job and `kill -9`s itself. Nothing tells the queue.
Every worker periodically calls `requeue_stale`: `RUNNING` jobs older than `visibility_timeout` are
rescued. This gives **at-least-once delivery**. If a job has no retries left, it is `FAILED`
instead, so a "poison pill" job can't crash worker after worker forever.

**Timeouts.** `time_limit` is a context manager that arms `signal.setitimer(ITIMER_REAL, n)`. When the
timer fires, `SIGALRM`'s handler raises `TaskTimeout` *inside* the running task. The `finally` block
always disarms the timer and restores the old handler.

**Generators.** `Worker.jobs()` is an infinite generator, so the loop reads `for job in worker.jobs()`.
`Backend.stream()` pages through a huge table lazily with `yield from`, using constant memory.

**Async dashboard.** `/api/stream` is an `async` generator producing Server-Sent Events. The database calls
are blocking, so they run in `asyncio.to_thread`; otherwise one slow query would freeze every other request.

## 3. Likely viva questions (with short answers)

1. **How is this different from just using threads or `multiprocessing.Pool`?**
   A pool lives and dies with one program. Here jobs are *persisted* in a database, so producers and workers
   are separate programs, jobs survive restarts, and workers can be added or killed at any time.
2. **What is the GIL and how do you work around it?** Only one thread executes Python bytecode at a time.
   For CPU-bound work we use separate processes, each with its own GIL.
3. **What happens if a worker crashes mid-job?** See "Crash recovery" above. Demo it: `miniqueue demo --crash`.
4. **What delivery guarantee do you provide?** At-least-once. Exactly-once is impossible in general, so
   tasks should be idempotent.
5. **How do you prevent two workers from taking the same job?** An atomic claim (`BEGIN IMMEDIATE`).
6. **Why `spawn` instead of `fork`?** `fork` copies the parent's memory and locks, which can deadlock with
   threads and open DB connections. `spawn` is safe and portable (it is the default on macOS and Windows).
7. **How would you scale this up?** Add workers (`-c 8`, or more machines on a shared DB). Swap SQLite for
   Redis/Postgres by writing one `Backend` subclass.
8. **What are the limitations of `SIGALRM` timeouts?** Unix-only, main thread only, and it cannot interrupt a
   C call that never returns to the interpreter. The fix is to run tasks in a killable subprocess.
9. **Explain `__init_subclass__`.** It is a hook called on the *parent* class each time a subclass is defined.
   It can replace a metaclass for simple registration.
10. **Why do `Job.mark_*` methods exist instead of backends mutating fields?** One place for the rules, so
    every backend behaves identically and the state machine is unit-testable without a database.
11. **What does `ParamSpec` buy you?** `add.delay("x")` is flagged by a type checker if `add` takes numbers.
    `delay` has exactly the wrapped function's parameter types.
12. **Why one SQLite connection per operation?** Simple, and safe across threads and processes. WAL mode
    lets readers (the dashboard) work while workers write.

## 4. Exercises to deepen understanding (do 2 or 3 before the viva)

- Add a `priority` field so higher-priority jobs are claimed first (change `ORDER BY` in `claim`).
- Add a `RedisBackend` (`pip install redis`): claim with a Lua script for atomicity.
- Add a `Job.wait(timeout)` that polls until the job is terminal and returns the result.
- Add a worker heartbeat so a legitimately long job isn't treated as dead.
- Write a periodic-task scheduler (`@task(every=60)`).
- Replace `SIGALRM` timeouts with a killable child process, and compare.
