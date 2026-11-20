"""Command line: ``miniqueue up | worker | dashboard | demo``."""

from __future__ import annotations

import argparse
import logging
import os
import threading
import time
import webbrowser

from .config import DEFAULT_URL, configure
from .worker import WorkerPool

DEFAULT_TASKS = ["miniqueue.examples"]


def _common(p: argparse.ArgumentParser) -> None:
    p.add_argument(
        "--url",
        default=os.environ.get("MINIQUEUE_URL", DEFAULT_URL),
        help=f"backend URL (default: {DEFAULT_URL})",
    )
    p.add_argument(
        "--tasks",
        action="append",
        help="module(s) containing your @task functions (default: miniqueue.examples)",
    )


def _serve(url: str, tasks: list[str], host: str, port: int) -> None:
    import uvicorn

    from .api import create_app

    configure(url)
    app = create_app(modules=tasks)
    uvicorn.run(app, host=host, port=port, log_level="warning", timeout_graceful_shutdown=2)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="miniqueue", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    up = sub.add_parser("up", help="start workers AND the dashboard (opens your browser)")
    _common(up)
    up.add_argument("-c", "--concurrency", type=int, default=3, help="worker processes")
    up.add_argument("--host", default="127.0.0.1")
    up.add_argument("--port", type=int, default=8000)
    up.add_argument("--no-browser", action="store_true")

    w = sub.add_parser("worker", help="start only worker processes")
    _common(w)
    w.add_argument("-c", "--concurrency", type=int, default=3, help="worker processes")

    d = sub.add_parser("dashboard", help="start only the dashboard/API")
    _common(d)
    d.add_argument("--host", default="127.0.0.1")
    d.add_argument("--port", type=int, default=8000)

    demo = sub.add_parser("demo", help="run a self-contained demo in the terminal")
    demo.add_argument("--workers", type=int, default=3)
    demo.add_argument("--crash", action="store_true", help="kill a worker mid-job to show recovery")

    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S")

    if args.command == "demo":
        from .demo import run_demo

        run_demo(workers=args.workers, crash=args.crash)
        return 0

    tasks = args.tasks or DEFAULT_TASKS

    if args.command == "dashboard":
        print(f"Dashboard: http://{args.host}:{args.port}")
        _serve(args.url, tasks, args.host, args.port)
        return 0

    if args.command == "worker":
        print(f"{args.concurrency} worker(s) running. Ctrl-C to stop.")
        with WorkerPool(args.url, modules=tasks, size=args.concurrency) as pool:
            try:
                while any(p.is_alive() for p in pool.processes):
                    time.sleep(0.5)
            except KeyboardInterrupt:
                print("\nStopping workers...")
        return 0

    # up
    url = f"http://{args.host}:{args.port}"
    print(f"miniqueue is up: {url}   ({args.concurrency} workers, Ctrl-C to stop)")
    if not args.no_browser:
        threading.Timer(1.0, webbrowser.open, [url]).start()
    with WorkerPool(args.url, modules=tasks, size=args.concurrency):
        try:
            _serve(args.url, tasks, args.host, args.port)
        except KeyboardInterrupt:
            pass
    print("Stopped.")
    return 0
