"""The dashboard + REST API (FastAPI).

CONCEPTS: FastAPI + pydantic validation, an *async generator* feeding
Server-Sent Events, ``asyncio.to_thread`` to keep blocking database calls off
the event loop, an application-factory function.

    GET  /                  the dashboard page
    GET  /api/tasks         registered tasks
    POST /api/jobs          enqueue (body: task, args, kwargs, delay, count)
    GET  /api/jobs          list jobs (?status=&limit=)
    GET  /api/jobs/{id}     one job
    GET  /api/stats         counts per status
    GET  /api/stream        live stats + recent jobs every second (SSE)
"""

from __future__ import annotations

import asyncio
import importlib
import json
from collections.abc import AsyncIterator, Iterable
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import HTMLResponse, StreamingResponse
from pydantic import BaseModel, Field

from .backends import Backend
from .config import get_backend
from .job import Status
from .task import Task

DASHBOARD = Path(__file__).parent / "static" / "dashboard.html"


class EnqueueRequest(BaseModel):
    task: str
    args: list[Any] = Field(default_factory=list)
    kwargs: dict[str, Any] = Field(default_factory=dict)
    delay: float = Field(0, ge=0, description="seconds before a worker may start it")
    count: int = Field(1, ge=1, le=200, description="enqueue this many identical jobs")


def create_app(
    backend: Backend | None = None, modules: Iterable[str] = ("miniqueue.examples",)
) -> FastAPI:
    for module in modules:  # make the tasks known so we can validate requests
        importlib.import_module(module)
    store = backend or get_backend()

    app = FastAPI(title="miniqueue", version="0.1.0")

    @app.get("/", response_class=HTMLResponse, include_in_schema=False)
    def index() -> str:
        return DASHBOARD.read_text(encoding="utf-8")

    @app.get("/api/tasks")
    def tasks() -> list[dict[str, Any]]:
        return [
            {"name": t.name, "retries": t.retries, "timeout": t.timeout, "doc": t.__doc__}
            for t in sorted(Task.registry.values(), key=lambda t: t.name)
        ]

    @app.post("/api/jobs")
    def enqueue(req: EnqueueRequest) -> list[dict[str, Any]]:
        task = Task.registry.get(req.task)
        if task is None:
            raise HTTPException(404, f"unknown task {req.task!r}")
        created = []
        for _ in range(req.count):
            job = task.make_job(req.args, req.kwargs, req.delay)
            store.enqueue(job)
            created.append(job.to_dict())
        return created

    @app.get("/api/jobs")
    def list_jobs(status: Status | None = None, limit: int = 50) -> list[dict[str, Any]]:
        return [j.to_dict() for j in store.list_jobs(status=status, limit=min(limit, 500))]

    @app.get("/api/jobs/{job_id}")
    def get_job(job_id: str) -> dict[str, Any]:
        job = store.get(job_id)
        if job is None:
            raise HTTPException(404, "no such job")
        return job.to_dict()

    @app.get("/api/stats")
    def stats() -> dict[str, int]:
        return store.stats()

    def snapshot() -> dict[str, Any]:
        return {
            "stats": store.stats(),
            "jobs": [j.to_dict() for j in store.list_jobs(limit=50)],
        }

    @app.get("/api/stream")
    async def stream(request: Request) -> StreamingResponse:
        async def events() -> AsyncIterator[str]:
            while not await request.is_disconnected():
                # Database calls block, so run them in a thread; the event loop
                # stays free to serve other requests meanwhile.
                data = await asyncio.to_thread(snapshot)
                yield f"data: {json.dumps(jsonable_encoder(data))}\n\n"
                await asyncio.sleep(1)

        return StreamingResponse(
            events(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    return app
