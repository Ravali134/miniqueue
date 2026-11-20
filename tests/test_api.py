import pytest
from fastapi.testclient import TestClient

from miniqueue import MemoryBackend, task
from miniqueue.api import create_app


@task
def api_add(a, b):
    return a + b


@pytest.fixture
def client():
    backend = MemoryBackend()
    app = create_app(backend=backend, modules=())
    return TestClient(app), backend


def test_dashboard_page_is_served(client):
    c, _ = client
    r = c.get("/")
    assert r.status_code == 200 and "miniqueue" in r.text


def test_enqueue_list_get_and_stats(client):
    c, backend = client
    r = c.post("/api/jobs", json={"task": "api_add", "args": [1, 2], "count": 3})
    assert r.status_code == 200 and len(r.json()) == 3
    job_id = r.json()[0]["id"]

    assert c.get("/api/stats").json()["pending"] == 3
    assert len(c.get("/api/jobs").json()) == 3
    assert len(c.get("/api/jobs?status=running").json()) == 0
    assert c.get(f"/api/jobs/{job_id}").json()["task_name"] == "api_add"
    assert backend.stats()["total"] == 3


def test_errors(client):
    c, _ = client
    assert c.post("/api/jobs", json={"task": "nope"}).status_code == 404
    assert c.get("/api/jobs/missing").status_code == 404
    assert c.post("/api/jobs", json={"task": "api_add", "count": 0}).status_code == 422


def test_task_listing(client):
    c, _ = client
    assert "api_add" in [t["name"] for t in c.get("/api/tasks").json()]
