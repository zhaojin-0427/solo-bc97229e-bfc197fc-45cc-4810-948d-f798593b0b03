"""真实后台线程集成：启动 worker 后作业自动完成（不手动驱动）。"""
from __future__ import annotations

import time

import pytest
from fastapi.testclient import TestClient

from conftest import upload


@pytest.fixture()
def live_app(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("BACKGROUND_WORKER", "1")
    monkeypatch.setenv("WORKER_POLL_INTERVAL", "0.05")

    from app import config, db
    from app.main import app
    from app.worker import worker as global_worker

    db.init_db()
    with TestClient(app) as client:  # lifespan 会自动启动 worker 线程
        yield client, config
        global_worker.stop()


def _poll_ready(client, day: str, timeout: float = 5.0) -> dict:
    deadline = time.time() + timeout
    last = {}
    while time.time() < deadline:
        last = client.get(f"/days/{day}/results").json()
        if last["ready"] or last["status"] == "failed":
            return last
        time.sleep(0.05)
    raise AssertionError(f"作业在 {timeout}s 内未完成：{last}")


def test_background_worker_computes_automatically(live_app):
    client, _config = live_app
    day = "2026-11-12"
    traffic = "基站ID,上行字节数,下行字节数\nC001,100,200\nC002,300,400\n"
    ownership = "基站ID,片区\nC001,城东\nC002,城东\n"
    upload(client, day, "traffic", traffic)
    upload(client, day, "ownership", ownership)

    body = _poll_ready(client, day)
    assert body["ready"] is True
    assert body["results"][0]["district"] == "城东"
    assert body["results"][0]["total_bytes"] == 1000

    # 替换后自动重算，无需任何手动触发
    traffic_v2 = "基站ID,上行字节数,下行字节数\nC001,1,2\nC002,3,4\n"
    upload(client, day, "traffic", traffic_v2)
    body2 = _poll_ready(client, day)
    assert body2["ready"] is True
    assert body2["results"][0]["total_bytes"] == 10
    assert body2["job_id"] != body["job_id"]
