"""pytest 夹具：每个用例独立临时数据目录，关闭后台线程由用例手动驱动 worker。"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

# 首次导入前给一个默认数据目录，具体用例再通过 monkeypatch 切换
import os  # noqa: E402
os.environ.setdefault("DATA_DIR", "/tmp/cell-warehouse-tests-default")
os.environ.setdefault("BACKGROUND_WORKER", "0")

from app import config, db  # noqa: E402
from app.main import app  # noqa: E402
from app.worker import JobWorker  # noqa: E402


@pytest.fixture()
def app_env(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("BACKGROUND_WORKER", "0")
    monkeypatch.setenv("WORKER_POLL_INTERVAL", "0.01")

    db.init_db()
    with TestClient(app) as client:
        yield {
            "client": client,
            "config": config,
            "db": db,
            "worker": JobWorker(),
            "data_dir": config.data_dir(),
        }


def run_worker(env, max_jobs: int = 10):
    """同步驱动 worker 处理队列，返回处理的作业数。"""
    conn = env["db"].connect()
    processed = 0
    try:
        for _ in range(max_jobs):
            if not env["worker"].run_pending_once(conn):
                break
            processed += 1
    finally:
        conn.close()
    return processed


def upload(client, day: str, file_type: str, csv_text: str, filename: str = "f.csv"):
    return client.post(
        f"/days/{day}/files/{file_type}",
        files={"file": (filename, csv_text.encode("utf-8"), "text/csv")},
    )
