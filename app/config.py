"""运行期配置。所有可变状态都落在挂载卷里，容器重启不丢数据。

配置项通过函数在调用时读取环境变量，便于测试用例切换数据目录，
也避免模块重载导致的类身份不一致问题。
"""
from __future__ import annotations

import os
from pathlib import Path


def data_dir() -> Path:
    return Path(os.getenv("DATA_DIR", "/data"))


def db_path() -> Path:
    return data_dir() / "warehouse.db"


def storage_dir() -> Path:
    return data_dir() / "files"


def max_upload_bytes() -> int:
    # 单次上传 CSV 的大小上限（默认 100 MB）
    return int(os.getenv("MAX_UPLOAD_BYTES", str(100 * 1024 * 1024)))


def worker_poll_interval() -> float:
    # 后台轮询间隔（秒）
    return float(os.getenv("WORKER_POLL_INTERVAL", "0.15"))


def background_worker_enabled() -> bool:
    # 测试时可关闭后台 worker，由用例自行驱动
    return os.getenv("BACKGROUND_WORKER", "1") != "0"


def ensure_dirs() -> None:
    data_dir().mkdir(parents=True, exist_ok=True)
    storage_dir().mkdir(parents=True, exist_ok=True)
