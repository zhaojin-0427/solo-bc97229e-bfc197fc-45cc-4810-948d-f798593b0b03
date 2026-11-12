"""运行期配置。所有可变状态都落在挂载卷里，容器重启不丢数据。

配置项通过函数在调用时读取环境变量，便于测试用例切换数据目录，
也避免模块重载导致的类身份不一致问题。

目录划分（崩溃安全的基础）：
- ``files/``    已提交的规范化文件，只有数据库 files 行引用的文件才允许长期存在；
- ``staging/``  未提交的暂存文件（流式原件、规范化临时文件、磁盘查重库），
                启动时整体清空，绝不存放正式数据。
"""
from __future__ import annotations

import os
from pathlib import Path


def data_dir() -> Path:
    return Path(os.getenv("DATA_DIR", "/data"))


def db_path() -> Path:
    return data_dir() / "warehouse.db"


def storage_dir() -> Path:
    """已提交的规范化 CSV 目录。"""
    return data_dir() / "files"


def staging_dir() -> Path:
    """未提交暂存目录：接收中的原件、规范化临时文件、磁盘查重库都在这里。"""
    return data_dir() / "staging"


def max_upload_bytes() -> int:
    # 单次上传 CSV 的大小上限（默认 100 MB），流式接收时逐块判定
    return int(os.getenv("MAX_UPLOAD_BYTES", str(100 * 1024 * 1024)))


def upload_chunk_bytes() -> int:
    # 每次从上传流读取的块大小（默认 1 MiB）：常驻内存只与块大小有关，与总行数无关
    return int(os.getenv("UPLOAD_CHUNK_BYTES", str(1024 * 1024)))


def worker_poll_interval() -> float:
    # 后台轮询间隔（秒）
    return float(os.getenv("WORKER_POLL_INTERVAL", "0.15"))


def background_worker_enabled() -> bool:
    # 测试时可关闭后台 worker，由用例自行驱动
    return os.getenv("BACKGROUND_WORKER", "1") != "0"


def ensure_dirs() -> None:
    data_dir().mkdir(parents=True, exist_ok=True)
    storage_dir().mkdir(parents=True, exist_ok=True)
    staging_dir().mkdir(parents=True, exist_ok=True)
