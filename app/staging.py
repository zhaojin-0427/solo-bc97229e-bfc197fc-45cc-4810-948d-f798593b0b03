"""未提交暂存区：流式原件、规范化临时文件、磁盘查重库的生命周期管理。

崩溃安全约定：
- 每次接收在 ``config.staging_dir()`` 下使用唯一的暂存前缀，互不干扰；
- 暂存文件永远不会被正式库引用，启动时整个暂存目录可以无风险清空；
- 已提交文件位于独立的 ``config.storage_dir()``，启动清扫只删除“数据库中
  没有任何 files 行引用”的孤儿文件，已提交文件绝不删除。
"""
from __future__ import annotations

import os
import shutil
import sqlite3
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from . import config


class SqliteDedup:
    """基于磁盘 SQLite 的基站ID 查重后端。

    用 ``WITHOUT ROWID`` 主键表承载全部 ID：去重压力落在磁盘文件上，
    常驻内存只与 SQLite 页缓存有关，不随 ID 总数线性增长。
    """

    def __init__(self, db_path: str) -> None:
        self._db_path = db_path
        self._conn: Optional[sqlite3.Connection] = None

    def prepare(self) -> None:
        """开始一趟全新校验（首次建库，重试时清空旧数据）。"""
        if self._conn is not None:
            self._conn.close()
            self._conn = None
        if os.path.exists(self._db_path):
            os.remove(self._db_path)
        conn = sqlite3.connect(self._db_path, isolation_level=None)
        try:
            conn.execute("PRAGMA journal_mode=MEMORY;")
            conn.execute("PRAGMA synchronous=OFF;")
            conn.execute("PRAGMA temp_store=MEMORY;")
            conn.execute("PRAGMA cache_size=-2000;")  # 约 2 MB 页缓存上限
            conn.execute(
                "CREATE TABLE seen_cells ("
                "cell_id TEXT PRIMARY KEY, first_line INTEGER NOT NULL"
                ") WITHOUT ROWID;"
            )
            # 整份校验在一个事务内完成：批量插入避免逐行提交的 fsync 开销，
            # 校验失败整份丢弃（连接关闭即回滚），天然符合“整份拒收”语义
            conn.execute("BEGIN;")
        except Exception:
            conn.close()
            raise
        self._conn = conn

    def seen_line(self, cell_id: str, line_no: int) -> Optional[int]:
        """已见过则返回首次行号；首次见到则登记 line_no 并返回 None。"""
        assert self._conn is not None, "调用 seen_line 前必须先 prepare()"
        row = self._conn.execute(
            "SELECT first_line FROM seen_cells WHERE cell_id=?", (cell_id,)
        ).fetchone()
        if row is not None:
            return int(row[0])
        self._conn.execute(
            "INSERT INTO seen_cells (cell_id, first_line) VALUES (?, ?)",
            (cell_id, line_no),
        )
        return None

    def commit(self) -> None:
        """校验成功后提交查重事务（调用方在规范化文件完整落盘后执行）。"""
        if self._conn is not None:
            self._conn.execute("COMMIT;")

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def remove(self) -> None:
        self.close()
        if os.path.exists(self._db_path):
            os.remove(self._db_path)


@dataclass
class StagingPaths:
    """一次上传对应的全部暂存路径。"""

    prefix: Path
    raw_path: Path          # 流式接收的原始字节
    normalized_path: Path   # 校验期间逐行写出的规范化 UTF-8 CSV（.tmp）
    dedup_db_path: Path     # 磁盘查重 SQLite

    @classmethod
    def create(cls) -> "StagingPaths":
        config.ensure_dirs()
        prefix = config.staging_dir() / f"upload_{os.getpid()}_{uuid.uuid4().hex}"
        return cls(
            prefix=prefix,
            raw_path=Path(f"{prefix}.raw"),
            normalized_path=Path(f"{prefix}.normalized.tmp"),
            dedup_db_path=Path(f"{prefix}.dedup.db"),
        )

    def cleanup(self) -> None:
        """丢弃本次接收的全部暂存产物（正式文件与正式库不受影响）。"""
        for path in (self.raw_path, self.normalized_path, self.dedup_db_path):
            try:
                path.unlink(missing_ok=True)
            except OSError:
                pass


def fsync_dir(path: Path) -> None:
    """目录 fsync：保证同目录下的 rename/新建在掉电后仍可见。"""
    fd = os.open(str(path), os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def wipe_staging_dir() -> None:
    """启动恢复：清除所有未提交暂存文件（不会触碰已提交目录）。"""
    staging = config.staging_dir()
    staging.mkdir(parents=True, exist_ok=True)
    for entry in staging.iterdir():
        try:
            if entry.is_file() or entry.is_symlink():
                entry.unlink(missing_ok=True)
            elif entry.is_dir():
                # 防御：正常流程只产生文件，目录则递归清掉
                shutil.rmtree(entry, ignore_errors=True)
        except OSError:
            pass


def sweep_orphan_files(conn: sqlite3.Connection) -> list[str]:
    """启动恢复：删除已提交目录里没有任何 files 行引用的孤儿文件。

    数据库引用到的文件一律保留（包括全部历史版本），只清扫“重命名已落盘、
    提交事务未完成”这类窗口留下的未引用文件。返回被清理的路径列表。
    """
    storage = config.storage_dir()
    storage.mkdir(parents=True, exist_ok=True)
    # stored_path 存的是绝对路径，只取文件名与正式目录中的条目按名比对
    referenced = {
        Path(r[0]).name
        for r in conn.execute("SELECT stored_path FROM files WHERE stored_path IS NOT NULL")
    }
    removed: list[str] = []
    for entry in storage.rglob("*"):
        if not entry.is_file():
            continue
        if entry.name in referenced:
            continue
        try:
            entry.unlink(missing_ok=True)
            removed.append(str(entry))
        except OSError:
            pass
    return removed


def check_referenced_files(conn: sqlite3.Connection) -> list[str]:
    """启动体检：返回数据库引用但磁盘缺失的文件路径（出现说明持久化被破坏）。"""
    missing = [
        r[0]
        for r in conn.execute("SELECT stored_path FROM files")
        if not Path(r[0]).exists()
    ]
    return missing
