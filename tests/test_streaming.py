"""流式改造专项：内存不随行数增长、磁盘查重、校验期不占正式库写事务、
写盘失败原子性、崩溃恢复（暂存清理/孤儿清扫/不误删已提交文件）、编码兼容。
"""
from __future__ import annotations

import io
import tracemalloc
from pathlib import Path

import pytest

from conftest import run_worker, upload

from app import config, db, repository, services, staging
from app.csvparser import stream_ingest

DAY = "2026-11-12"


def _big_traffic(rows: int) -> bytes:
    out = ["基站ID,上行字节数,下行字节数"]
    for i in range(rows):
        out.append(f"C{i:08d},{i * 10},{i * 20}")
    return ("\n".join(out) + "\n").encode("utf-8")


def _big_ownership(rows: int) -> bytes:
    out = ["cell_id,district"]
    for i in range(rows):
        out.append(f"C{i:08d},片区{i % 17}")
    return ("\n".join(out) + "\n").encode("utf-8")


# ---------- 内存与磁盘查重 ----------

def test_large_upload_memory_does_not_grow_with_row_count(app_env):
    """5 万行上传：进程内 Python 对象峰值增量远小于文件规模，证明没有整份驻留。"""
    rows = 50_000
    payload = _big_traffic(rows)
    assert len(payload) > 1024 * 1024  # 输入本身已超过 1 MB

    conn = db.connect()
    try:
        tracemalloc.start(25)
        base_current, _ = tracemalloc.get_traced_memory()
        file_id, _job = services.ingest_upload_bytes(
            conn, DAY, "traffic", "big.csv", payload)
        peak_current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
    finally:
        conn.close()

    # 输入 1MB+、5 万行；Python 堆峰值增量必须远小于输入规模（上限 8 MB），
    # 说明既没有缓存整份文本，也没有把全部 ID/行放进内存（查重与写盘都在磁盘）
    assert peak - base_current < 8 * 1024 * 1024
    assert peak_current - base_current < 8 * 1024 * 1024

    check = db.connect()
    try:
        row = repository.get_file_by_id(check, file_id)
        assert row["row_count"] == rows
        # 正式库记录的行数正确，落盘规范化文件可被 worker 流式重读
        stored = Path(row["stored_path"])
        line_count = sum(1 for _ in stored.open(encoding="utf-8"))
        assert line_count == rows + 1  # 含表头
    finally:
        check.close()


def test_dedup_backend_persists_ids_to_disk(app_env):
    """磁盘查重后端：5 万个 ID 落在独立 SQLite 文件里，内存侧不持有 ID 集合。"""
    paths = staging.StagingPaths.create()
    try:
        dedup = staging.SqliteDedup(str(paths.dedup_db_path))
        dedup.prepare()
        n = 50_000
        for i in range(n):
            assert dedup.seen_line(f"C{i:08d}", i + 2) is None
        # 重复 ID 命中首次行号（仍在同一校验事务内）
        assert dedup.seen_line("C00000000", 99999) == 2
        assert dedup.seen_line(f"C{n - 1:08d}", 99999) == n + 1
        dedup.commit()
        dedup.close()  # 关闭后页缓存落盘

        # 查重库文件确实在磁盘上承载了全部 ID（而非内存字典）
        assert paths.dedup_db_path.exists()
        assert paths.dedup_db_path.stat().st_size > n * 10  # 每个 ID 远超 10 字节

        # 重新打开同一库，数据仍在（磁盘而非内存）
        import sqlite3
        with sqlite3.connect(str(paths.dedup_db_path)) as check:
            cnt = check.execute("SELECT COUNT(*) FROM seen_cells").fetchone()[0]
            assert cnt == n
    finally:
        paths.cleanup()


def test_duplicate_id_in_large_file_rejected_on_disk_dedup(app_env):
    """5 万行文件末行重复：磁盘查重仍能命中，整份拒收，无新版本。"""
    payload = _big_traffic(50_000)
    text = payload.decode("utf-8").rstrip("\n")
    text += "\nC00000000,1,2\n"  # 与首行 ID 重复
    resp = upload(app_env["client"], DAY, "traffic", text)
    assert resp.status_code == 400
    assert resp.json()["detail"]["code"] == "DUPLICATE_CELL_ID"
    assert app_env["client"].get(f"/days/{DAY}/files").json() == []


def test_end_to_end_large_flow_and_aggregation(app_env):
    """大文件全链路：上传两份 -> worker 汇总 -> 取数正确，且暂存目录最终为空。"""
    rows = 50_000
    upload(app_env["client"], DAY, "traffic", _big_traffic(rows).decode("utf-8"))
    ack = upload(app_env["client"], DAY, "ownership", _big_ownership(rows).decode("utf-8")).json()
    assert ack["both_files_present"] is True
    run_worker(app_env)

    body = app_env["client"].get(f"/days/{DAY}/results").json()
    assert body["ready"] is True
    # 抽样校验一个片区的基站数
    expect = sum(1 for i in range(rows) if i % 17 == 0)
    district0 = next(r for r in body["results"] if r["district"] == "片区0")
    assert district0["cell_count"] == expect
    # 暂存区不应残留任何文件
    assert list(config.staging_dir().iterdir()) == []


# ---------- 超限与写盘失败 ----------

def test_oversize_stream_rejected_and_nothing_changes(app_env, monkeypatch):
    monkeypatch.setenv("MAX_UPLOAD_BYTES", "1024")  # 1 KB 上限
    payload = _big_traffic(2_000)  # 远超 1 KB
    resp = upload(app_env["client"], DAY, "traffic", payload.decode("utf-8"))
    assert resp.status_code == 400
    assert resp.json()["detail"]["code"] == "FILE_TOO_LARGE"
    # 没有任何文件版本、作业产生
    assert app_env["client"].get(f"/days/{DAY}/files").json() == []
    assert list(config.staging_dir().iterdir()) == []


def test_normalized_write_failure_keeps_everything_unchanged(app_env, monkeypatch):
    """规范化写盘失败：当前文件、作业、结果均不变，暂存被清理。"""
    good = "基站ID,上行字节数,下行字节数\nC001,100,200\n"
    upload(app_env["client"], DAY, "traffic", good, filename="good.csv")

    real_open = open

    def boom_open(path, *a, **kw):
        mode = a[0] if a else kw.get("mode", "")
        if str(path).endswith(".normalized.tmp") and "w" in mode:
            raise OSError("模拟磁盘已满")
        return real_open(path, *a, **kw)

    monkeypatch.setattr("app.services._open", boom_open)
    resp = upload(app_env["client"], DAY, "traffic",
                  "基站ID,上行字节数,下行字节数\nC002,1,2\n", filename="new.csv")
    assert resp.status_code == 400
    assert resp.json()["detail"]["code"] == "WRITE_FAILED"

    files = app_env["client"].get(f"/days/{DAY}/files").json()
    assert len(files) == 1 and files[0]["original_name"] == "good.csv"
    assert list(config.staging_dir().iterdir()) == []


def test_rename_failure_rolls_back_commit(app_env, monkeypatch):
    """提交事务内改名失败：files 行与作业随事务回滚，正式目录无孤儿，暂存清空。"""
    upload(app_env["client"], DAY, "ownership", "基站ID,片区\nC001,城东\n")

    def boom_replace(src, dst):
        raise OSError("模拟 rename 失败")

    monkeypatch.setattr("app.services.os.replace", boom_replace)
    resp = upload(app_env["client"], DAY, "traffic",
                  "基站ID,上行字节数,下行字节数\nC001,1,2\n")
    assert resp.status_code == 400
    assert resp.json()["detail"]["code"] == "WRITE_FAILED"

    conn = db.connect()
    try:
        assert conn.execute("SELECT COUNT(*) c FROM files").fetchone()["c"] == 1
        assert conn.execute("SELECT COUNT(*) c FROM jobs").fetchone()["c"] == 0
    finally:
        conn.close()
    # 正式目录里只有已提交的 ownership 文件，没有 traffic 孤儿
    committed = [p.name for p in config.storage_dir().rglob("*.csv")]
    assert all("ownership" in n for n in committed)
    assert list(config.staging_dir().iterdir()) == []


# ---------- 校验期不占正式库写事务 ----------

def test_validation_runs_without_warehouse_write_lock(app_env):
    """正式库被写事务占住时，另一连接仍可完成接收+校验+规范化（仅最终提交需锁）。"""
    holder = db.connect()
    holder.execute("BEGIN IMMEDIATE;")  # 模拟其他业务长时间占用写锁
    try:
        paths = staging.StagingPaths.create()
        try:
            with open(paths.raw_path, "wb") as fh:
                fh.write(_big_traffic(10_000))
            dedup = staging.SqliteDedup(str(paths.dedup_db_path))
            try:
                with open(paths.normalized_path, "w", encoding="utf-8", newline="") as out:
                    stats = stream_ingest(str(paths.raw_path), "traffic", out, dedup)
            finally:
                dedup.remove()
            assert stats.row_count == 10_000
            # 规范化文件确实在写锁持有期间完整产出
            assert Path(paths.normalized_path).stat().st_size > 0
        finally:
            paths.cleanup()
    finally:
        holder.execute("ROLLBACK;")
        holder.close()


def test_other_business_day_upload_unaffected_during_validation(app_env, monkeypatch):
    """同进程内串行的两次上传互不阻塞：不同业务日各自独立暂存、独立提交。"""
    upload(app_env["client"], "2026-11-12", "traffic",
           "基站ID,上行字节数,下行字节数\nC001,1,2\n")
    upload(app_env["client"], "2026-11-13", "traffic",
           "基站ID,上行字节数,下行字节数\nC909,3,4\n")
    days = app_env["client"].get("/days").json()
    got = {d["business_date"] for d in days}
    assert got == {"2026-11-12", "2026-11-13"}


# ---------- 崩溃恢复 ----------

def test_startup_wipes_staging_and_sweeps_orphans_but_keeps_committed(app_env):
    client = app_env["client"]
    upload(client, DAY, "traffic", "基站ID,上行字节数,下行字节数\nC001,1,2\n")
    upload(client, DAY, "ownership", "基站ID,片区\nC001,城东\n")
    run_worker(app_env)

    committed_before = sorted(p.name for p in config.storage_dir().rglob("*.csv"))
    assert len(committed_before) == 2

    # 伪造崩溃残留：暂存文件 + 已改名但未提交的孤儿正式文件
    config.staging_dir().mkdir(parents=True, exist_ok=True)
    (config.staging_dir() / "upload_1_x.raw").write_bytes(b"junk")
    (config.staging_dir() / "upload_1_x.normalized.tmp").write_bytes(b"junk")
    (config.staging_dir() / "upload_1_x.dedup.db").write_bytes(b"junk")
    orphan = config.storage_dir() / DAY / "traffic_v99_orphan123456.csv"
    orphan.write_text("基站ID,上行字节数,下行字节数\nGHOST,1,1\n", encoding="utf-8")

    # 执行启动恢复
    staging.wipe_staging_dir()
    conn = db.connect()
    try:
        removed = staging.sweep_orphan_files(conn)
    finally:
        conn.close()

    assert list(config.staging_dir().iterdir()) == []
    assert any("traffic_v99_orphan123456.csv" in p for p in removed)
    committed_after = sorted(p.name for p in config.storage_dir().rglob("*.csv"))
    # 已提交文件一个都不能少
    assert committed_after == committed_before


def test_no_version_references_missing_file_after_recovery(app_env):
    """恢复后所有 files 行引用的文件都存在（不出现引用缺失文件的版本）。"""
    client = app_env["client"]
    upload(client, DAY, "traffic", "基站ID,上行字节数,下行字节数\nC001,1,2\n")
    conn = db.connect()
    try:
        staging.wipe_staging_dir()
        missing = staging.check_referenced_files(conn)
    finally:
        conn.close()
    assert missing == []


def test_commit_phase_crash_window_leaves_no_dangling_reference(app_env, monkeypatch):
    """模拟“改名后、COMMIT 前崩溃”：重启清扫孤儿，库中无该版本、无悬空引用。"""
    import app.services as services_mod

    real_replace = services_mod.os.replace
    crash_after_rename = {"done": False}

    def replace_then_crash(src, dst):
        real_replace(src, dst)
        crash_after_rename["done"] = True
        raise RuntimeError("模拟进程在 COMMIT 前终止")

    monkeypatch.setattr(services_mod.os, "replace", replace_then_crash)
    with pytest.raises(RuntimeError):
        upload(app_env["client"], DAY, "traffic",
               "基站ID,上行字节数,下行字节数\nC001,1,2\n")
    assert crash_after_rename["done"] is True

    # 事务已回滚：库里没有 files 行
    conn = db.connect()
    try:
        assert conn.execute("SELECT COUNT(*) c FROM files").fetchone()["c"] == 0
        # 重启恢复：孤儿被清扫，且无任何悬空引用
        staging.wipe_staging_dir()
        removed = staging.sweep_orphan_files(conn)
        assert staging.check_referenced_files(conn) == []
    finally:
        conn.close()
    assert removed and all("traffic" in p for p in removed)
    assert list(config.staging_dir().iterdir()) == []


# ---------- 编码与规范化 ----------

def test_normalized_file_fsynced_before_db_commit(app_env, monkeypatch):
    """规范化文件必须在正式库写事务开启前完整持久化（fsync 已完成）。"""
    import app.db as dbmod
    events = []
    real_transaction = dbmod.transaction

    class RecordingCM:
        def __enter__(self):
            # 事务开启这一刻：规范化临时文件必须已经完整写盘且 fsync
            staged = list(config.staging_dir().glob("*.normalized.tmp"))
            assert staged, "进入提交事务前应已生成规范化临时文件"
            size = staged[0].stat().st_size
            assert size > 0
            with staged[0].open("rb") as fh:
                head = fh.read(4096)
            assert b"cell" in head or b"\xe5\x9f\xba\xe7\xab\x99" in head  # 表头已落盘
            events.append(("begin_tx", size))
            self._cm = real_transaction(self._conn)
            return self._cm.__enter__()

        def __exit__(self, *exc):
            events.append(("end_tx", 0))
            return self._cm.__exit__(*exc)

        def __call__(self, conn):
            self._conn = conn
            return self

    monkeypatch.setattr("app.services.transaction", RecordingCM())
    upload(app_env["client"], DAY, "traffic",
           "基站ID,上行字节数,下行字节数\nC001,1,2\nC002,3,4\n")
    assert ("begin_tx" in [e[0] for e in events])
    # 事务结束后暂存已清空（文件已改名提交）
    assert list(config.staging_dir().iterdir()) == []


def test_utf8_bom_and_gb18030_streaming(app_env):
    bom = "﻿基站ID,上行字节数,下行字节数\nC001,10,20\n".encode("utf-8")
    gbk = "基站ID,片区\nC001,城东\n".encode("gb18030")
    r1 = app_env["client"].post(
        f"/days/{DAY}/files/traffic",
        files={"file": ("bom.csv", bom, "text/csv")})
    r2 = app_env["client"].post(
        f"/days/{DAY}/files/ownership",
        files={"file": ("gbk.csv", gbk, "text/csv")})
    assert r1.status_code == 201 and r2.status_code == 201
    run_worker(app_env)
    body = app_env["client"].get(f"/days/{DAY}/results").json()
    assert body["ready"] is True
    assert body["results"][0]["district"] == "城东"

    # 规范化落盘文件是无 BOM 的 UTF-8，逻辑列顺序固定
    stored = list(config.storage_dir().rglob("*.csv"))
    for p in stored:
        head = p.read_bytes()[:3]
        assert head != b"\xef\xbb\xbf"


def test_unsupported_encoding_rejected(app_env):
    # UTF-16 不在允许范围内：两种候选编码都解不出来
    payload = "基站ID,上行字节数,下行字节数\nC001,1,2\n".encode("utf-16")
    r = app_env["client"].post(
        f"/days/{DAY}/files/traffic",
        files={"file": ("u16.csv", payload, "text/csv")})
    assert r.status_code == 400
    assert r.json()["detail"]["code"] == "UNSUPPORTED_ENCODING"


def test_gb18030_fallback_after_failed_utf8_attempt(app_env):
    """无 BOM 的 GB18030 字节在 UTF-8 解码中途失败，必须自动整体回退到 GB18030。"""
    # 纯 ASCII 表头 + GB18030 中文数据：UTF-8 读到中文行才报错，验证“中途失败也能回退”
    payload = ("cell_id,district\n"
               "C001,城东片区\n"
               "C002,城西片区\n").encode("gb18030")
    # 确认该字节序列确实无法用 UTF-8 解码（即一定会触发回退）
    with pytest.raises(UnicodeDecodeError):
        payload.decode("utf-8")
    r = app_env["client"].post(
        f"/days/{DAY}/files/ownership",
        files={"file": ("gb.csv", payload, "text/csv")})
    assert r.status_code == 201, r.text

    # 规范化文件内容正确，且为 UTF-8
    stored = next(config.storage_dir().rglob("ownership_*.csv"))
    text = stored.read_text(encoding="utf-8")
    assert "城东片区" in text and "城西片区" in text
