"""可靠性：陈旧作业不发布、重启接续、计算期间可查询、多业务日隔离、编码兼容。"""
from __future__ import annotations

import sqlite3

from conftest import run_worker, upload

from app import repository

DAY = "2026-11-12"

TRAFFIC = """基站ID,上行字节数,下行字节数
C001,100,200
C002,300,400
"""
OWNERSHIP = """基站ID,片区
C001,城东
C002,城东
"""
TRAFFIC_V2 = """基站ID,上行字节数,下行字节数
C001,1000,2000
C002,3000,4000
"""


def test_stale_running_job_never_publishes(app_env):
    """作业计算期间文件被替换：旧作业即使走到发布也必须丢弃结果。"""
    client = app_env["client"]
    upload(client, DAY, "traffic", TRAFFIC)
    ack = upload(client, DAY, "ownership", OWNERSHIP).json()
    old_job_id = ack["job"]["job_id"]

    conn = app_env["db"].connect()
    try:
        # 模拟 worker 已原子认领、正在 running
        claimed = repository.claim_next_job(conn)
        assert claimed["id"] == old_job_id
        assert claimed["status"] == "running"

        # 此时上传替换文件：在途旧作业被作废，新作业排队
        ack2 = upload(client, DAY, "traffic", TRAFFIC_V2).json()
        new_job_id = ack2["job"]["job_id"]
        assert new_job_id != old_job_id

        # worker 用独立连接推进度：新读快照能看到已提交的取代状态并中止
        # 注意：JobFailure 必须在此处延迟导入，与运行时模块保持同一个类对象
        from app.errors import JobFailure
        worker_conn = app_env["db"].connect()
        try:
            try:
                repository.update_progress(worker_conn, old_job_id, 50, "汇总中")
                raise AssertionError("应当抛出 SUPERSEDED")
            except JobFailure as exc:
                assert exc.code == "SUPERSEDED"
        finally:
            worker_conn.close()

        # 旧作业状态已是 superseded，且没有任何结果
        old = repository.get_job(conn, old_job_id)
        assert old["status"] == "superseded"
        assert repository.get_results(conn, old_job_id) == []
    finally:
        conn.close()

    # 新作业正常完成并对外可见
    run_worker(app_env)
    body = client.get(f"/days/{DAY}/results").json()
    assert body["ready"] is True
    assert body["job_id"] == new_job_id
    assert body["results"][0]["uplink_bytes"] == 4000


def test_complete_job_rejects_publish_after_supersede(app_env):
    """发布瞬间被取代：complete_job 返回 False，结果不落库。"""
    client = app_env["client"]
    upload(client, DAY, "traffic", TRAFFIC)
    ack = upload(client, DAY, "ownership", OWNERSHIP).json()
    job_id = ack["job"]["job_id"]

    conn = app_env["db"].connect()
    try:
        repository.claim_next_job(conn)
        # 直接把作业手工置为 superseded（模拟替换发生在最后一刻）
        conn.execute(
            "UPDATE jobs SET status='superseded' WHERE id=?", (job_id,))
        published = repository.complete_job(
            conn, job_id, [("城东", 2, 400, 600, 1000)])
        assert published is False
        assert repository.get_results(conn, job_id) == []
    finally:
        conn.close()


def test_restart_resumes_interrupted_job(app_env):
    """模拟进程崩溃：running 作业在重启后重新排队并完成，数据不丢。"""
    client = app_env["client"]
    upload(client, DAY, "traffic", TRAFFIC)
    upload(client, DAY, "ownership", OWNERSHIP)

    conn = app_env["db"].connect()
    try:
        claimed = repository.claim_next_job(conn)
        job_id = claimed["id"]
        assert repository.get_job(conn, job_id)["status"] == "running"
    finally:
        conn.close()

    # “重启”：新连接执行启动恢复逻辑
    conn2 = app_env["db"].connect()
    try:
        reset = repository.reset_interrupted_jobs(conn2)
        assert reset == 1
        row = repository.get_job(conn2, job_id)
        assert row["status"] == "queued"
        assert row["progress_percent"] == 0
    finally:
        conn2.close()

    # 输入文件与作业记录均持久，恢复后接续完成
    run_worker(app_env)
    body = client.get(f"/days/{DAY}/results").json()
    assert body["ready"] is True
    assert body["job_id"] == job_id
    assert body["results"][0]["total_bytes"] == 1000

    # SQLite 文件确实落在数据卷目录
    assert app_env["config"].db_path().exists()


def test_queries_keep_working_during_computation(app_env):
    """计算期间上传与查询都不阻塞（结果未就绪明确告知 computing）。"""
    client = app_env["client"]
    upload(client, DAY, "traffic", TRAFFIC)
    upload(client, DAY, "ownership", OWNERSHIP)

    conn = app_env["db"].connect()
    try:
        repository.claim_next_job(conn)  # 作业停在 running
        # 计算期间反复查询，始终得到明确的 computing 状态而非错误或旧数据
        for _ in range(3):
            body = client.get(f"/days/{DAY}/results").json()
            assert body["ready"] is False
            assert body["status"] == "computing"
        # 计算期间仍可上传另一个业务日的数据
        other = upload(client, "2026-11-13", "traffic", TRAFFIC)
        assert other.status_code == 201
    finally:
        conn.close()


def test_multiple_business_days_are_isolated(app_env):
    client = app_env["client"]
    upload(client, "2026-11-12", "traffic", TRAFFIC)
    upload(client, "2026-11-12", "ownership", OWNERSHIP)
    run_worker(app_env)

    # 另一个业务日只上传了流量文件
    upload(client, "2026-11-13", "traffic", TRAFFIC_V2)
    run_worker(app_env)

    day1 = client.get("/days/2026-11-12/results").json()
    day2 = client.get("/days/2026-11-13/results").json()
    assert day1["ready"] is True
    assert day2["ready"] is False and day2["status"] == "waiting_files"

    jobs = client.get("/jobs").json()
    days = {j["business_date"] for j in jobs}
    assert days == {"2026-11-12"}


def test_only_latest_generation_of_same_pair_is_current(app_env):
    """同一文件对手工触发多代作业时，仅最新一代 current=true。"""
    client = app_env["client"]
    upload(client, DAY, "traffic", TRAFFIC)
    upload(client, DAY, "ownership", OWNERSHIP)
    run_worker(app_env)

    conn = app_env["db"].connect()
    try:
        job = repository.get_latest_job_for_pair(conn, DAY, 1, 2)
        # 模拟同文件对再入队一代（实际场景中由重新触发产生）
        new_id = repository.enqueue_job(conn, DAY, 1, 2)
        run_worker(app_env)
    finally:
        conn.close()

    old = client.get(f"/jobs/{job['id']}").json()
    new = client.get(f"/jobs/{new_id}").json()
    assert old["current"] is False
    assert new["current"] is True
    # 取数始终指向最新一代
    body = client.get(f"/days/{DAY}/results").json()
    assert body["job_id"] == new_id


def test_english_headers_and_gbk_encoding(app_env):
    client = app_env["client"]
    traffic = "cell_id,uplink_bytes,downlink_bytes\r\nC001,10,20\r\nC002,30,40\r\n"
    # GBK 编码的中文片区名
    ownership = "cell_id,district\r\nC001,城东\r\nC002,城西\r\n".encode("gb18030")

    r1 = client.post(
        f"/days/{DAY}/files/traffic",
        files={"file": ("t.csv", traffic.encode("utf-8"), "text/csv")},
    )
    assert r1.status_code == 201
    r2 = client.post(
        f"/days/{DAY}/files/ownership",
        files={"file": ("o.csv", ownership, "text/csv")},
    )
    assert r2.status_code == 201
    run_worker(app_env)
    body = client.get(f"/days/{DAY}/results").json()
    assert body["ready"] is True
    districts = {r["district"]: r for r in body["results"]}
    assert districts["城东"]["total_bytes"] == 30
    assert districts["城西"]["total_bytes"] == 70


def test_files_persisted_alongside_database(app_env):
    client = app_env["client"]
    upload(client, DAY, "traffic", TRAFFIC, filename="raw.csv")
    upload(client, DAY, "ownership", OWNERSHIP)
    run_worker(app_env)

    storage = app_env["config"].storage_dir()
    stored = list(storage.rglob("*.csv"))
    assert len(stored) == 2
    # 落盘文件为规范化 UTF-8，可被 worker 流式重读
    for path in stored:
        text = path.read_text(encoding="utf-8")
        assert "C001" in text

    # 数据库里记录了存储路径与哈希
    import pathlib
    conn = app_env["db"].connect()
    try:
        rows = conn.execute("SELECT stored_path, sha256 FROM files").fetchall()
        assert all(r["sha256"] and len(r["sha256"]) == 64 for r in rows)
        assert all(pathlib.Path(r["stored_path"]).exists() for r in rows)
    finally:
        conn.close()
