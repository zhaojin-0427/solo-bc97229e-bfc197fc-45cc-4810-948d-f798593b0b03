"""端到端主流程：上传 -> 汇总 -> 替换 -> 重算。"""
from __future__ import annotations

from conftest import run_worker, upload

TRAFFIC = """基站ID,上行字节数,下行字节数
C001,1200000,5400000
C002,800000,3200000
C003,2500000,9800000
C004,640000,2100000
C005,1750000,6600000
C006,980000,4100000
"""

OWNERSHIP = """基站ID,片区
C001,城东
C002,城东
C003,城西
C004,城西
C005,城南
C006,城南
"""

TRAFFIC_V2 = """基站ID,上行字节数,下行字节数
C001,2000000,7000000
C002,900000,3500000
C003,3000000,12000000
C004,700000,2500000
C005,1900000,7200000
C006,1100000,4600000
"""

DAY = "2026-11-12"


def test_empty_day_reports_waiting(app_env):
    resp = app_env["client"].get(f"/days/{DAY}/results")
    assert resp.status_code == 200
    body = resp.json()
    assert body["ready"] is False
    assert body["status"] == "waiting_files"
    assert "traffic" in body["message"] and "ownership" in body["message"]


def test_only_one_file_still_waiting(app_env):
    resp = upload(app_env["client"], DAY, "traffic", TRAFFIC)
    assert resp.status_code == 201
    assert resp.json()["both_files_present"] is False

    body = app_env["client"].get(f"/days/{DAY}/results").json()
    assert body["ready"] is False
    assert body["status"] == "waiting_files"


def test_full_flow_and_aggregation(app_env):
    client = app_env["client"]
    upload(client, DAY, "traffic", TRAFFIC)
    ack = upload(client, DAY, "ownership", OWNERSHIP).json()
    assert ack["both_files_present"] is True
    assert ack["job"]["status"] == "queued"
    first_job_id = ack["job"]["job_id"]

    # 未就绪时明确说明，且不返回任何旧结果
    pending = client.get(f"/days/{DAY}/results").json()
    assert pending["ready"] is False
    assert pending["status"] == "computing"
    assert pending["results"] == []

    run_worker(app_env)

    ready = client.get(f"/days/{DAY}/results").json()
    assert ready["ready"] is True
    assert ready["status"] == "ready"
    assert ready["job_id"] == first_job_id

    by_district = {r["district"]: r for r in ready["results"]}
    # 城东 = C001 + C002
    east = by_district["城东"]
    assert east["cell_count"] == 2
    assert east["uplink_bytes"] == 2_000_000
    assert east["downlink_bytes"] == 8_600_000
    assert east["total_bytes"] == 10_600_000
    # 城西 = C003 + C004
    west = by_district["城西"]
    assert west["uplink_bytes"] == 3_140_000
    assert west["downlink_bytes"] == 11_900_000
    assert west["total_bytes"] == 15_040_000
    # 城南
    south = by_district["城南"]
    assert south["cell_count"] == 2
    assert south["total_bytes"] == 13_430_000

    job = client.get(f"/jobs/{first_job_id}").json()
    assert job["status"] == "succeeded"
    assert job["progress_percent"] == 100
    assert job["current"] is True
    assert job["result_row_count"] == 3


def test_replacement_invalidates_old_result_and_recomputes(app_env):
    client = app_env["client"]
    upload(client, DAY, "traffic", TRAFFIC)
    upload(client, DAY, "ownership", OWNERSHIP)
    run_worker(app_env)

    first = client.get(f"/days/{DAY}/results").json()
    assert first["ready"] is True
    first_job_id = first["job_id"]

    # 整份替换流量文件
    ack = upload(client, DAY, "traffic", TRAFFIC_V2, filename="traffic_v2.csv").json()
    assert ack["job"] is not None
    new_job_id = ack["job"]["job_id"]
    assert new_job_id != first_job_id

    # 替换成功后旧结果立即失效：取数只认当前文件对的新作业
    during = client.get(f"/days/{DAY}/results").json()
    assert during["ready"] is False
    assert during["status"] == "computing"
    assert during["job_id"] == new_job_id

    # 旧作业保留为终态 succeeded 供审计，但已不对应当前文件（current=false）
    old_job = client.get(f"/jobs/{first_job_id}").json()
    assert old_job["status"] == "succeeded"
    assert old_job["current"] is False

    # 旧结果行仍在库内但取数接口绝不返回（只认当前文件对的新作业）
    conn = app_env["db"].connect()
    try:
        from app import repository
        old_rows = repository.get_results(conn, first_job_id)
        assert len(old_rows) == 3  # 历史结果保留可审计
    finally:
        conn.close()

    run_worker(app_env)
    redone = client.get(f"/days/{DAY}/results").json()
    assert redone["ready"] is True
    assert redone["job_id"] == new_job_id
    east = next(r for r in redone["results"] if r["district"] == "城东")
    assert east["uplink_bytes"] == 2_900_000
    assert east["total_bytes"] == 13_400_000

    # 文件版本号递增，可追溯历史
    files = client.get(f"/days/{DAY}/files").json()
    traffic_files = [f for f in files if f["file_type"] == "traffic"]
    assert traffic_files[0]["version"] == 2
    assert traffic_files[0]["original_name"] == "traffic_v2.csv"


def test_replace_ownership_file_recomputes_too(app_env):
    client = app_env["client"]
    upload(client, DAY, "traffic", TRAFFIC)
    upload(client, DAY, "ownership", OWNERSHIP)
    run_worker(app_env)

    new_ownership = """基站ID,片区
C001,北区
C002,北区
C003,北区
C004,南区
C005,南区
C006,南区
"""
    upload(client, DAY, "ownership", new_ownership)
    run_worker(app_env)
    body = client.get(f"/days/{DAY}/results").json()
    assert body["ready"] is True
    districts = sorted(r["district"] for r in body["results"])
    assert districts == ["北区", "南区"]


def test_day_listing(app_env):
    client = app_env["client"]
    upload(client, DAY, "traffic", TRAFFIC)
    upload(client, DAY, "ownership", OWNERSHIP)
    run_worker(app_env)
    days = client.get("/days").json()
    assert any(d["business_date"] == DAY and d["ready"] is True for d in days)
