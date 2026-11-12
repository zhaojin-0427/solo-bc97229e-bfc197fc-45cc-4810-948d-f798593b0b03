"""失败路径：缺归属整次失败、CSV 校验整份拒收、失败原因可查询。"""
from __future__ import annotations

from conftest import run_worker, upload

DAY = "2026-11-12"

OWNERSHIP = """基站ID,片区
C001,城东
C002,城东
"""


def test_missing_ownership_fails_entire_job(app_env):
    client = app_env["client"]
    traffic = """基站ID,上行字节数,下行字节数
C001,100,200
C002,300,400
C999,500,600
C888,700,800
"""
    upload(client, DAY, "traffic", traffic)
    ack = upload(client, DAY, "ownership", OWNERSHIP).json()
    job_id = ack["job"]["job_id"]

    run_worker(app_env)

    job = client.get(f"/jobs/{job_id}").json()
    assert job["status"] == "failed"
    assert job["error_code"] == "MISSING_OWNERSHIP"
    assert "2 个基站" in job["error_message"]
    assert set(job["missing_ownership"]) == {"C999", "C888"}

    result = client.get(f"/days/{DAY}/results").json()
    assert result["ready"] is False
    assert result["status"] == "failed"
    assert result["results"] == []
    assert set(result["missing_ownership"]) == {"C999", "C888"}

    # 失败作业没有写入任何结果行
    conn = app_env["db"].connect()
    try:
        rows = conn.execute("SELECT COUNT(*) AS c FROM results WHERE job_id=?", (job_id,)).fetchone()
        assert rows["c"] == 0
    finally:
        conn.close()


def test_fix_ownership_then_new_job_succeeds(app_env):
    client = app_env["client"]
    bad_traffic = """基站ID,上行字节数,下行字节数
C001,100,200
C999,500,600
"""
    upload(client, DAY, "traffic", bad_traffic)
    upload(client, DAY, "ownership", OWNERSHIP)
    run_worker(app_env)
    assert client.get(f"/days/{DAY}/results").json()["status"] == "failed"

    # 重新上传一份完整归属文件，触发重算并成功
    fixed = """基站ID,片区
C001,城东
C999,城东
"""
    upload(client, DAY, "ownership", fixed)
    run_worker(app_env)
    body = client.get(f"/days/{DAY}/results").json()
    assert body["ready"] is True
    assert body["status"] == "ready"
    east = body["results"][0]
    assert east["cell_count"] == 2
    assert east["total_bytes"] == 1400


def test_duplicate_cell_id_rejected_and_old_version_kept(app_env):
    client = app_env["client"]
    good = """基站ID,上行字节数,下行字节数
C001,100,200
"""
    upload(client, DAY, "traffic", good, filename="good.csv")

    dup = """基站ID,上行字节数,下行字节数
C001,100,200
C001,300,400
"""
    resp = upload(client, DAY, "traffic", dup, filename="dup.csv")
    assert resp.status_code == 400
    assert resp.json()["detail"]["code"] == "DUPLICATE_CELL_ID"

    # 旧版本仍然生效，没有产生 v2
    files = client.get(f"/days/{DAY}/files").json()
    assert len(files) == 1
    assert files[0]["version"] == 1
    assert files[0]["original_name"] == "good.csv"


def test_negative_and_non_integer_rejected(app_env):
    client = app_env["client"]
    bad = """基站ID,上行字节数,下行字节数
C001,-1,200
C002,abc,400
"""
    resp = upload(client, DAY, "traffic", bad)
    assert resp.status_code == 400
    assert resp.json()["detail"]["code"] == "INVALID_VALUE"


def test_missing_column_rejected(app_env):
    client = app_env["client"]
    bad = """基站ID,上行字节数
C001,100
"""
    resp = upload(client, DAY, "traffic", bad)
    assert resp.status_code == 400
    assert resp.json()["detail"]["code"] == "MISSING_COLUMN"


def test_empty_and_header_only_rejected(app_env):
    client = app_env["client"]
    assert upload(client, DAY, "traffic", "").status_code == 400
    header_only = "基站ID,上行字节数,下行字节数\n"
    resp = upload(client, DAY, "traffic", header_only)
    assert resp.status_code == 400
    assert resp.json()["detail"]["code"] == "NO_DATA_ROWS"


def test_zero_bytes_allowed(app_env):
    client = app_env["client"]
    traffic = """基站ID,上行字节数,下行字节数
C001,0,0
"""
    ownership = """基站ID,片区
C001,城东
"""
    upload(client, DAY, "traffic", traffic)
    upload(client, DAY, "ownership", ownership)
    run_worker(app_env)
    body = client.get(f"/days/{DAY}/results").json()
    assert body["ready"] is True
    assert body["results"][0]["total_bytes"] == 0


def test_invalid_date_and_type(app_env):
    client = app_env["client"]
    assert client.get("/days/2026-13-40/results").status_code == 422
    assert client.get("/days/2026-02-30/results").status_code == 422
    resp = upload(client, DAY, "unknown", "a,b\n1,2\n")
    assert resp.status_code == 422


def test_job_not_found(app_env):
    assert app_env["client"].get("/jobs/9999").status_code == 404
