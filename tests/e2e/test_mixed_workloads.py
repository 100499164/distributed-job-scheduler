"""Run the user-facing showcase against real HTTP processes and PostgreSQL."""

import subprocess
import sys

import pytest
from conftest import eventually

from deploy.demo import call
from scheduler.worker.runtime import Client, RemoteError

pytestmark = [pytest.mark.integration, pytest.mark.e2e]


def test_showcase_with_shared_worker_processes(cluster, db):
    for index in range(3):
        cluster.worker(f"mixed-{index}")
    eventually(lambda: db.run(lambda c: c.execute("SELECT count(*) AS n FROM workers").fetchone())["n"] == 3)
    result = subprocess.run(
        [sys.executable, "deploy/showcase.py", "--api", cluster.api_url, "--timeout", "60"],
        cwd=cluster.root,
        capture_output=True,
        text=True,
        timeout=75,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "All three results and idempotency checks verified." in result.stdout
    assert (
        db.run(lambda c: c.execute("SELECT count(*) AS n FROM jobs WHERE status='COMPLETED'").fetchone())["n"]
        == 3
    )
    operations = db.run(
        lambda c: c.execute("""SELECT a.worker_id,count(DISTINCT j.operation) AS n
        FROM task_attempts a JOIN tasks t ON t.id=a.task_id JOIN jobs j ON j.id=t.job_id
        WHERE a.status='SUCCEEDED' GROUP BY a.worker_id""").fetchall()
    )
    assert any(row["n"] > 1 for row in operations)


def test_http_rejects_mismatched_completion(cluster):
    from uuid import uuid4

    worker = str(uuid4())
    call(
        cluster.api_url,
        "/v1/jobs",
        {
            "name": "sum",
            "taskCount": 1,
            "payload": {"operation": "RANGE_SUM", "fromInclusive": 1, "toExclusive": 101},
        },
        "mismatch",
    )
    client = Client(cluster.scheduler_url)
    client.post(
        "/internal/v1/workers/register",
        {"workerId": worker, "hostname": "test", "version": "1", "capacity": 1},
    )
    assignment = client.post("/internal/v1/claims", {"workerId": worker, "claimRequestId": str(uuid4())})
    path = f"/internal/v1/attempts/{assignment['attemptId']}"
    client.post(path + "/start", {"workerId": worker})
    with pytest.raises(RemoteError) as error:
        client.post(
            path + "/completion", {"workerId": worker, "outcome": "SUCCEEDED", "result": {"primeCount": 1}}
        )
    assert error.value.status == 400 and error.value.code == "INVALID_RESULT"
    client.post(
        path + "/completion", {"workerId": worker, "outcome": "SUCCEEDED", "result": {"rangeSum": 5050}}
    )
