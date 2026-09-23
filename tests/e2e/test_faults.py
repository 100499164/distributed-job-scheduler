import threading
from urllib.error import URLError
from uuid import UUID, uuid4

import pytest
from conftest import eventually
from test_end_to_end import create, get, sieve_count

from scheduler.worker.runtime import Client, RemoteError, Worker
from scheduler.worker.workload import prime_count

pytestmark = [pytest.mark.integration, pytest.mark.e2e, pytest.mark.fault]
FAST = {
    "HEARTBEAT_INTERVAL_MS": "100",
    "WORKER_TIMEOUT_MS": "1200",
    "EXECUTION_LEASE_MS": "1500",
    "ASSIGNMENT_TIMEOUT_MS": "2500",
    "RECOVERY_INTERVAL_MS": "100",
    "MAX_EXECUTION_MS": "30000",
}


@pytest.mark.parametrize("cluster", [FAST], indirect=True)
def test_kill_calculating_worker_reassign_and_reject_old_result(cluster, db):
    victim = cluster.worker("victim")
    eventually(lambda: db.run(lambda c: c.execute("SELECT count(*) AS n FROM workers").fetchone())["n"] == 1)
    job = create(cluster.api_url, count=4, end=800000)
    old = eventually(
        lambda: db.run(
            lambda c: c.execute("""SELECT a.*,t.payload FROM task_attempts a JOIN tasks t ON t.id=a.task_id
        WHERE a.status='RUNNING' LIMIT 1""").fetchone()
        )
    )
    victim.kill()
    victim.wait(timeout=5)
    cluster.worker("replacement")
    eventually(
        lambda: db.run(
            lambda c: c.execute(
                "SELECT count(*) AS n FROM task_attempts WHERE task_id=%s", (old["task_id"],)
            ).fetchone()
        )["n"]
        >= 2,
        20,
    )
    with pytest.raises(RemoteError) as error:
        Client(cluster.scheduler_url).post(
            f"/internal/v1/attempts/{old['id']}/completion",
            {"workerId": str(old["worker_id"]), "outcome": "SUCCEEDED", "result": {"primeCount": 0}},
        )
    assert error.value.status == 409
    final = eventually(
        lambda: (
            r if (r := get(cluster.api_url + "/v1/jobs/" + job["id"]))["status"] == "COMPLETED" else None
        ),
        40,
    )
    assert final["result"]["totalPrimeCount"] == sieve_count(800000)
    history = db.run(
        lambda c: c.execute(
            "SELECT status,error_code FROM task_attempts WHERE id=%s", (old["id"],)
        ).fetchone()
    )
    assert history == {"status": "EXPIRED", "error_code": "WORKER_LOST"}


def test_short_scheduler_restart_preserves_live_attempt(cluster, db):
    job = create(cluster.api_url, count=1, end=10000)
    client, worker = Client(cluster.scheduler_url), str(uuid4())
    client.post(
        "/internal/v1/workers/register",
        {"workerId": worker, "hostname": "test", "capacity": 1, "version": "1"},
    )
    assignment = client.post("/internal/v1/claims", {"workerId": worker, "claimRequestId": str(uuid4())})
    started = client.post(f"/internal/v1/attempts/{assignment['attemptId']}/start", {"workerId": worker})
    cluster.scheduler.kill()
    cluster.scheduler.wait(timeout=5)
    cluster.start_scheduler()
    assert (
        client.post(f"/internal/v1/attempts/{assignment['attemptId']}/start", {"workerId": worker}) == started
    )
    client.post(
        f"/internal/v1/attempts/{assignment['attemptId']}/completion",
        {"workerId": worker, "outcome": "SUCCEEDED", "result": {"primeCount": prime_count(2, 10000)}},
    )
    assert get(cluster.api_url + "/v1/jobs/" + job["id"])["result"]["totalPrimeCount"] == 1229


def test_scheduler_restart_with_expired_lease_recovers_state(cluster, db):
    job = create(cluster.api_url, count=1, end=10000)
    client, worker = Client(cluster.scheduler_url), str(uuid4())
    client.post(
        "/internal/v1/workers/register",
        {"workerId": worker, "hostname": "test", "capacity": 1, "version": "1"},
    )
    claim_key = str(uuid4())
    a = client.post("/internal/v1/claims", {"workerId": worker, "claimRequestId": claim_key})
    client.post(f"/internal/v1/attempts/{a['attemptId']}/start", {"workerId": worker})
    cluster.scheduler.kill()
    cluster.scheduler.wait(timeout=5)
    db.run(
        lambda c: c.execute(
            """UPDATE task_attempts SET assigned_at=clock_timestamp()-interval '60 seconds',
        started_at=clock_timestamp()-interval '50 seconds',lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=%s""",
            (UUID(a["attemptId"]),),
        )
    )
    cluster.start_scheduler()
    with pytest.raises(RemoteError) as error:
        client.post("/internal/v1/claims", {"workerId": worker, "claimRequestId": claim_key})
    assert error.value.status == 409
    cluster.worker("after-restart")
    result = eventually(
        lambda: (
            r if (r := get(cluster.api_url + "/v1/jobs/" + job["id"]))["status"] == "COMPLETED" else None
        )
    )
    assert result["result"]["totalPrimeCount"] == 1229
    assert db.run(lambda c: c.execute("SELECT count(*) AS n FROM task_attempts").fetchone())["n"] == 2


class DropOnce(Client):
    def __init__(self, url, suffix):
        super().__init__(url)
        self.suffix, self.dropped = suffix, False

    def post(self, path, body):
        result = super().post(path, body)
        if path.endswith(self.suffix) and result is not None and not self.dropped:
            self.dropped = True
            raise URLError("Test-only response loss AFTER committed HTTP operation")
        return result


class CountingWorker(Worker):
    calculations = 0

    def _calculate(self, slot):
        self.calculations += 1
        return super()._calculate(slot)


@pytest.mark.parametrize("suffix", ["/claims", "/start", "/completion"])
def test_worker_retries_same_identity_after_response_loss(cluster, db, suffix):
    client = DropOnce(cluster.scheduler_url, suffix)
    worker = CountingWorker(cluster.scheduler_url, client=client)
    thread = threading.Thread(target=worker.run, daemon=True)
    thread.start()
    try:
        job = create(cluster.api_url, count=1, end=10000)
        result = eventually(
            lambda: (
                r if (r := get(cluster.api_url + "/v1/jobs/" + job["id"]))["status"] == "COMPLETED" else None
            )
        )
        eventually(lambda: not worker.slots and worker.pending_claim is None)
        assert client.dropped
        assert worker.calculations == 1
        assert result["result"]["totalPrimeCount"] == 1229
        assert db.run(lambda c: c.execute("SELECT count(*) AS n FROM task_attempts").fetchone())["n"] == 1
    finally:
        worker.stop.set()
        thread.join(timeout=10)
        assert not thread.is_alive()
