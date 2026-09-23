from threading import Event, Thread
from uuid import uuid4

import pytest
from conftest import eventually
from test_faults import FAST, DropOnce

from deploy.demo import call
from scheduler.worker.runtime import Worker
from scheduler.worker.workload import Cancelled

pytestmark = [pytest.mark.integration, pytest.mark.e2e]


def create(api, operation="RANGE_SUM", count=2):
    payload = (
        {"operation": operation, "samples": 10000, "seed": 10}
        if operation == "MONTE_CARLO_PI"
        else {"operation": operation, "fromInclusive": 2, "toExclusive": 100000}
    )
    return call(
        api, "/v1/jobs", {"name": "capability-e2e", "taskCount": count, "payload": payload}, str(uuid4())
    )


@pytest.mark.parametrize("cluster", [FAST], indirect=True)
def test_specialized_workers_over_http_and_late_compatible_worker(cluster, db):
    cluster.spawn("prime", ["-m", "scheduler.worker.runtime"], {"WORKER_OPERATIONS": "PRIME_COUNT"})
    eventually(lambda: db.run(lambda c: c.execute("SELECT count(*) AS n FROM workers").fetchone())["n"] == 1)
    pi = create(cluster.api_url, "MONTE_CARLO_PI")
    # The same worker processes an unrelated prime job while pi remains pending.
    prime = create(cluster.api_url, "PRIME_COUNT")
    eventually(lambda: call(cluster.api_url, "/v1/jobs/" + prime["id"])["status"] == "COMPLETED")
    assert call(cluster.api_url, "/v1/jobs/" + pi["id"])["status"] == "QUEUED"
    cluster.spawn("pi", ["-m", "scheduler.worker.runtime"], {"WORKER_OPERATIONS": "MONTE_CARLO_PI"})
    eventually(lambda: call(cluster.api_url, "/v1/jobs/" + pi["id"])["status"] == "COMPLETED")


class ControlledWorker(Worker):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.entered, self.release = Event(), Event()

    def _calculate(self, slot):
        self.entered.set()
        while not self.release.wait(0.01):
            if slot.cancel.is_set():
                raise Cancelled()
        return super()._calculate(slot)


@pytest.mark.parametrize("cluster", [FAST], indirect=True)
@pytest.mark.parametrize("timeout", [False, True])
def test_draining_renews_leases_resolves_lost_ack_and_stops_claiming(cluster, db, timeout):
    job = create(cluster.api_url)
    worker = ControlledWorker(
        cluster.scheduler_url,
        operations=["RANGE_SUM"],
        drain_timeout=1 if timeout else 10,
        client=DropOnce(cluster.scheduler_url, "/completion"),
    )
    thread = Thread(target=worker.run, daemon=True)
    thread.start()
    try:
        assert worker.entered.wait(10)
        worker.request_drain()
        first = db.run(lambda c: c.execute("SELECT lease_expires_at FROM task_attempts").fetchone())[
            "lease_expires_at"
        ]
        eventually(
            lambda: db.run(lambda c: c.execute("SELECT lease_expires_at FROM task_attempts").fetchone())[
                "lease_expires_at"
            ]
            > first
        )
        if not timeout:
            worker.release.set()
        thread.join(timeout=10)
        assert not thread.is_alive()
        assert len(call(cluster.api_url, f"/v1/jobs/{job['id']}/tasks")["items"]) == 2
        assert db.run(lambda c: c.execute("SELECT count(*) AS n FROM task_attempts").fetchone())["n"] == 1
        if not timeout:
            assert worker.client.dropped
            assert call(cluster.api_url, "/v1/jobs/" + job["id"])["completedTasks"] == 1
        else:
            eventually(
                lambda: db.run(lambda c: c.execute("SELECT status FROM task_attempts").fetchone())["status"]
                == "EXPIRED"
            )
    finally:
        worker.stop.set()
        worker.release.set()
        thread.join(timeout=10)


@pytest.mark.parametrize("cluster", [FAST], indirect=True)
def test_sigterm_drains_real_process_without_claiming_more(cluster, db):
    job = call(
        cluster.api_url,
        "/v1/jobs",
        {
            "name": "signal-drain",
            "taskCount": 100,
            "payload": {"operation": "PRIME_COUNT", "fromInclusive": 2, "toExclusive": 5000000},
        },
        str(uuid4()),
    )
    worker = cluster.worker("draining-process")
    eventually(
        lambda: db.run(
            lambda c: c.execute("SELECT count(*) AS n FROM task_attempts WHERE status='RUNNING'").fetchone()
        )["n"]
        > 0
    )
    worker.terminate()
    assert worker.wait(timeout=10) == 0
    tasks = call(cluster.api_url, f"/v1/jobs/{job['id']}/tasks?limit=200")["items"]
    assert any(t["status"] == "COMPLETED" for t in tasks)
    assert any(t["status"] == "QUEUED" for t in tasks)
    assert not any(t["status"] in ("RUNNING", "ASSIGNED") for t in tasks)
