import threading
from uuid import UUID

import pytest
from conftest import eventually
from test_end_to_end import create, get
from test_faults import FAST

from scheduler.worker.runtime import Worker
from scheduler.worker.workload import Cancelled

pytestmark = [pytest.mark.integration, pytest.mark.e2e, pytest.mark.fault]


class BlockedWorker(Worker):
    """Test-only workload handler. No fault switch or endpoint exists in production."""

    def __init__(self, url, suppress_heartbeats=False):
        super().__init__(url)
        self.release_calculation = threading.Event()
        self.calculating = threading.Event()
        self.suppress_heartbeats = suppress_heartbeats

    def _start_heartbeats(self):
        if not self.suppress_heartbeats:
            super()._start_heartbeats()

    def _calculate(self, slot):
        self.calculating.set()
        while not self.release_calculation.wait(0.01):
            if slot.cancel.is_set():
                raise Cancelled()
        return super()._calculate(slot)


@pytest.mark.parametrize("cluster", [FAST], indirect=True)
def test_suppress_heartbeats_while_old_calculation_stays_alive(cluster, db):
    old = BlockedWorker(cluster.scheduler_url, suppress_heartbeats=True)
    thread = threading.Thread(target=old.run, daemon=True)
    thread.start()
    try:
        job = create(cluster.api_url, count=1, end=10000)
        assert old.calculating.wait(10)
        cluster.worker("healthy")
        final = eventually(
            lambda: (
                r if (r := get(cluster.api_url + "/v1/jobs/" + job["id"]))["status"] == "COMPLETED" else None
            ),
            20,
        )
        assert final["result"]["totalPrimeCount"] == 1229
        assert thread.is_alive()  # Physical old execution can outlive persistent ownership.
        old.release_calculation.set()
        eventually(lambda: not old.slots or old.stop.is_set())
        assert (
            db.run(
                lambda c: c.execute(
                    "SELECT status FROM task_attempts WHERE worker_id=%s", (UUID(old.id),)
                ).fetchone()
            )["status"]
            == "EXPIRED"
        )
        assert get(cluster.api_url + "/v1/jobs/" + job["id"])["completedTasks"] == 1
    finally:
        old.stop.set()
        old.release_calculation.set()
        thread.join(10)
        assert not thread.is_alive()


@pytest.mark.parametrize("cluster", [{**FAST, "MAX_EXECUTION_MS": "700"}], indirect=True)
def test_healthy_process_with_stalled_calculation_hits_absolute_deadline(cluster, db):
    old = BlockedWorker(cluster.scheduler_url)
    thread = threading.Thread(target=old.run, daemon=True)
    thread.start()
    try:
        job = create(cluster.api_url, count=1, end=10000)
        assert old.calculating.wait(10)

        def expired():
            row = db.run(
                lambda c: c.execute(
                    "SELECT error_code FROM task_attempts WHERE worker_id=%s AND status='EXPIRED' LIMIT 1",
                    (UUID(old.id),),
                ).fetchone()
            )
            return row

        assert eventually(expired, 10)["error_code"] == "EXECUTION_TIMEOUT"
        assert (
            db.run(lambda c: c.execute("SELECT status FROM workers WHERE id=%s", (UUID(old.id),)).fetchone())[
                "status"
            ]
            == "ONLINE"
        )
        old.stop.set()
        old.release_calculation.set()
        thread.join(10)
        cluster.worker("healthy-after-deadline")
        result = eventually(
            lambda: (
                r if (r := get(cluster.api_url + "/v1/jobs/" + job["id"]))["status"] == "COMPLETED" else None
            ),
            20,
        )
        assert result["result"]["totalPrimeCount"] == 1229
    finally:
        old.stop.set()
        old.release_calculation.set()
        thread.join(10)
