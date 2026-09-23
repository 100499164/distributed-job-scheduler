from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from uuid import uuid4

import pytest
from test_jobs import request

from scheduler.config import Settings
from scheduler.control_plane.domain import Conflict
from scheduler.control_plane.jobs import Jobs
from scheduler.control_plane.scheduling import Scheduler
from scheduler.protocol.models import Claim, Register

pytestmark = pytest.mark.integration


def register(scheduler, capacity=1):
    worker = uuid4()
    scheduler.register(Register(workerId=worker, hostname="test", capacity=capacity, version="1"))
    return worker


def test_registration_idempotent_immutable_and_expired(db):
    scheduler = Scheduler(db, Settings())
    req = Register(workerId=uuid4(), hostname="test", capacity=1, version="1")
    assert scheduler.register(req) == scheduler.register(req)
    with pytest.raises(Conflict):
        scheduler.register(req.model_copy(update={"capacity": 2}))
    db.run(
        lambda c: c.execute(
            "UPDATE workers SET registered_at=clock_timestamp()-interval '1 hour',last_heartbeat_at=clock_timestamp()-interval '40 seconds'"
        )
    )
    with pytest.raises(Conflict) as error:
        scheduler.register(req)
    assert error.value.code == "SESSION_EXPIRED"


def test_many_workers_and_repeated_claim(db):
    Jobs(db, Settings()).create(request(count=8), "job")
    scheduler = Scheduler(db, Settings())
    workers = [register(scheduler) for _ in range(8)]
    barrier = Barrier(8)

    def claim(worker):
        req = Claim(workerId=worker, claimRequestId=uuid4())
        barrier.wait(timeout=10)
        first = scheduler.claim(req)
        assert scheduler.claim(req) == first
        return first

    with ThreadPoolExecutor(max_workers=8) as pool:
        assigned = list(pool.map(claim, workers))
    assert len({r["taskId"] for r in assigned}) == 8
    assert db.run(lambda c: c.execute("SELECT sum(attempt_count) AS n FROM tasks").fetchone())["n"] == 8


def test_two_claims_compete_for_one_slot(db):
    Jobs(db, Settings()).create(request(count=2), "job")
    scheduler = Scheduler(db, Settings())
    worker, barrier = register(scheduler), Barrier(2)

    def claim(_):
        barrier.wait(timeout=10)
        try:
            return scheduler.claim(Claim(workerId=worker, claimRequestId=uuid4()))
        except Conflict as exc:
            return exc.code

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(claim, range(2)))
    assert sum(isinstance(r, dict) for r in results) == 1
    assert "CAPACITY_EXHAUSTED" in results


def test_empty_claim_not_sticky_and_rollback(db):
    scheduler = Scheduler(db, Settings())
    worker = register(scheduler)
    req = Claim(workerId=worker, claimRequestId=uuid4())
    assert scheduler.claim(req) is None
    Jobs(db, Settings()).create(request(count=1), "job")

    def hook(name, c):
        if name == "claim_before_commit":
            raise RuntimeError("injected rollback")

    scheduler.hook = hook
    with pytest.raises(RuntimeError):
        scheduler.claim(req)
    scheduler.hook = lambda name, c: None
    assert scheduler.claim(req)["attemptNumber"] == 1
