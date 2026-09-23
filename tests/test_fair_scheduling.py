from uuid import uuid4

import pytest
from test_capabilities import claim, finish, job, register

from scheduler.config import Settings
from scheduler.control_plane.jobs import Jobs
from scheduler.control_plane.scheduling import Scheduler
from scheduler.protocol.models import Claim

pytestmark = pytest.mark.integration


def test_small_later_job_runs_before_large_finishes(db):
    jobs, scheduler = Jobs(db, Settings()), Scheduler(db, Settings())
    large = job(jobs, count=1000)
    worker = register(scheduler)
    finish(scheduler, worker, claim(scheduler, worker))
    small = job(jobs, count=4)
    order = []
    for _ in range(8):
        assignment = claim(scheduler, worker)
        order.append(assignment["jobId"])
        finish(scheduler, worker, assignment)
    assert order == [small["id"], large["id"]] * 4
    assert jobs.job(small["id"])["status"] == "COMPLETED"
    assert jobs.job(large["id"])["completedTasks"] == 5


def test_three_jobs_round_robin_survives_scheduler_reconstruction(db):
    jobs, scheduler = Jobs(db, Settings()), Scheduler(db, Settings())
    created = [job(jobs, count=20) for _ in range(3)]
    worker = register(scheduler, capacity=12)
    order = []
    for _ in range(9):
        # New service object each time: no in-memory rotation state.
        scheduler = Scheduler(db, Settings())
        request = Claim(workerId=worker, claimRequestId=uuid4())
        assignment = scheduler.claim(request)
        assert scheduler.claim(request) == assignment  # replay does not consume a turn
        order.append(assignment["jobId"])
    assert order == [j["id"] for j in created] * 3
    assert db.run(lambda c: c.execute("SELECT count(*) AS n FROM task_attempts").fetchone())["n"] == 9


def test_locked_tasks_do_not_hide_other_jobs_and_backoff_is_respected(db):
    jobs, scheduler = Jobs(db, Settings()), Scheduler(db, Settings())
    first = job(jobs, count=2)
    later = job(jobs, count=2)
    worker = register(scheduler, capacity=3)
    with db.transaction() as blocker:
        blocker.execute("SELECT id FROM tasks WHERE job_id=%s FOR UPDATE", (first["id"],))
        assert claim(scheduler, worker)["jobId"] == later["id"]
    assert claim(scheduler, worker)["jobId"] == first["id"]


def test_incompatible_jobs_do_not_block_supported_work(db):
    jobs, scheduler = Jobs(db, Settings()), Scheduler(db, Settings())
    job(jobs, "MONTE_CARLO_PI", 1000)
    supported = job(jobs, "RANGE_SUM", 4)
    worker = register(scheduler, ["RANGE_SUM"])
    assert claim(scheduler, worker)["jobId"] == supported["id"]


def test_assignment_rollback_does_not_consume_fairness_turn(db):
    jobs, scheduler = Jobs(db, Settings()), Scheduler(db, Settings())
    first, second = job(jobs), job(jobs)
    worker = register(scheduler)

    def fail(name, connection):
        if name == "claim_before_commit":
            raise RuntimeError("rollback")

    scheduler.hook = fail
    with pytest.raises(RuntimeError):
        claim(scheduler, worker)
    scheduler.hook = lambda *args: None
    assignment = claim(scheduler, worker)
    assert assignment["jobId"] == first["id"]
    finish(scheduler, worker, assignment)
    assert claim(scheduler, worker)["jobId"] == second["id"]
