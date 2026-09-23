from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from uuid import uuid4

import pytest
from test_claims import register
from test_completion import completion, setup_job
from test_heartbeats import expire_lease
from test_jobs import request

from scheduler.config import Settings
from scheduler.control_plane.domain import Conflict
from scheduler.control_plane.jobs import Jobs
from scheduler.control_plane.recovery import Recovery
from scheduler.control_plane.scheduling import Scheduler
from scheduler.protocol.models import Claim, Completion, Heartbeat

pytestmark = pytest.mark.integration


def eligible(db):
    db.run(lambda c: c.execute("UPDATE tasks SET available_at=clock_timestamp() WHERE status='RETRY_WAIT'"))


def age_worker(db, worker):
    db.run(
        lambda c: c.execute(
            """UPDATE workers SET registered_at=clock_timestamp()-interval '2 minutes',
        last_heartbeat_at=clock_timestamp()-interval '1 minute' WHERE id=%s""",
            (worker,),
        )
    )


def test_recover_dead_worker_and_reject_old_result(db):
    s, jobs, job, [(w, a)] = setup_job(db)
    s.start(a["attemptId"], w)
    age_worker(db, w)
    recovery = Recovery(s)
    recovery.sweep()
    assert jobs.worker(w)["status"] == "OFFLINE"
    assert jobs.task(a["taskId"])["status"] == "RETRY_WAIT"
    assert jobs.attempts(a["taskId"])["items"][0]["errorCode"] == "WORKER_LOST"
    eligible(db)
    new_worker = register(s)
    new = s.claim(Claim(workerId=new_worker, claimRequestId=uuid4()))
    assert new["attemptNumber"] == 2
    s.start(new["attemptId"], new_worker)
    with pytest.raises(Conflict):
        s.complete(a["attemptId"], completion(w, a))
    s.complete(new["attemptId"], completion(new_worker, new))
    assert jobs.job(job["id"])["result"]["totalPrimeCount"] == 25


def test_transient_retry_permanent_failure_and_no_fail_fast(db):
    s, jobs, job, pairs = setup_job(db, 2)
    w, a = pairs[0]
    s.start(a["attemptId"], w)
    failure = Completion(
        workerId=w, outcome="FAILED", error={"code": "TRANSIENT_ERROR", "message": "temporary"}
    )
    s.complete(a["attemptId"], failure)
    assert jobs.task(a["taskId"])["status"] == "RETRY_WAIT"
    eligible(db)
    new = s.claim(Claim(workerId=w, claimRequestId=uuid4()))
    s.start(new["attemptId"], w)
    assert s.complete(a["attemptId"], failure)["status"] == "FAILED"  # Historical failure ACK.
    s.complete(
        new["attemptId"],
        Completion(workerId=w, outcome="FAILED", error={"code": "INVALID_PAYLOAD", "message": "permanent"}),
    )
    assert jobs.job(job["id"])["status"] == "RUNNING"
    w2, a2 = pairs[1]
    s.start(a2["attemptId"], w2)
    s.complete(a2["attemptId"], completion(w2, a2))
    final = jobs.job(job["id"])
    assert final["status"] == "FAILED" and final["result"] is None
    assert final["failedTasks"] == final["completedTasks"] == 1


def test_assignment_loss_with_zero_retries(db):
    s, jobs = Scheduler(db, Settings()), Jobs(db, Settings())
    job, _ = jobs.create(request(count=1, maxRetries=0), "zero")
    w = register(s)
    a = s.claim(Claim(workerId=w, claimRequestId=uuid4()))
    db.run(
        lambda c: c.execute(
            "UPDATE task_attempts SET assigned_at=clock_timestamp()-interval '30 seconds',lease_expires_at=clock_timestamp()-interval '1 second'"
        )
    )
    recovery = Recovery(s)
    recovery.sweep()
    recovery.sweep()
    assert jobs.job(job["id"])["status"] == "FAILED"
    assert jobs.job(job["id"])["failedTasks"] == 1
    assert jobs.attempts(a["taskId"])["items"][0]["errorCode"] == "ASSIGNMENT_TIMEOUT"


def test_recovery_interrupted_is_resumable(db):
    s, jobs, job, pairs = setup_job(db, 2)
    for w, a in pairs:
        age_worker(db, w)
    closures = 0

    def interrupt(name, c):
        nonlocal closures
        if name == "recovery_before_commit":
            closures += 1
            if closures == 2:
                raise RuntimeError("scheduler interrupted")

    s.hook = interrupt
    with pytest.raises(RuntimeError):
        Recovery(s).sweep()
    assert (
        db.run(
            lambda c: c.execute("SELECT count(*) AS n FROM task_attempts WHERE status='EXPIRED'").fetchone()
        )["n"]
        == 1
    )
    s.hook = lambda *args: None
    Recovery(s).sweep()
    assert (
        db.run(
            lambda c: c.execute("SELECT count(*) AS n FROM task_attempts WHERE status='EXPIRED'").fetchone()
        )["n"]
        == 2
    )


def test_heartbeat_competing_with_offline_cannot_resurrect(db):
    s, jobs, job, [(w, a)] = setup_job(db)
    age_worker(db, w)
    barrier = Barrier(2)

    def heartbeat():
        barrier.wait(timeout=10)
        with pytest.raises(Conflict):
            s.heartbeat(w, Heartbeat(activeAttemptIds=[]))

    def offline():
        barrier.wait(timeout=10)
        Recovery(s).offline(w)

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(heartbeat), pool.submit(offline)]
        for f in futures:
            f.result(timeout=15)
    assert jobs.worker(w)["status"] == "OFFLINE"


def test_completion_against_expiration_has_one_closure(db):
    s, jobs, job, [(w, a)] = setup_job(db)
    s.start(a["attemptId"], w)
    expire_lease(db, a["attemptId"])
    barrier = Barrier(2)

    def finish():
        barrier.wait(timeout=10)
        with pytest.raises(Conflict):
            s.complete(a["attemptId"], completion(w, a))

    def expire():
        barrier.wait(timeout=10)
        Recovery(s).expire(a["attemptId"])

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(finish), pool.submit(expire)]
        for f in futures:
            f.result(timeout=15)
    assert jobs.task(a["taskId"])["status"] == "RETRY_WAIT"
    assert jobs.job(job["id"])["completedTasks"] == 0
