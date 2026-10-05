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
    # Make retry-wait tasks immediately eligible again.
    db.run(
        lambda c: c.execute(
            """
            UPDATE tasks
            SET available_at = clock_timestamp()
            WHERE status = 'RETRY_WAIT'
            """
        )
    )


def age_worker(db, worker):
    # Force the worker past the configured heartbeat timeout.
    db.run(
        lambda c: c.execute(
            """
            UPDATE workers
            SET
                registered_at = clock_timestamp() - interval '2 minutes',
                last_heartbeat_at = clock_timestamp() - interval '1 minute'
            WHERE id = %s
            """,
            (worker,),
        )
    )


def test_recover_dead_worker_and_reject_old_result(db):
    scheduler, jobs, job, [(worker, assignment)] = setup_job(db)

    scheduler.start(
        assignment["attemptId"],
        worker,
    )

    age_worker(
        db,
        worker,
    )

    recovery = Recovery(scheduler)
    recovery.sweep()

    assert jobs.worker(worker)["status"] == "OFFLINE"

    assert jobs.task(assignment["taskId"])["status"] == "RETRY_WAIT"

    assert jobs.attempts(assignment["taskId"])["items"][0]["errorCode"] == "WORKER_LOST"

    eligible(db)

    new_worker = register(scheduler)

    new_assignment = scheduler.claim(
        Claim(
            workerId=new_worker,
            claimRequestId=uuid4(),
        )
    )

    assert new_assignment["attemptNumber"] == 2

    scheduler.start(
        new_assignment["attemptId"],
        new_worker,
    )

    # The old attempt cannot complete after replacement has been assigned.
    with pytest.raises(Conflict):
        scheduler.complete(
            assignment["attemptId"],
            completion(
                worker,
                assignment,
            ),
        )

    scheduler.complete(
        new_assignment["attemptId"],
        completion(
            new_worker,
            new_assignment,
        ),
    )

    assert jobs.job(job["id"])["result"]["totalPrimeCount"] == 25


def test_transient_retry_permanent_failure_and_no_fail_fast(db):
    scheduler, jobs, job, pairs = setup_job(
        db,
        2,
    )

    worker, assignment = pairs[0]

    scheduler.start(
        assignment["attemptId"],
        worker,
    )

    failure = Completion(
        workerId=worker,
        outcome="FAILED",
        error={
            "code": "TRANSIENT_ERROR",
            "message": "temporary",
        },
    )

    scheduler.complete(
        assignment["attemptId"],
        failure,
    )

    assert jobs.task(assignment["taskId"])["status"] == "RETRY_WAIT"

    eligible(db)

    new_assignment = scheduler.claim(
        Claim(
            workerId=worker,
            claimRequestId=uuid4(),
        )
    )

    scheduler.start(
        new_assignment["attemptId"],
        worker,
    )

    # Historical completion responses remain replayable.
    assert (
        scheduler.complete(
            assignment["attemptId"],
            failure,
        )["status"]
        == "FAILED"
    )

    # Permanent failures consume the task without scheduling another retry.
    scheduler.complete(
        new_assignment["attemptId"],
        Completion(
            workerId=worker,
            outcome="FAILED",
            error={
                "code": "INVALID_PAYLOAD",
                "message": "permanent",
            },
        ),
    )

    # One failed task does not fail the whole job while other tasks are active.
    assert jobs.job(job["id"])["status"] == "RUNNING"

    second_worker, second_assignment = pairs[1]

    scheduler.start(
        second_assignment["attemptId"],
        second_worker,
    )

    scheduler.complete(
        second_assignment["attemptId"],
        completion(
            second_worker,
            second_assignment,
        ),
    )

    final = jobs.job(job["id"])

    assert final["status"] == "FAILED" and final["result"] is None

    assert final["failedTasks"] == final["completedTasks"] == 1


def test_assignment_loss_with_zero_retries(db):
    scheduler = Scheduler(
        db,
        Settings(),
    )
    jobs = Jobs(
        db,
        Settings(),
    )

    job, _ = jobs.create(
        request(
            count=1,
            maxRetries=0,
        ),
        "zero",
    )

    worker = register(scheduler)

    assignment = scheduler.claim(
        Claim(
            workerId=worker,
            claimRequestId=uuid4(),
        )
    )

    db.run(
        lambda c: c.execute(
            """
            UPDATE task_attempts
            SET
                assigned_at = clock_timestamp() - interval '30 seconds',
                lease_expires_at = clock_timestamp() - interval '1 second'
            """
        )
    )

    recovery = Recovery(scheduler)

    # Running recovery repeatedly must be safe.
    recovery.sweep()
    recovery.sweep()

    assert jobs.job(job["id"])["status"] == "FAILED"

    assert jobs.job(job["id"])["failedTasks"] == 1

    assert jobs.attempts(assignment["taskId"])["items"][0]["errorCode"] == "ASSIGNMENT_TIMEOUT"


def test_recovery_interrupted_is_resumable(db):
    scheduler, jobs, job, pairs = setup_job(
        db,
        2,
    )

    for worker, assignment in pairs:
        age_worker(
            db,
            worker,
        )

    closures = 0

    def interrupt(name, c):
        nonlocal closures

        if name == "recovery_before_commit":
            closures += 1

            if closures == 2:
                raise RuntimeError("scheduler interrupted")

    scheduler.hook = interrupt

    # Recovery may partially commit before the process is interrupted.
    with pytest.raises(RuntimeError):
        Recovery(scheduler).sweep()

    assert (
        db.run(
            lambda c: c.execute(
                """
                SELECT count(*) AS n
                FROM task_attempts
                WHERE status = 'EXPIRED'
                """
            ).fetchone()
        )["n"]
        == 1
    )

    scheduler.hook = lambda *args: None

    # A later sweep must safely continue from the persisted state.
    Recovery(scheduler).sweep()

    assert (
        db.run(
            lambda c: c.execute(
                """
                SELECT count(*) AS n
                FROM task_attempts
                WHERE status = 'EXPIRED'
                """
            ).fetchone()
        )["n"]
        == 2
    )


def test_heartbeat_competing_with_offline_cannot_resurrect(db):
    scheduler, jobs, job, [(worker, assignment)] = setup_job(db)

    age_worker(
        db,
        worker,
    )

    barrier = Barrier(2)

    def heartbeat():
        barrier.wait(timeout=10)

        with pytest.raises(Conflict):
            scheduler.heartbeat(
                worker,
                Heartbeat(activeAttemptIds=[]),
            )

    def offline():
        barrier.wait(timeout=10)

        Recovery(scheduler).offline(worker)

    # Heartbeat and recovery race over the same worker state.
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [
            pool.submit(heartbeat),
            pool.submit(offline),
        ]

        for future in futures:
            future.result(timeout=15)

    # Once recovery marks the worker offline, heartbeat cannot resurrect it.
    assert jobs.worker(worker)["status"] == "OFFLINE"


def test_completion_against_expiration_has_one_closure(db):
    scheduler, jobs, job, [(worker, assignment)] = setup_job(db)

    scheduler.start(
        assignment["attemptId"],
        worker,
    )

    expire_lease(
        db,
        assignment["attemptId"],
    )

    barrier = Barrier(2)

    def finish():
        barrier.wait(timeout=10)

        with pytest.raises(Conflict):
            scheduler.complete(
                assignment["attemptId"],
                completion(
                    worker,
                    assignment,
                ),
            )

    def expire():
        barrier.wait(timeout=10)

        Recovery(scheduler).expire(assignment["attemptId"])

    # Completion and expiration race, but only one closure may win.
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [
            pool.submit(finish),
            pool.submit(expire),
        ]

        for future in futures:
            future.result(timeout=15)

    assert jobs.task(assignment["taskId"])["status"] == "RETRY_WAIT"

    assert jobs.job(job["id"])["completedTasks"] == 0
