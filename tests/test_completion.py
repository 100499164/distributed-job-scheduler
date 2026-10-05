from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from uuid import uuid4

import pytest
from test_claims import register
from test_jobs import request

from scheduler.config import Settings
from scheduler.control_plane.domain import Conflict
from scheduler.control_plane.jobs import Jobs
from scheduler.control_plane.scheduling import Scheduler
from scheduler.protocol.models import Claim, Completion
from scheduler.worker.workload import Cancelled, prime_count


def setup_job(db, count=1):
    scheduler = Scheduler(db, Settings())
    jobs = Jobs(db, Settings())

    job, _ = jobs.create(
        request(count),
        "job",
    )

    assigned = []

    for _ in range(count):
        worker = register(scheduler)

        assigned.append(
            (
                worker,
                scheduler.claim(
                    Claim(
                        workerId=worker,
                        claimRequestId=uuid4(),
                    )
                ),
            )
        )

    return scheduler, jobs, job, assigned


def completion(worker, assignment):
    payload = assignment["payload"]

    return Completion(
        workerId=worker,
        outcome="SUCCEEDED",
        result={
            "primeCount": prime_count(
                payload["fromInclusive"],
                payload["toExclusive"],
            )
        },
    )


def test_prime_known_and_cooperative():
    assert prime_count(2, 100) == 25
    assert prime_count(2, 100000) == 9592
    assert prime_count(10, 11) == 0

    # Long-running calculations must cooperate with cancellation.
    with pytest.raises(Cancelled):
        prime_count(
            2,
            10000,
            lambda: True,
        )


@pytest.mark.integration
def test_start_and_completion_idempotence_even_after_offline(db):
    scheduler, jobs, job, [(worker, assignment)] = setup_job(db)
    attempt_id = assignment["attemptId"]

    # Completion is invalid until the assigned attempt has started.
    with pytest.raises(Conflict):
        scheduler.complete(
            attempt_id,
            completion(worker, assignment),
        )

    first = scheduler.start(
        attempt_id,
        worker,
    )

    # Starting the same attempt again is idempotent.
    assert scheduler.start(
        attempt_id,
        worker,
    ) == first

    result = scheduler.complete(
        attempt_id,
        completion(worker, assignment),
    )

    db.run(
        lambda c: c.execute(
            """
            UPDATE workers
            SET
                status = 'OFFLINE',
                offline_at = clock_timestamp()
            """
        )
    )

    # An already accepted completion remains replayable even if the worker goes offline.
    assert scheduler.complete(
        attempt_id,
        completion(worker, assignment),
    ) == result

    # Replaying the attempt with a different result is a conflict.
    with pytest.raises(Conflict):
        scheduler.complete(
            attempt_id,
            Completion(
                workerId=worker,
                outcome="SUCCEEDED",
                result={"primeCount": 0},
            ),
        )

    state = jobs.job(job["id"])

    assert (
        state["status"] == "COMPLETED"
        and state["completedTasks"] == 1
    )

    assert state["result"] == {
        "totalPrimeCount": 25
    }


@pytest.mark.integration
def test_last_completions_concurrent(db):
    scheduler, jobs, job, assignments = setup_job(
        db,
        2,
    )

    barrier = Barrier(2)

    for worker, assignment in assignments:
        scheduler.start(
            assignment["attemptId"],
            worker,
        )

    def finish(item):
        worker, assignment = item

        # Make both final completions race for the same job update.
        barrier.wait(timeout=10)

        return scheduler.complete(
            assignment["attemptId"],
            completion(
                worker,
                assignment,
            ),
        )

    with ThreadPoolExecutor(
        max_workers=2
    ) as pool:
        list(
            pool.map(
                finish,
                assignments,
            )
        )

    assert jobs.job(
        job["id"]
    )["result"] == {
        "totalPrimeCount": 25
    }

    assert jobs.job(
        job["id"]
    )["completedTasks"] == 2


@pytest.mark.integration
def test_lost_completion_ack_and_wrong_owner(db):
    scheduler, jobs, job, [(worker, assignment)] = setup_job(db)
    attempt_id = assignment["attemptId"]

    scheduler.start(
        attempt_id,
        worker,
    )

    # Only the worker that owns the attempt may complete it.
    with pytest.raises(Conflict):
        scheduler.complete(
            attempt_id,
            completion(
                uuid4(),
                assignment,
            ),
        )

    def drop(name, c):
        if name == "completion_after_commit":
            raise ConnectionError(
                "ACK lost after commit"
            )

    scheduler.hook = drop

    # Simulate the DB commit succeeding while the client loses the ACK.
    with pytest.raises(ConnectionError):
        scheduler.complete(
            attempt_id,
            completion(
                worker,
                assignment,
            ),
        )

    scheduler.hook = lambda *args: None

    # Retrying the same completion must recover the committed result.
    assert scheduler.complete(
        attempt_id,
        completion(
            worker,
            assignment,
        ),
    )["status"] == "SUCCEEDED"

    assert jobs.job(
        job["id"]
    )["completedTasks"] == 1