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
    scheduler, jobs = Scheduler(db, Settings()), Jobs(db, Settings())
    job, _ = jobs.create(request(count), "job")
    assigned = []
    for _ in range(count):
        worker = register(scheduler)
        assigned.append((worker, scheduler.claim(Claim(workerId=worker, claimRequestId=uuid4()))))
    return scheduler, jobs, job, assigned


def completion(worker, assignment):
    p = assignment["payload"]
    return Completion(
        workerId=worker,
        outcome="SUCCEEDED",
        result={"primeCount": prime_count(p["fromInclusive"], p["toExclusive"])},
    )


def test_prime_known_and_cooperative():
    assert prime_count(2, 100) == 25
    assert prime_count(2, 100000) == 9592
    assert prime_count(10, 11) == 0
    with pytest.raises(Cancelled):
        prime_count(2, 10000, lambda: True)


@pytest.mark.integration
def test_start_and_completion_idempotence_even_after_offline(db):
    s, jobs, job, [(worker, a)] = setup_job(db)
    aid = a["attemptId"]
    with pytest.raises(Conflict):
        s.complete(aid, completion(worker, a))
    first = s.start(aid, worker)
    assert s.start(aid, worker) == first
    result = s.complete(aid, completion(worker, a))
    db.run(lambda c: c.execute("UPDATE workers SET status='OFFLINE',offline_at=clock_timestamp()"))
    assert s.complete(aid, completion(worker, a)) == result
    with pytest.raises(Conflict):
        s.complete(aid, Completion(workerId=worker, outcome="SUCCEEDED", result={"primeCount": 0}))
    state = jobs.job(job["id"])
    assert state["status"] == "COMPLETED" and state["completedTasks"] == 1
    assert state["result"] == {"totalPrimeCount": 25}


@pytest.mark.integration
def test_last_completions_concurrent(db):
    s, jobs, job, assignments = setup_job(db, 2)
    barrier = Barrier(2)
    for w, a in assignments:
        s.start(a["attemptId"], w)

    def finish(item):
        w, a = item
        barrier.wait(timeout=10)
        return s.complete(a["attemptId"], completion(w, a))

    with ThreadPoolExecutor(max_workers=2) as pool:
        list(pool.map(finish, assignments))
    assert jobs.job(job["id"])["result"] == {"totalPrimeCount": 25}
    assert jobs.job(job["id"])["completedTasks"] == 2


@pytest.mark.integration
def test_lost_completion_ack_and_wrong_owner(db):
    s, jobs, job, [(worker, a)] = setup_job(db)
    aid = a["attemptId"]
    s.start(aid, worker)
    with pytest.raises(Conflict):
        s.complete(aid, completion(uuid4(), a))

    def drop(name, c):
        if name == "completion_after_commit":
            raise ConnectionError("ACK lost after commit")

    s.hook = drop
    with pytest.raises(ConnectionError):
        s.complete(aid, completion(worker, a))
    s.hook = lambda *args: None
    assert s.complete(aid, completion(worker, a))["status"] == "SUCCEEDED"
    assert jobs.job(job["id"])["completedTasks"] == 1
