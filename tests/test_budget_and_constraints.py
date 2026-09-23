from uuid import uuid4

import psycopg
import pytest
from test_claims import register
from test_jobs import request
from test_recovery import eligible

from scheduler.config import Settings
from scheduler.control_plane.jobs import Jobs
from scheduler.control_plane.scheduling import Scheduler
from scheduler.protocol.models import Claim, Completion

pytestmark = pytest.mark.integration


def test_retry_budget_exhausted_after_four_reservations(db):
    jobs, s = Jobs(db, Settings()), Scheduler(db, Settings())
    job, _ = jobs.create(request(count=1), "budget")
    worker = register(s)
    for n in range(1, 5):
        assigned = s.claim(Claim(workerId=worker, claimRequestId=uuid4()))
        assert assigned["attemptNumber"] == n
        s.start(assigned["attemptId"], worker)
        s.complete(
            assigned["attemptId"],
            Completion(
                workerId=worker, outcome="FAILED", error={"code": "TRANSIENT_ERROR", "message": "test"}
            ),
        )
        eligible(db)
    assert jobs.job(job["id"])["status"] == "FAILED"
    assert jobs.job(job["id"])["failedTasks"] == 1
    assert s.claim(Claim(workerId=worker, claimRequestId=uuid4())) is None


def test_unique_active_attempt_and_foreign_keys_are_enforced(db):
    jobs, s = Jobs(db, Settings()), Scheduler(db, Settings())
    job, _ = jobs.create(request(count=1), "constraints")
    worker = register(s)
    a = s.claim(Claim(workerId=worker, claimRequestId=uuid4()))
    with pytest.raises(psycopg.errors.UniqueViolation):
        db.run(
            lambda c: c.execute(
                """INSERT INTO task_attempts(id,task_id,worker_id,attempt_number,claim_request_id,status,lease_expires_at)
            VALUES (%s,%s,%s,2,%s,'ASSIGNED',clock_timestamp()+interval '15 seconds')""",
                (uuid4(), a["taskId"], worker, uuid4()),
            )
        )
    with pytest.raises(psycopg.errors.ForeignKeyViolation):
        db.run(lambda c: c.execute("DELETE FROM jobs WHERE id=%s", (job["id"],)))
    with pytest.raises(psycopg.errors.CheckViolation):
        db.run(lambda c: c.execute("UPDATE jobs SET completed_tasks=2 WHERE id=%s", (job["id"],)))
