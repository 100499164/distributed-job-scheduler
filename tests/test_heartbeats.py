from uuid import uuid4

import pytest
from test_completion import completion, setup_job

from scheduler.control_plane.domain import Conflict
from scheduler.protocol.models import Heartbeat

pytestmark = pytest.mark.integration


def expire_lease(db, aid):
    db.run(
        lambda c: c.execute(
            """UPDATE task_attempts SET assigned_at=clock_timestamp()-interval '60 seconds',
        started_at=clock_timestamp()-interval '50 seconds',lease_expires_at=clock_timestamp()-interval '1 second'
        WHERE id=%s""",
            (aid,),
        )
    )


def test_only_listed_running_attempts_renew_and_deadline_caps(db):
    s, jobs, job, [(w, a)] = setup_job(db)
    aid = a["attemptId"]
    heartbeat = Heartbeat(activeAttemptIds=[aid])
    assert s.heartbeat(w, heartbeat)["rejected"][0]["reason"] == "NOT_RUNNING"
    assert jobs.attempts(a["taskId"])["items"][0]["leaseExpiresAt"] == a["leaseExpiresAt"]
    initial = s.start(aid, w)
    s.heartbeat(w, Heartbeat(activeAttemptIds=[]))
    assert jobs.attempts(a["taskId"])["items"][0]["leaseExpiresAt"] == initial["leaseExpiresAt"]
    response = s.heartbeat(w, heartbeat)
    assert response["renewed"][0]["leaseExpiresAt"] > initial["leaseExpiresAt"]
    deadline = db.run(
        lambda c: c.execute(
            """UPDATE task_attempts SET execution_deadline_at=clock_timestamp()+interval '5 seconds',
        lease_expires_at=clock_timestamp()+interval '3 seconds' WHERE id=%s RETURNING execution_deadline_at""",
            (aid,),
        ).fetchone()
    )["execution_deadline_at"]
    assert s.heartbeat(w, heartbeat)["renewed"][0]["leaseExpiresAt"] == deadline


def test_expired_unrecovered_lease_rejects_start_completion_and_renewal(db):
    s, jobs, job, [(w, a)] = setup_job(db)
    aid = a["attemptId"]
    s.start(aid, w)
    expire_lease(db, aid)
    assert s.heartbeat(w, Heartbeat(activeAttemptIds=[aid]))["rejected"][0]["reason"] == "EXPIRED"
    with pytest.raises(Conflict):
        s.start(aid, w)
    with pytest.raises(Conflict):
        s.complete(aid, completion(w, a))
    assert jobs.job(job["id"])["completedTasks"] == 0


def test_foreign_and_unknown_attempts_are_not_renewed(db):
    s, jobs, job, pairs = setup_job(db, 2)
    w, a = pairs[0]
    other_a = pairs[1][1]
    assert (
        s.heartbeat(w, Heartbeat(activeAttemptIds=[other_a["attemptId"]]))["rejected"][0]["reason"]
        == "WRONG_OWNER"
    )
    assert s.heartbeat(w, Heartbeat(activeAttemptIds=[uuid4()]))["rejected"][0]["reason"] == "UNKNOWN_ATTEMPT"
    db.run(
        lambda c: c.execute(
            "UPDATE workers SET status='OFFLINE',offline_at=clock_timestamp() WHERE id=%s", (w,)
        )
    )
    with pytest.raises(Conflict) as error:
        s.heartbeat(w, Heartbeat(activeAttemptIds=[]))
    assert error.value.code == "SESSION_EXPIRED"
