import time
from concurrent.futures import ThreadPoolExecutor
from threading import Event

import pytest
from test_completion import completion, setup_job

from scheduler.control_plane.domain import Conflict
from scheduler.control_plane.recovery import Recovery
from scheduler.protocol.models import Heartbeat

pytestmark = [pytest.mark.integration, pytest.mark.fault]


def test_completion_rechecks_time_after_waiting_for_job_lock(db):
    s, jobs, job, [(w, a)] = setup_job(db)
    s.start(a["attemptId"], w)
    db.run(
        lambda c: c.execute(
            "UPDATE task_attempts SET lease_expires_at=clock_timestamp()+interval '0.3 seconds'"
        )
    )
    waiting = Event()

    def hook(name, c):
        if name == "worker_locked":
            waiting.set()

    s.hook = hook
    with ThreadPoolExecutor(max_workers=1) as pool:
        with db.transaction() as blocker:
            blocker.execute("SELECT id FROM jobs WHERE id=%s FOR UPDATE", (job["id"],))
            future = pool.submit(s.complete, a["attemptId"], completion(w, a))
            assert waiting.wait(timeout=5)
            deadline = time.monotonic() + 3
            while not db.run(
                lambda c: c.execute(
                    "SELECT clock_timestamp()>=lease_expires_at AS expired FROM task_attempts"
                ).fetchone()
            )["expired"]:
                assert time.monotonic() < deadline
                time.sleep(0.005)
        with pytest.raises(Conflict):
            future.result(timeout=5)
    assert jobs.job(job["id"])["completedTasks"] == 0


def test_absolute_deadline_expires_despite_recent_process_heartbeat(db):
    s, jobs, job, [(w, a)] = setup_job(db)
    s.start(a["attemptId"], w)
    db.run(
        lambda c: c.execute("""UPDATE task_attempts SET assigned_at=clock_timestamp()-interval '30 seconds',
        started_at=clock_timestamp()-interval '20 seconds',execution_deadline_at=clock_timestamp()-interval '1 second',
        lease_expires_at=clock_timestamp()-interval '2 seconds'""")
    )
    assert s.heartbeat(w, Heartbeat(activeAttemptIds=[a["attemptId"]]))["rejected"][0]["reason"] == "EXPIRED"
    Recovery(s).sweep()
    assert jobs.worker(w)["status"] == "ONLINE"
    assert jobs.attempts(a["taskId"])["items"][0]["errorCode"] == "EXECUTION_TIMEOUT"
