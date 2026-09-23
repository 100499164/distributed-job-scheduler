from datetime import datetime, timedelta, timezone
from itertools import product

import pytest
from pydantic import ValidationError

from scheduler.control_plane.domain import (
    attempt_valid,
    backoff,
    fingerprint,
    retry_state,
    session_valid,
    transition,
)
from scheduler.protocol.models import Completion, CreateJob, PrimePayload


def test_terminal_states_and_invalid_transitions():
    for entity, terminals, states in [
        ("job", ["COMPLETED", "FAILED"], ["QUEUED", "RUNNING", "COMPLETED", "FAILED"]),
        (
            "task",
            ["COMPLETED", "FAILED"],
            ["QUEUED", "ASSIGNED", "RUNNING", "RETRY_WAIT", "COMPLETED", "FAILED"],
        ),
        (
            "attempt",
            ["SUCCEEDED", "FAILED", "EXPIRED"],
            ["ASSIGNED", "RUNNING", "SUCCEEDED", "FAILED", "EXPIRED"],
        ),
        ("worker", ["OFFLINE"], ["ONLINE", "OFFLINE"]),
    ]:
        for old, new in product(terminals, states):
            with pytest.raises(ValueError):
                transition(entity, old, new)
    for old, new in [("ASSIGNED", "COMPLETED"), ("RETRY_WAIT", "QUEUED"), ("QUEUED", "COMPLETED")]:
        with pytest.raises(ValueError):
            transition("task", old, new)


def test_retries_and_backoff():
    assert retry_state(1, 0, "WORKER_LOST") == "FAILED"
    assert retry_state(1, 3, "INVALID_PAYLOAD") == "FAILED"
    assert retry_state(3, 3, "TRANSIENT_ERROR") == "RETRY_WAIT"
    assert retry_state(4, 3, "TRANSIENT_ERROR") == "FAILED"
    assert [backoff(n, 1).total_seconds() for n in range(1, 8)] == [1, 2, 4, 8, 16, 30, 30]
    assert backoff(100000, 1.2).total_seconds() == 36
    with pytest.raises(ValueError):
        backoff(1, float("nan"))


def test_exact_time_limits():
    now = datetime.now(timezone.utc)
    w = {"status": "ONLINE", "last_heartbeat_at": now - timedelta(seconds=30)}
    assert not session_valid(w, now, 30000)
    assert session_valid(w, now - timedelta(microseconds=1), 30000)
    a = {"lease_expires_at": now, "execution_deadline_at": now + timedelta(seconds=30)}
    assert not attempt_valid(a, now)
    assert attempt_valid(a, now - timedelta(microseconds=1))


@pytest.mark.parametrize("bad", [True, "2", 2.5, -1, 1_000_000_001])
def test_strict_payload(bad):
    with pytest.raises(ValidationError):
        PrimePayload(fromInclusive=bad, toExclusive=10)


def test_invalid_partition_and_outcome():
    with pytest.raises(ValidationError):
        CreateJob(name="x", taskCount=10, payload={"fromInclusive": 2, "toExclusive": 5})
    with pytest.raises(ValidationError):
        Completion(workerId="00000000-0000-0000-0000-000000000001", outcome="EXPIRED")
    assert fingerprint({"a": 1, "b": 2}) == fingerprint({"b": 2, "a": 1})
