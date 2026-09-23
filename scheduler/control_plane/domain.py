import hashlib
import json
from datetime import datetime, timedelta

from scheduler.persistence.database import Row

EDGES = {
    "job": {("QUEUED", "RUNNING"), ("RUNNING", "COMPLETED"), ("RUNNING", "FAILED")},
    "task": {
        ("QUEUED", "ASSIGNED"),
        ("RETRY_WAIT", "ASSIGNED"),
        ("ASSIGNED", "RUNNING"),
        ("ASSIGNED", "RETRY_WAIT"),
        ("ASSIGNED", "FAILED"),
        ("RUNNING", "COMPLETED"),
        ("RUNNING", "RETRY_WAIT"),
        ("RUNNING", "FAILED"),
    },
    "attempt": {
        ("ASSIGNED", "RUNNING"),
        ("ASSIGNED", "EXPIRED"),
        ("RUNNING", "SUCCEEDED"),
        ("RUNNING", "FAILED"),
        ("RUNNING", "EXPIRED"),
    },
    "worker": {("ONLINE", "OFFLINE")},
}
RETRYABLE = {"TRANSIENT_ERROR", "WORKER_LOST", "ASSIGNMENT_TIMEOUT", "LEASE_EXPIRED", "EXECUTION_TIMEOUT"}


def transition(entity: str, previous: str, new: str) -> None:
    if (previous, new) not in EDGES[entity]:
        raise ValueError(f"Invalid {entity} transition {previous} -> {new}")


def fingerprint(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    ).hexdigest()


def retry_state(attempt_count: int, max_retries: int, error: str | None) -> str:
    if not 0 <= max_retries <= 10 or not 1 <= attempt_count <= max_retries + 1:
        raise ValueError("Invalid attempt budget")
    return "RETRY_WAIT" if error in RETRYABLE and attempt_count < max_retries + 1 else "FAILED"


def backoff(attempt_number: int, jitter: float) -> timedelta:
    if attempt_number < 1 or not 0.8 <= jitter <= 1.2:
        raise ValueError("Invalid backoff input")
    return timedelta(seconds=min(30, 2 ** min(attempt_number - 1, 5)) * jitter)


def session_valid(worker: Row, now: datetime, timeout_ms: int) -> bool:
    return worker["status"] == "ONLINE" and now < worker["last_heartbeat_at"] + timedelta(
        milliseconds=timeout_ms
    )


def attempt_valid(attempt: Row, now: datetime) -> bool:
    deadline = attempt["execution_deadline_at"]
    return now < attempt["lease_expires_at"] and (deadline is None or now < deadline)


class Conflict(Exception):
    def __init__(self, code: str, message: str, status: int = 409) -> None:
        super().__init__(message)
        self.code, self.message, self.status = code, message, status
