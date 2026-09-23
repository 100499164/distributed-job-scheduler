import random
from datetime import datetime, timedelta
from typing import TypedDict
from uuid import UUID, uuid4

from psycopg import Connection
from psycopg.types.json import Jsonb

from scheduler.config import Settings
from scheduler.control_plane.domain import (
    Conflict,
    attempt_valid,
    backoff,
    fingerprint,
    retry_state,
    session_valid,
    transition,
)
from scheduler.control_plane.selection import select_task
from scheduler.observability import Metrics, event
from scheduler.persistence.database import Database, Hook, Row, required_row
from scheduler.protocol.models import Claim, Completion, Heartbeat, Register
from scheduler.workloads.catalog import workload


class Assignment(TypedDict):
    taskId: UUID
    attemptId: UUID
    attemptNumber: int
    jobId: UUID
    status: str
    leaseExpiresAt: datetime
    payload: dict[str, str | int]


def db_now(c: Connection[Row]) -> datetime:
    return required_row(c.execute("SELECT clock_timestamp() AS now").fetchone())["now"]


class Scheduler:
    def __init__(
        self,
        db: Database,
        settings: Settings,
        metrics: Metrics | None = None,
        hook: Hook = lambda name, connection: None,
    ) -> None:
        self.db = db
        self.settings = settings
        self.hook = hook
        self.metrics = metrics or Metrics()

    def _worker(self, c: Connection[Row], worker_id: UUID) -> Row:
        row = c.execute("SELECT * FROM workers WHERE id=%s FOR UPDATE", (worker_id,)).fetchone()
        if row is None:
            raise Conflict("NOT_FOUND", "Worker not found", 404)
        self.hook("worker_locked", c)
        return row

    def _session(self, worker: Row, now: datetime) -> None:
        if not session_valid(worker, now, self.settings.worker_timeout_ms):
            raise Conflict("SESSION_EXPIRED", "Worker session expired; stop this instance")

    def _attempt(
        self, c: Connection[Row], attempt_id: UUID, owner: UUID | None = None
    ) -> tuple[Row, Row, Row]:
        # Only immutable relationship discovery precedes locks. No authoritative state is read here.
        identity = c.execute(
            "SELECT task_id,worker_id FROM task_attempts WHERE id=%s", (attempt_id,)
        ).fetchone()
        if identity is None:
            raise Conflict("NOT_FOUND", "Attempt not found", 404)
        worker = self._worker(c, identity["worker_id"])
        task = required_row(
            c.execute("SELECT * FROM tasks WHERE id=%s FOR UPDATE", (identity["task_id"],)).fetchone()
        )
        attempt = required_row(
            c.execute("SELECT * FROM task_attempts WHERE id=%s FOR UPDATE", (attempt_id,)).fetchone()
        )
        if owner is not None and attempt["worker_id"] != owner:
            raise Conflict("WRONG_OWNER", "Attempt belongs to another worker")
        return worker, task, attempt

    def _valid(self, worker: Row, task: Row, attempt: Row, now: datetime, allowed: set[str]) -> None:
        self._session(worker, now)
        if attempt["status"] not in allowed or not attempt_valid(attempt, now):
            raise Conflict("STALE_ATTEMPT", f"Attempt is not executable ({attempt['status']})")
        if task["status"] != attempt["status"]:
            raise Conflict("INVARIANT_VIOLATION", "Task and active attempt disagree", 503)

    @staticmethod
    def _assignment(task: Row, attempt: Row) -> Assignment:
        return {
            "taskId": task["id"],
            "attemptId": attempt["id"],
            "attemptNumber": attempt["attempt_number"],
            "jobId": task["job_id"],
            "status": attempt["status"],
            "leaseExpiresAt": attempt["lease_expires_at"],
            "payload": task["payload"],
        }

    def register(self, request: Register) -> Row:
        if request.capacity > self.settings.max_capacity:
            raise Conflict("CAPACITY_LIMIT", "Capacity exceeds configured limit", 400)

        def transaction(c: Connection[Row]) -> Row:
            c.execute(
                """INSERT INTO workers(id,hostname,version,status,capacity,supported_operations)
                VALUES (%s,%s,%s,'ONLINE',%s,%s) ON CONFLICT(id) DO NOTHING""",
                (
                    request.worker_id,
                    request.hostname,
                    request.version,
                    request.capacity,
                    request.supported_operations,
                ),
            )
            worker = self._worker(c, request.worker_id)
            self._session(worker, db_now(c))
            if (
                worker["hostname"],
                worker["version"],
                worker["capacity"],
                sorted(worker["supported_operations"]),
            ) != (request.hostname, request.version, request.capacity, request.supported_operations):
                raise Conflict("REGISTRATION_CONFLICT", "Registration data is immutable")
            return {
                "workerId": worker["id"],
                "status": worker["status"],
                "capacity": worker["capacity"],
                "supportedOperations": worker["supported_operations"],
                "settings": self.settings.wire(),
            }

        result = self.db.run(transaction)
        event(
            "worker_registered",
            worker_id=request.worker_id,
            supported_operations=request.supported_operations,
            new_state="ONLINE",
        )
        return result

    def start(self, attempt_id: UUID, owner: UUID) -> Row:
        def transaction(c: Connection[Row]) -> Row:
            worker, task, attempt = self._attempt(c, attempt_id, owner)
            now = db_now(c)
            self._valid(worker, task, attempt, now, {"ASSIGNED", "RUNNING"})
            if attempt["status"] == "ASSIGNED":
                transition("attempt", "ASSIGNED", "RUNNING")
                transition("task", "ASSIGNED", "RUNNING")
                deadline = now + timedelta(milliseconds=self.settings.max_execution_ms)
                lease = min(now + timedelta(milliseconds=self.settings.execution_lease_ms), deadline)
                attempt = required_row(
                    c.execute(
                        """UPDATE task_attempts SET status='RUNNING',started_at=%s,
                    execution_deadline_at=%s,lease_expires_at=%s WHERE id=%s RETURNING *""",
                        (now, deadline, lease, attempt_id),
                    ).fetchone()
                )
                c.execute(
                    "UPDATE tasks SET status='RUNNING',first_started_at=coalesce(first_started_at,%s) WHERE id=%s",
                    (now, task["id"]),
                )
            self.hook("start_before_commit", c)
            return {
                "attemptId": attempt_id,
                "status": "RUNNING",
                "startedAt": attempt["started_at"],
                "leaseExpiresAt": attempt["lease_expires_at"],
                "executionDeadlineAt": attempt["execution_deadline_at"],
            }

        result = self.db.run(transaction)
        event("attempt_started", worker_id=owner, attempt_id=attempt_id, new_state="RUNNING")
        return result

    def _job(self, c: Connection[Row], job_id: UUID) -> Row:
        job = required_row(c.execute("SELECT * FROM jobs WHERE id=%s FOR UPDATE", (job_id,)).fetchone())
        self.hook("job_locked", c)
        return job

    def _finish_task(
        self,
        c: Connection[Row],
        task: Row,
        job: Row,
        now: datetime,
        new_state: str,
        error: str | None = None,
        jitter: float = 1,
    ) -> None:
        transition("task", task["status"], new_state)
        if new_state == "RETRY_WAIT":
            c.execute(
                "UPDATE tasks SET status='RETRY_WAIT',last_error_code=%s,available_at=%s WHERE id=%s",
                (error, now + backoff(task["attempt_count"], jitter), task["id"]),
            )
            return
        c.execute(
            "UPDATE tasks SET status=%s,last_error_code=%s,finished_at=%s WHERE id=%s",
            (new_state, error, now, task["id"]),
        )
        completed = job["completed_tasks"]
        failed = job["failed_tasks"]
        if new_state == "COMPLETED":
            completed += 1
        elif new_state == "FAILED":
            failed += 1
        finished = completed + failed == job["task_count"]
        state = "RUNNING"
        if finished:
            state = "FAILED" if failed else "COMPLETED"
            transition("job", job["status"], state)
        # Job was read AFTER its lock. Both counts and final state change in this one statement.
        c.execute(
            "UPDATE jobs SET completed_tasks=%s,failed_tasks=%s,status=%s,finished_at=%s WHERE id=%s",
            (completed, failed, state, now if finished else None, job["id"]),
        )

    def complete(self, attempt_id: UUID, request: Completion) -> Row:
        content = request.model_dump(mode="json", by_alias=True, exclude={"worker_id"})
        digest = fingerprint(content)
        jitter = random.uniform(0.8, 1.2)

        def transaction(c: Connection[Row]) -> tuple[Row, str]:
            worker, task, attempt = self._attempt(c, attempt_id, request.worker_id)
            # Historical ACK comes before session/lease checks, and never modifies Job/Task.
            if attempt["completion_hash"] is not None:
                if attempt["completion_hash"] != digest:
                    raise Conflict("COMPLETION_CONFLICT", "Accepted completion has different content")
                return {"attemptId": attempt_id, "status": attempt["status"]}, "duplicate"
            job = self._job(c, task["job_id"])
            now = db_now(c)  # Includes any time spent waiting for the final Job lock.
            self._valid(worker, task, attempt, now, {"RUNNING"})
            if request.result is not None:
                try:
                    workload(task["payload"]["operation"]).validate_result(task["payload"], request.result)
                except ValueError as exc:
                    raise Conflict("INVALID_RESULT", str(exc), 400) from None
            transition("attempt", attempt["status"], request.outcome)
            error = request.error.code if request.error else None
            new_state = (
                "COMPLETED"
                if request.outcome == "SUCCEEDED"
                else retry_state(task["attempt_count"], task["max_retries"], error)
            )
            c.execute(
                """UPDATE task_attempts SET status=%s,finished_at=%s,error_code=%s,error_message=%s,
                result=%s,completion_hash=%s WHERE id=%s""",
                (
                    request.outcome,
                    now,
                    error,
                    request.error.message if request.error else None,
                    Jsonb(request.result.model_dump(by_alias=True)) if request.result else None,
                    digest,
                    attempt_id,
                ),
            )
            self._finish_task(c, task, job, now, new_state, error, jitter)
            self.hook("completion_before_commit", c)
            return {"attemptId": attempt_id, "status": request.outcome}, new_state

        result, outcome = self.db.run(transaction)
        self.hook("completion_after_commit", None)
        self.metrics.operations.labels("completion", outcome).inc()
        if outcome == "RETRY_WAIT":
            self.metrics.retries.inc()
        event(
            "completion_" + outcome.lower(),
            worker_id=request.worker_id,
            attempt_id=attempt_id,
            new_state=result["status"],
        )
        return result

    def heartbeat(self, worker_id: UUID, request: Heartbeat) -> Row:
        def transaction(c: Connection[Row]) -> Row:
            worker = self._worker(c, worker_id)
            if len(request.active_attempt_ids) > worker["capacity"]:
                raise Conflict("HEARTBEAT_LIMIT", "Attempt list exceeds session capacity", 400)
            identities = c.execute(
                "SELECT id,task_id,worker_id FROM task_attempts WHERE id=ANY(%s)",
                (request.active_attempt_ids,),
            ).fetchall()
            own = [a for a in identities if a["worker_id"] == worker_id]
            task_ids = sorted({a["task_id"] for a in own})
            # Lock ALL tasks in global UUID order before locking any attempts.
            tasks = c.execute(
                "SELECT * FROM tasks WHERE id=ANY(%s) ORDER BY id FOR UPDATE", (task_ids,)
            ).fetchall()
            attempts = c.execute(
                "SELECT * FROM task_attempts WHERE id=ANY(%s) ORDER BY task_id,id FOR UPDATE",
                ([a["id"] for a in own],),
            ).fetchall()
            by_id = {a["id"]: a for a in attempts}
            owners = {a["id"]: a["worker_id"] for a in identities}
            task_states = {t["id"]: t["status"] for t in tasks}
            now = db_now(c)
            self._session(worker, now)
            c.execute("UPDATE workers SET last_heartbeat_at=%s WHERE id=%s", (now, worker_id))
            renewed, rejected = [], []
            for aid in request.active_attempt_ids:
                a = by_id.get(aid)
                if aid not in owners:
                    reason = "UNKNOWN_ATTEMPT"
                elif owners[aid] != worker_id:
                    reason = "WRONG_OWNER"
                elif a is None or a["status"] != "RUNNING":
                    reason = "NOT_RUNNING"
                elif not attempt_valid(a, now):
                    reason = "EXPIRED"
                elif task_states[a["task_id"]] != "RUNNING":
                    reason = "INVARIANT_VIOLATION"
                else:
                    lease = min(
                        now + timedelta(milliseconds=self.settings.execution_lease_ms),
                        a["execution_deadline_at"],
                    )
                    c.execute("UPDATE task_attempts SET lease_expires_at=%s WHERE id=%s", (lease, aid))
                    renewed.append({"attemptId": aid, "leaseExpiresAt": lease})
                    continue
                rejected.append({"attemptId": aid, "reason": reason})
            self.hook("heartbeat_before_commit", c)
            return {"renewed": renewed, "rejected": rejected}

        return self.db.run(transaction)

    def claim(self, request: Claim) -> Assignment | None:
        def transaction(c: Connection[Row]) -> tuple[Assignment | None, str]:
            worker = self._worker(c, request.worker_id)
            previous = c.execute(
                "SELECT id FROM task_attempts WHERE worker_id=%s AND claim_request_id=%s",
                (request.worker_id, request.claim_request_id),
            ).fetchone()
            if previous:
                worker, previous_task, previous_attempt = self._attempt(c, previous["id"], request.worker_id)
                self._valid(worker, previous_task, previous_attempt, db_now(c), {"ASSIGNED"})
                return self._assignment(previous_task, previous_attempt), "duplicate"
            self._session(worker, db_now(c))
            # A NEW statement after acquiring Worker sees prior commits under READ COMMITTED.
            occupied = required_row(
                c.execute(
                    "SELECT count(*) AS n FROM task_attempts WHERE worker_id=%s AND status IN ('ASSIGNED','RUNNING')",
                    (worker["id"],),
                ).fetchone()
            )["n"]
            if occupied >= worker["capacity"]:
                raise Conflict("CAPACITY_EXHAUSTED", "Reconcile local slots; do not register a new identity")
            task = select_task(c, worker["supported_operations"])
            if task is None:
                return None, "empty"
            # There is no existing Attempt to lock on creation. Job is the final existing-row lock.
            job = required_row(
                c.execute("SELECT * FROM jobs WHERE id=%s FOR UPDATE", (task["job_id"],)).fetchone()
            )
            if job["status"] not in ("QUEUED", "RUNNING"):
                raise Conflict("INVARIANT_VIOLATION", "Eligible task belongs to a terminal job", 503)
            # Capabilities are immutable for this worker session. Recheck under the
            # final Job lock as a defense against inconsistent persisted payloads.
            if (
                job["operation"] not in worker["supported_operations"]
                or task["payload"]["operation"] != job["operation"]
            ):
                raise Conflict("INVARIANT_VIOLATION", "Task operation is incompatible with worker/job", 503)
            now = db_now(c)
            self._session(worker, now)
            transition("task", task["status"], "ASSIGNED")
            attempt = required_row(
                c.execute(
                    """INSERT INTO task_attempts(id,task_id,worker_id,attempt_number,claim_request_id,
                status,assigned_at,lease_expires_at) VALUES (%s,%s,%s,%s,%s,'ASSIGNED',%s,%s) RETURNING *""",
                    (
                        uuid4(),
                        task["id"],
                        worker["id"],
                        task["attempt_count"] + 1,
                        request.claim_request_id,
                        now,
                        now + timedelta(milliseconds=self.settings.assignment_timeout_ms),
                    ),
                ).fetchone()
            )
            c.execute(
                "UPDATE tasks SET status='ASSIGNED',attempt_count=attempt_count+1 WHERE id=%s", (task["id"],)
            )
            if job["status"] == "QUEUED":
                transition("job", job["status"], "RUNNING")
                c.execute("UPDATE jobs SET status='RUNNING',started_at=%s WHERE id=%s", (now, job["id"]))
            self.hook("claim_before_commit", c)
            return self._assignment(task, attempt), "accepted"

        result, outcome = self.db.run(transaction)
        self.metrics.operations.labels("claim", outcome).inc()
        if result:
            if outcome == "accepted":
                self.metrics.assignments.labels(result["payload"]["operation"]).inc()
            event(
                "claim_" + outcome,
                worker_id=request.worker_id,
                job_id=result["jobId"],
                task_id=result["taskId"],
                attempt_id=result["attemptId"],
                operation=result["payload"]["operation"],
                new_state="ASSIGNED",
            )
        return result
