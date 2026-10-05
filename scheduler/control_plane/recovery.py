import random
import time
from threading import Event
from uuid import UUID

from psycopg import Connection

from scheduler.control_plane.domain import (
    retry_state,
    session_valid,
    transition,
)
from scheduler.control_plane.scheduling import Scheduler, db_now
from scheduler.observability import event
from scheduler.persistence.database import Row


class Recovery:

    def __init__(
        self,
        scheduler: Scheduler,
        stop: Event | None = None,
    ) -> None:
        self.scheduler = scheduler
        self.stop = stop
        self.db = scheduler.db
        self.settings = scheduler.settings

    def offline(
        self,
        worker_id: UUID,
    ) -> bool:
        def transaction(
            c: Connection[Row],
        ) -> bool:
            # Lock and reload the worker before deciding whether it is dead.
            worker = self.scheduler._worker(
                c,
                worker_id,
            )

            now = db_now(c)

            # A worker is only marked offline once its heartbeat has really expired.
            if worker["status"] == "ONLINE" and not session_valid(
                worker,
                now,
                self.settings.worker_timeout_ms,
            ):
                transition(
                    "worker",
                    "ONLINE",
                    "OFFLINE",
                )

                c.execute(
                    """
                    UPDATE workers
                    SET status='OFFLINE', offline_at=%s
                    WHERE id=%s
                    """,
                    (
                        now,
                        worker_id,
                    ),
                )

                return True

            return False

        changed = self.db.run(transaction)

        if changed:
            event(
                "worker_lost",
                worker_id=worker_id,
                previous_state="ONLINE",
                new_state="OFFLINE",
                reason="WORKER_LOST",
            )

        return changed

    def expire(
        self,
        attempt_id: UUID,
    ) -> Row | None:
        # Jitter avoids every retry becoming ready at exactly the same time.
        jitter = random.uniform(
            0.8,
            1.2,
        )

        def transaction(
            c: Connection[Row],
        ) -> Row | None:
            # Reload the full ownership chain before expiring anything.
            worker, task, attempt = self.scheduler._attempt(
                c,
                attempt_id,
            )

            # Finished attempts are no longer recovery candidates.
            if attempt["status"] not in (
                "ASSIGNED",
                "RUNNING",
            ):
                return None

            job = self.scheduler._job(
                c,
                task["job_id"],
            )

            now = db_now(c)

            # Worker loss takes priority over lease/deadline expiration.
            if not session_valid(
                worker,
                now,
                self.settings.worker_timeout_ms,
            ):
                reason = "WORKER_LOST"

            elif (
                attempt["execution_deadline_at"] is not None
                and now >= attempt["execution_deadline_at"]
            ):
                reason = "EXECUTION_TIMEOUT"

            elif now >= attempt["lease_expires_at"]:
                reason = (
                    "ASSIGNMENT_TIMEOUT"
                    if attempt["status"] == "ASSIGNED"
                    else "LEASE_EXPIRED"
                )

            else:
                # The candidate may have been renewed while waiting for locks.
                return None

            # Active task and attempt states must always move together.
            if task["status"] != attempt["status"]:
                raise RuntimeError(
                    "Active task/attempt invariant violation"
                )

            transition(
                "attempt",
                attempt["status"],
                "EXPIRED",
            )

            # Decide whether this task should retry or fail permanently.
            state = retry_state(
                task["attempt_count"],
                task["max_retries"],
                reason,
            )

            c.execute(
                """
                UPDATE task_attempts
                SET status='EXPIRED',
                    finished_at=%s,
                    error_code=%s
                WHERE id=%s
                """,
                (
                    now,
                    reason,
                    attempt_id,
                ),
            )

            # Update the task and job consistently with the expired attempt.
            self.scheduler._finish_task(
                c,
                task,
                job,
                now,
                state,
                reason,
                jitter,
            )

            self.scheduler.hook(
                "recovery_before_commit",
                c,
            )

            return {
                "worker_id": worker["id"],
                "task_id": task["id"],
                "job_id": task["job_id"],
                "attempt_id": attempt_id,
                "operation": task["payload"]["operation"],
                "reason": reason,
                "previous_state": task["status"],
                "new_state": state,
            }

        result = self.db.run(transaction)

        if result:
            self.scheduler.metrics.expirations.labels(
                result["reason"]
            ).inc()

            if result["new_state"] == "RETRY_WAIT":
                self.scheduler.metrics.retries.inc()

            event(
                "attempt_expired",
                **result,
            )

        return result

    def sweep(self) -> None:
        started = time.monotonic()

        try:
            # First find workers whose heartbeat has expired.
            workers = self.db.run(
                lambda c: c.execute(
                    """
                    SELECT id
                    FROM workers
                    WHERE status='ONLINE'
                        AND last_heartbeat_at <=
                            clock_timestamp() - (%s * interval '1 millisecond')
                    ORDER BY last_heartbeat_at, id
                    LIMIT 200
                    """,
                    (
                        self.settings.worker_timeout_ms,
                    ),
                ).fetchall()
            )

            for worker in workers:
                if self.stop and self.stop.is_set():
                    return

                self.offline(
                    worker["id"]
                )

            # These are only candidate IDs. expire() revalidates everything
            # after taking the required locks.
            attempts = self.db.run(
                lambda c: c.execute(
                    """
                    SELECT a.id
                    FROM task_attempts a
                    JOIN workers w
                        ON w.id = a.worker_id
                    WHERE a.status IN ('ASSIGNED', 'RUNNING')
                        AND (
                            w.status = 'OFFLINE'
                            OR w.last_heartbeat_at <=
                                clock_timestamp() - (%s * interval '1 millisecond')
                            OR a.lease_expires_at <= clock_timestamp()
                            OR a.execution_deadline_at <= clock_timestamp()
                        )
                    ORDER BY a.lease_expires_at, a.id
                    LIMIT 200
                    """,
                    (
                        self.settings.worker_timeout_ms,
                    ),
                ).fetchall()
            )

            for attempt in attempts:
                if self.stop and self.stop.is_set():
                    return

                self.expire(
                    attempt["id"]
                )

            # Detect active tasks that somehow lost their active attempt.
            corrupt = self.db.run(
                lambda c: c.execute(
                    """
                    SELECT t.id
                    FROM tasks t
                    WHERE t.status IN ('ASSIGNED', 'RUNNING')
                        AND NOT EXISTS (
                            SELECT 1
                            FROM task_attempts a
                            WHERE a.task_id = t.id
                                AND a.status IN ('ASSIGNED', 'RUNNING')
                        )
                    LIMIT 20
                    """
                ).fetchall()
            )

            for row in corrupt:
                event(
                    "invariant_violation",
                    task_id=row["id"],
                    reason="ACTIVE_TASK_WITHOUT_ACTIVE_ATTEMPT",
                )

        finally:
            # Record sweep duration even if recovery exits early or fails.
            self.scheduler.metrics.sweep.observe(
                time.monotonic() - started
            )