"""Task selection with basic fairness between jobs."""

from psycopg import Connection

from scheduler.persistence.database import Row

# Claims can race, so fairness is best-effort rather than strict.
# Task rows are locked here; the job row is locked later during the claim.
ELIGIBLE_JOBS = """
SELECT
    j.id,
    (
        SELECT max(a.assigned_at)
        FROM tasks history
        JOIN task_attempts a
            ON a.task_id = history.id
        WHERE history.job_id = j.id
    ) AS last_assigned
FROM jobs j
WHERE j.status IN ('QUEUED', 'RUNNING')
    AND j.operation = ANY(%s)
    AND EXISTS (
        SELECT 1
        FROM tasks eligible
        WHERE eligible.job_id = j.id
            AND eligible.status IN ('QUEUED', 'RETRY_WAIT')
            AND eligible.available_at <= statement_timestamp()
    )
ORDER BY
    last_assigned NULLS FIRST,
    j.created_at,
    j.id
"""


# Pick the oldest available task and skip rows already claimed by another transaction.
ELIGIBLE_TASK = """
SELECT *
FROM tasks
WHERE job_id = %s
    AND status IN ('QUEUED', 'RETRY_WAIT')
    AND available_at <= clock_timestamp()
ORDER BY
    available_at,
    created_at,
    id
LIMIT 1
FOR UPDATE SKIP LOCKED
"""


def select_task(
    connection: Connection[Row],
    supported_operations: list[str],
) -> Row | None:
    """Pick an available task from the least recently served compatible job."""

    # Jobs that have never been served are considered first.
    jobs = connection.execute(
        ELIGIBLE_JOBS,
        (supported_operations,),
    ).fetchall()

    for job in jobs:
        # Another scheduler may already hold the first task, so try the next job
        # if SKIP LOCKED leaves this one with nothing available.
        task = connection.execute(
            ELIGIBLE_TASK,
            (job["id"],),
        ).fetchone()

        if task is not None:
            return task

    return None
