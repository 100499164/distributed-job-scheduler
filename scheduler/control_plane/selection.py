"""Task selection with basic fairness between jobs."""

from psycopg import Connection

from scheduler.persistence.database import Row

# Claims can race here, so fairness is approximate rather than strict.
# Task rows are locked with SKIP LOCKED; job rows are locked later during the claim.
ELIGIBLE_JOBS = """SELECT j.id,
    (SELECT max(a.assigned_at) FROM tasks history
     JOIN task_attempts a ON a.task_id=history.id WHERE history.job_id=j.id) AS last_assigned
    FROM jobs j
    WHERE j.status IN ('QUEUED','RUNNING') AND j.operation=ANY(%s)
      AND EXISTS (SELECT 1 FROM tasks eligible WHERE eligible.job_id=j.id
                  AND eligible.status IN ('QUEUED','RETRY_WAIT')
                  AND eligible.available_at<=statement_timestamp())
    ORDER BY last_assigned NULLS FIRST,j.created_at,j.id"""

ELIGIBLE_TASK = """SELECT * FROM tasks WHERE job_id=%s
    AND status IN ('QUEUED','RETRY_WAIT') AND available_at<=clock_timestamp()
    ORDER BY available_at,created_at,id LIMIT 1 FOR UPDATE SKIP LOCKED"""


def select_task(connection: Connection[Row], supported_operations: list[str]) -> Row | None:
    """Pick an available task from the least recently served compatible job."""
    jobs = connection.execute(ELIGIBLE_JOBS, (supported_operations,)).fetchall()
    for job in jobs:
        task = connection.execute(ELIGIBLE_TASK, (job["id"],)).fetchone()
        if task is not None:
            return task
    return None
