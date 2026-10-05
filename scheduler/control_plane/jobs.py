import base64
import json
from collections.abc import Callable, Sequence
from datetime import datetime
from uuid import UUID, uuid4

from psycopg import Connection
from psycopg.types.json import Jsonb

from scheduler.config import Settings
from scheduler.control_plane.domain import Conflict, fingerprint
from scheduler.persistence.database import Database, Hook, Row
from scheduler.protocol.models import CreateJob, camel
from scheduler.workloads.catalog import partitions, workload


def wire(row: Row) -> Row:
    row = dict(row)

    if "_task_results" in row:
        results = row.pop("_task_results")

        # Only reduce task results once the whole job has completed.
        row["result"] = workload(row["operation"]).reduce(results) if row["status"] == "COMPLETED" else None

    # API responses use camelCase while the database keeps snake_case.
    return {camel(k): v for k, v in row.items()}


def encode_cursor(
    kind: str,
    values: Sequence[object],
) -> str:
    # Include the cursor kind so it cannot be reused by another endpoint.
    return base64.urlsafe_b64encode(
        json.dumps(
            [kind, *map(str, values)],
            separators=(",", ":"),
        ).encode()
    ).decode()


def decode_cursor(
    cursor: str | None,
    kind: str,
    types: Sequence[Callable[[str], datetime | UUID | int]],
) -> tuple[datetime | UUID | int, ...] | None:
    if not cursor:
        return None

    try:
        # Keep cursors bounded before decoding arbitrary input.
        if len(cursor) > 1024:
            raise ValueError()

        value = json.loads(
            base64.b64decode(
                cursor,
                altchars=b"-_",
                validate=True,
            )
        )

        # The cursor must belong to this query and contain the expected values.
        if value[0] != kind or len(value) != len(types) + 1:
            raise ValueError()

        return tuple(convert(item) for convert, item in zip(types, value[1:]))

    except (
        ValueError,
        TypeError,
        IndexError,
        KeyError,
    ):
        raise Conflict(
            "INVALID_CURSOR",
            "Invalid pagination cursor",
            400,
        ) from None


# Job view including the reduced result once the job is complete.
JOB_SELECT = """
SELECT
    j.id,
    j.name,
    j.operation,
    j.status,
    j.task_count,
    j.completed_tasks,
    j.failed_tasks,
    j.created_at,
    j.started_at,
    j.finished_at,
    CASE
        WHEN j.status = 'COMPLETED' THEN (
            SELECT jsonb_agg(a.result)
            FROM tasks t
            JOIN task_attempts a
                ON a.task_id = t.id
                AND a.status = 'SUCCEEDED'
            WHERE t.job_id = j.id
                AND t.status = 'COMPLETED'
        )
        ELSE NULL
    END AS _task_results
FROM jobs j
"""


# Task view including retry count and successful result.
TASK_SELECT = """
SELECT
    t.*,
    greatest(0, t.attempt_count - 1) AS retry_count,
    (
        SELECT a.result
        FROM task_attempts a
        WHERE a.task_id = t.id
            AND a.status = 'SUCCEEDED'
    ) AS result
FROM tasks t
"""


# Worker view with its current slot usage.
WORKER_SELECT = """
SELECT
    w.*,
    count(a.id)::int AS occupied_slots,
    w.capacity - count(a.id)::int AS available_capacity,
    count(a.id) = w.capacity AS busy
FROM workers w
LEFT JOIN task_attempts a
    ON a.worker_id = w.id
    AND a.status IN ('ASSIGNED', 'RUNNING')
"""


class Jobs:
    def __init__(
        self,
        db: Database,
        settings: Settings,
        hook: Hook = lambda name, connection: None,
    ) -> None:
        self.db = db
        self.settings = settings
        self.hook = hook

    def create(
        self,
        request: CreateJob,
        key: str | None,
    ) -> tuple[Row, bool]:
        if not key or not key.strip() or len(key.encode()) > 1024:
            raise Conflict(
                "INVALID_IDEMPOTENCY_KEY",
                "Idempotency-Key is required (up to 1024 bytes)",
                400,
            )

        if request.task_count > self.settings.max_tasks:
            raise Conflict(
                "TASK_LIMIT",
                "Too many tasks",
                400,
            )

        # Use the configured default when the client does not set a retry budget.
        retries = (
            request.max_retries if request.max_retries is not None else self.settings.default_max_retries
        )

        # Normalize the request before hashing it for idempotency checks.
        normalized = request.model_dump(
            by_alias=True,
            mode="json",
        )

        normalized["maxRetries"] = retries

        # Detect reuse of the same key with different request content.
        digest = fingerprint(normalized)

        job_id = uuid4()

        # Split the job into task payloads before entering the transaction.
        rows = [
            (uuid4(), index, payload)
            for index, payload in partitions(
                request.payload,
                request.task_count,
            )
        ]

        def transaction(
            c: Connection[Row],
        ) -> tuple[Row, bool]:
            # PostgreSQL arbitrates concurrent requests using the same key.
            inserted = c.execute(
                """
                INSERT INTO jobs(
                    id,
                    name,
                    operation,
                    status,
                    payload,
                    task_count,
                    idempotency_key,
                    request_hash
                )
                VALUES (%s, %s, %s, 'QUEUED', %s, %s, %s, %s)
                ON CONFLICT (idempotency_key)
                DO NOTHING
                RETURNING id
                """,
                (
                    job_id,
                    request.name,
                    request.payload.operation,
                    Jsonb(normalized["payload"]),
                    request.task_count,
                    key,
                    digest,
                ),
            ).fetchone()

            if not inserted:
                # The key already exists, so load the original job.
                existing = c.execute(
                    """
                    SELECT *
                    FROM jobs
                    WHERE idempotency_key = %s
                    """,
                    (key,),
                ).fetchone()

                assert existing is not None

                # Same key is only valid when the request itself is identical.
                if existing["request_hash"] != digest:
                    raise Conflict(
                        "IDEMPOTENCY_CONFLICT",
                        "Key already used with different content",
                    )

                return self._created(existing), False

            # Tasks are inserted in the same transaction as the job.
            with c.cursor() as cursor:
                cursor.executemany(
                    """
                    INSERT INTO tasks(
                        id,
                        job_id,
                        partition_index,
                        status,
                        payload,
                        max_retries
                    )
                    VALUES (%s, %s, %s, 'QUEUED', %s, %s)
                    """,
                    [
                        (
                            task_id,
                            job_id,
                            index,
                            Jsonb(payload),
                            retries,
                        )
                        for task_id, index, payload in rows
                    ],
                )

            self.hook(
                "create_before_commit",
                c,
            )

            return {
                "id": job_id,
                "status": "QUEUED",
                "taskCount": request.task_count,
                "completedTasks": 0,
                "failedTasks": 0,
            }, True

        return self.db.run(transaction)

    @staticmethod
    def _created(row: Row) -> Row:
        return wire(
            {
                k: row[k]
                for k in (
                    "id",
                    "status",
                    "task_count",
                    "completed_tasks",
                    "failed_tasks",
                )
            }
        )

    def job(
        self,
        job_id: UUID,
    ) -> Row:
        return self._one(
            JOB_SELECT + " WHERE j.id=%s",
            (job_id,),
        )

    def task(
        self,
        task_id: UUID,
    ) -> Row:
        return self._one(
            TASK_SELECT + " WHERE t.id=%s",
            (task_id,),
        )

    def worker(
        self,
        worker_id: UUID,
    ) -> Row:
        return self._one(
            WORKER_SELECT + " WHERE w.id=%s GROUP BY w.id",
            (worker_id,),
        )

    def _one(
        self,
        query: str,
        params: Sequence[object],
    ) -> Row:
        row = self.db.run(
            lambda c: c.execute(
                query,
                params,
            ).fetchone()
        )

        if row is None:
            raise Conflict(
                "NOT_FOUND",
                "Identity not found",
                404,
            )

        return wire(row)

    def list_jobs(
        self,
        limit: int,
        cursor: str | None = None,
        status: str | None = None,
    ) -> Row:
        # Include the filter in the cursor kind so cursors stay query-specific.
        kind = "jobs:" + (status or "all")

        after = decode_cursor(
            cursor,
            kind,
            (
                datetime.fromisoformat,
                UUID,
            ),
        )

        # Build filters separately so status and pagination can be combined.
        conditions: list[str] = []
        params: list[object] = []

        if status:
            conditions.append("j.status=%s")
            params.append(status)

        if after:
            # Cursor timestamps must be timezone-aware to keep ordering clear.
            if not isinstance(after[0], datetime) or after[0].tzinfo is None:
                raise Conflict(
                    "INVALID_CURSOR",
                    "Cursor time requires timezone",
                    400,
                )

            conditions.append("(j.created_at,j.id)>(%s,%s)")

            params.extend(after)

        query = JOB_SELECT + (" WHERE " + " AND ".join(conditions) if conditions else "")

        return self._page(
            query + " ORDER BY j.created_at,j.id LIMIT %s",
            (*params, limit + 1),
            limit,
            kind,
            (
                "created_at",
                "id",
            ),
        )

    def list_tasks(
        self,
        job_id: UUID,
        limit: int,
        cursor: str | None = None,
    ) -> Row:
        # This also verifies that the job exists.
        self.job(job_id)

        kind = "tasks:" + str(job_id)

        after = decode_cursor(
            cursor,
            kind,
            (
                int,
                UUID,
            ),
        )

        query = TASK_SELECT + " WHERE t.job_id=%s"

        params: list[object] = [job_id]

        if after:
            query += " AND (t.partition_index,t.id)>(%s,%s)"
            params.extend(after)

        return self._page(
            query + " ORDER BY t.partition_index,t.id LIMIT %s",
            (*params, limit + 1),
            limit,
            kind,
            (
                "partition_index",
                "id",
            ),
        )

    def attempts(
        self,
        task_id: UUID,
    ) -> Row:
        # This also verifies that the task exists.
        self.task(task_id)

        # Attempts are returned in execution order.
        rows = self.db.run(
            lambda c: c.execute(
                """
                SELECT
                    id,
                    task_id,
                    worker_id,
                    attempt_number,
                    claim_request_id,
                    status,
                    assigned_at,
                    started_at,
                    finished_at,
                    lease_expires_at,
                    execution_deadline_at,
                    error_code,
                    error_message,
                    result
                FROM task_attempts
                WHERE task_id=%s
                ORDER BY attempt_number
                """,
                (task_id,),
            ).fetchall()
        )

        return {
            "items": list(map(wire, rows)),
            "nextCursor": None,
        }

    def list_workers(
        self,
        limit: int,
        cursor: str | None = None,
    ) -> Row:
        after = decode_cursor(
            cursor,
            "workers",
            (UUID,),
        )

        return self._page(
            WORKER_SELECT + (" WHERE w.id>%s" if after else "") + " GROUP BY w.id ORDER BY w.id LIMIT %s",
            (
                *(after or ()),
                limit + 1,
            ),
            limit,
            "workers",
            ("id",),
        )

    def _page(
        self,
        query: str,
        params: Sequence[object],
        limit: int,
        kind: str,
        keys: Sequence[str],
    ) -> Row:
        # Fetch one extra row to know whether another page exists.
        rows = self.db.run(
            lambda c: c.execute(
                query,
                params,
            ).fetchall()
        )

        # The cursor points to the last row actually returned to the client.
        next_cursor = (
            encode_cursor(
                kind,
                [rows[limit - 1][k] for k in keys],
            )
            if len(rows) > limit
            else None
        )

        return {
            "items": list(
                map(
                    wire,
                    rows[:limit],
                )
            ),
            "nextCursor": next_cursor,
        }
