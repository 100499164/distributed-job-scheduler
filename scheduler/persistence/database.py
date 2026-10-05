"""Database connection, migrations and transaction helpers."""

import hashlib
import random
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any, TypeVar

import psycopg
from psycopg import Connection
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

Row = dict[str, Any]
T = TypeVar("T")
Hook = Callable[[str, Connection[Row] | None], None]

MIGRATIONS = Path(__file__).with_name("migrations")


def migrate(dsn: str) -> None:
    # Prevent two processes from applying migrations at the same time.
    with psycopg.connect(
        dsn,
        connect_timeout=3,
    ) as connection:
        connection.execute("SELECT pg_advisory_xact_lock(730214001)")

        # Keep a checksum for every applied migration.
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version TEXT PRIMARY KEY,
                checksum TEXT NOT NULL,
                applied_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
            )
            """
        )

        # File names define migration order.
        for path in sorted(MIGRATIONS.glob("*.sql")):
            content = path.read_bytes()
            checksum = hashlib.sha256(content).hexdigest()

            row = connection.execute(
                """
                SELECT checksum
                FROM schema_migrations
                WHERE version=%s
                """,
                (path.name,),
            ).fetchone()

            if row:
                # Applied migrations must never change afterwards.
                if row[0] != checksum:
                    raise RuntimeError(f"Applied migration checksum mismatch: {path.name}")

                continue

            connection.execute(content.decode("utf-8"))

            connection.execute(
                """
                INSERT INTO schema_migrations(
                    version,
                    checksum
                )
                VALUES (%s,%s)
                """,
                (
                    path.name,
                    checksum,
                ),
            )


class Database:
    def __init__(
        self,
        dsn: str,
        max_size: int = 16,
    ) -> None:
        self.pool: ConnectionPool[Connection[Row]] = ConnectionPool(
            dsn,
            min_size=0,
            max_size=max_size,
            timeout=3,
            kwargs={
                "row_factory": dict_row,
                "connect_timeout": 3,
                "autocommit": True,
            },
            open=True,
        )

    @contextmanager
    def transaction(
        self,
    ) -> Iterator[Connection[Row]]:
        with self.pool.connection() as connection:
            with connection.transaction():
                # READ COMMITTED is enough because row locks protect
                # the state transitions that require serialization.
                connection.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")

                # Avoid waiting indefinitely on contended locks.
                connection.execute("SET LOCAL lock_timeout = '3s'")

                # Bound individual transactions so stuck queries fail fast.
                connection.execute("SET LOCAL statement_timeout = '10s'")

                yield connection

    def run(
        self,
        operation: Callable[[Connection[Row]], T],
    ) -> T:
        attempt = 0

        while True:
            try:
                with self.transaction() as connection:
                    result = operation(connection)

                # Return only after the transaction has committed.
                return result

            except (
                psycopg.errors.DeadlockDetected,
                psycopg.errors.SerializationFailure,
                psycopg.errors.LockNotAvailable,
            ):
                attempt += 1

                # Retry transient concurrency failures a limited number of times.
                if attempt == 3:
                    raise

                # Small jitter prevents concurrent retries from lining up again.
                time.sleep(
                    random.uniform(
                        0.01,
                        0.03,
                    )
                    * attempt
                )

    def ready(self) -> bool:
        def check(
            connection: Connection[Row],
        ) -> bool:
            rows = connection.execute(
                """
                SELECT version,checksum
                FROM schema_migrations
                """
            ).fetchall()

            # Readiness requires the database schema to match local migrations.
            expected = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in MIGRATIONS.glob("*.sql")}

            if {r["version"]: r["checksum"] for r in rows} != expected:
                return False

            # The service must be able to read and modify every core table.
            for name in (
                "jobs",
                "tasks",
                "workers",
                "task_attempts",
            ):
                row = connection.execute(
                    """
                    SELECT
                        has_table_privilege(current_user,%s,'SELECT')
                        AND has_table_privilege(current_user,%s,'INSERT')
                        AND has_table_privilege(current_user,%s,'UPDATE')
                        AS permitted
                    """,
                    (
                        name,
                        name,
                        name,
                    ),
                ).fetchone()

                if row is None or not row["permitted"]:
                    return False

            return True

        return self.run(check)

    def close(self) -> None:
        self.pool.close()


def required_row(
    row: Row | None,
) -> Row:
    # Missing rows here indicate an internal invariant violation.
    if row is None:
        raise RuntimeError("Required database row missing")

    return row
