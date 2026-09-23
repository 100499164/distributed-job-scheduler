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
    with psycopg.connect(dsn, connect_timeout=3) as connection:
        connection.execute("SELECT pg_advisory_xact_lock(730214001)")
        connection.execute("""CREATE TABLE IF NOT EXISTS schema_migrations (
            version TEXT PRIMARY KEY, checksum TEXT NOT NULL,
            applied_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp())""")
        for path in sorted(MIGRATIONS.glob("*.sql")):
            content = path.read_bytes()
            checksum = hashlib.sha256(content).hexdigest()
            row = connection.execute(
                "SELECT checksum FROM schema_migrations WHERE version=%s", (path.name,)
            ).fetchone()
            if row:
                if row[0] != checksum:
                    raise RuntimeError(f"Applied migration checksum mismatch: {path.name}")
                continue
            connection.execute(content.decode("utf-8"))
            connection.execute(
                "INSERT INTO schema_migrations(version,checksum) VALUES (%s,%s)", (path.name, checksum)
            )


class Database:
    def __init__(self, dsn: str, max_size: int = 16) -> None:
        self.pool: ConnectionPool[Connection[Row]] = ConnectionPool(
            dsn,
            min_size=0,
            max_size=max_size,
            timeout=3,
            kwargs={"row_factory": dict_row, "connect_timeout": 3, "autocommit": True},
            open=True,
        )

    @contextmanager
    def transaction(self) -> Iterator[Connection[Row]]:
        with self.pool.connection() as connection:
            with connection.transaction():
                connection.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
                connection.execute("SET LOCAL lock_timeout = '3s'")
                connection.execute("SET LOCAL statement_timeout = '10s'")
                yield connection

    def run(self, operation: Callable[[Connection[Row]], T]) -> T:
        attempt = 0
        while True:
            try:
                with self.transaction() as connection:
                    result = operation(connection)
                return result  # Only return after the transaction has committed.
            except (
                psycopg.errors.DeadlockDetected,
                psycopg.errors.SerializationFailure,
                psycopg.errors.LockNotAvailable,
            ):
                attempt += 1
                if attempt == 3:
                    raise
                time.sleep(random.uniform(0.01, 0.03) * attempt)

    def ready(self) -> bool:
        def check(connection: Connection[Row]) -> bool:
            rows = connection.execute("SELECT version,checksum FROM schema_migrations").fetchall()
            expected = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in MIGRATIONS.glob("*.sql")}
            if {r["version"]: r["checksum"] for r in rows} != expected:
                return False
            for name in ("jobs", "tasks", "workers", "task_attempts"):
                row = connection.execute(
                    """SELECT has_table_privilege(current_user,%s,'SELECT')
                    AND has_table_privilege(current_user,%s,'INSERT')
                    AND has_table_privilege(current_user,%s,'UPDATE') AS permitted""",
                    (name, name, name),
                ).fetchone()
                if row is None or not row["permitted"]:
                    return False
            return True

        return self.run(check)

    def close(self) -> None:
        self.pool.close()


def required_row(row: Row | None) -> Row:
    if row is None:
        raise RuntimeError("Required database row missing")
    return row
