import os

import pytest
from invariants import assert_invariants

from scheduler.persistence.database import Database, migrate


@pytest.fixture(scope="session")
def dsn():
    """Explicit test DB or Testcontainers; never silently skip missing infrastructure."""
    configured = os.environ.get("TEST_DATABASE_URL")

    if configured:
        yield configured
    else:
        from testcontainers.community.postgres import PostgresContainer

        with PostgresContainer("postgres:17.6-alpine3.22") as postgres:
            yield postgres.get_connection_url().replace(
                "postgresql+psycopg2://",
                "postgresql://",
            )


@pytest.fixture
def db(dsn):
    # Ensure every test runs against the current schema.
    migrate(dsn)

    database = Database(dsn)

    # Tests share the same database, so start each one from an empty state.
    database.run(lambda c: c.execute("TRUNCATE task_attempts,tasks,workers,jobs"))

    yield database

    try:
        # Catch state corruption even when the test itself appears to pass.
        assert_invariants(database)
    finally:
        database.close()
