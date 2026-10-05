"""Failure boundaries must preserve diagnostics, protocol safety and bounded shutdown."""

import json
import subprocess
import sys
import textwrap
import time
from concurrent.futures import Future
from threading import Event
from uuid import uuid4

import psycopg
import pytest
from fastapi.testclient import TestClient

from scheduler.config import Settings
from scheduler.control_plane.app import create_app
from scheduler.control_plane.recovery import Recovery
from scheduler.observability import logger
from scheduler.persistence.database import migrate
from scheduler.protocol.models import Completion, PrimeCountResult
from scheduler.worker.runtime import Slot, Worker


@pytest.fixture
def records(caplog):
    logger.addHandler(caplog.handler)
    caplog.set_level(
        "INFO",
        logger="scheduler",
    )

    try:
        yield caplog
    finally:
        logger.removeHandler(caplog.handler)


def until(predicate):
    deadline = time.monotonic() + 5

    while time.monotonic() < deadline:
        if predicate():
            return

        time.sleep(0.01)

    raise AssertionError("Condition deadline exceeded")


def test_prime_result_rename_preserves_wire_and_completion():
    result = PrimeCountResult(prime_count=4)

    assert result.model_dump(by_alias=True) == {"primeCount": 4}

    completion = Completion.model_validate(
        {
            "workerId": str(uuid4()),
            "outcome": "SUCCEEDED",
            "result": {
                "primeCount": 4,
            },
        }
    )

    assert isinstance(
        completion.result,
        PrimeCountResult,
    )

    # Internal field renames must not change the public wire contract.
    assert completion.model_dump(
        mode="json",
        by_alias=True,
    )["result"] == {"primeCount": 4}


def test_executor_bug_is_logged_and_reported_as_permanent_failure(records):
    sent = []

    class Client:
        def post(self, path, body):
            sent.append(body)

    worker = Worker(
        "unused",
        client=Client(),
    )

    future = Future()

    try:
        try:
            raise LookupError("internal diagnostic detail")
        except LookupError as exc:
            future.set_exception(exc)

        slot = Slot(
            {
                "attemptId": "attempt",
                "taskId": "task",
                "payload": {},
            },
            future=future,
        )

        worker._advance(
            "attempt",
            slot,
        )

        # Internal failures become a stable protocol-level execution error.
        assert sent == [
            {
                "workerId": worker.id,
                "outcome": "FAILED",
                "error": {
                    "code": "EXECUTION_ERROR",
                    "message": "Workload execution failed",
                },
            }
        ]

        log = next(
            json.loads(record.message) for record in records.records if "execution_error" in record.message
        )

        # Diagnostics stay in logs, not in the worker protocol response.
        assert "Traceback (most recent call last)" in log["traceback"]

        assert "LookupError: internal diagnostic detail" in log["traceback"]

        # Keep identical completion content so a lost ACK can be retried safely.
        assert slot.completion == sent[0]

    finally:
        worker.executor.shutdown(wait=True)


def test_uncooperative_thread_causes_real_bounded_process_exit():
    # Never monkeypatch os._exit in the pytest process:
    # exercise the real process boundary.
    program = textwrap.dedent(
        """
        from threading import Event

        from scheduler.observability import configure_logs
        from scheduler.worker.runtime import Worker, Slot


        configure_logs("worker")

        entered = Event()


        def ignores_cancellation():
            entered.set()
            Event().wait()


        class StoppingWorker(Worker):
            def _register(self):
                return False


        worker = StoppingWorker("unused")

        future = worker.executor.submit(
            ignores_cancellation
        )

        assert entered.wait(2)

        worker.slots["hung"] = Slot(
            {"attemptId": "hung"},
            future=future,
        )

        worker.run()

        print("UNREACHABLE", flush=True)
        """
    )

    result = subprocess.run(
        [
            sys.executable,
            "-c",
            program,
        ],
        capture_output=True,
        text=True,
        timeout=15,
    )

    # A non-cooperative workload must not keep the process alive indefinitely.
    assert result.returncode == 2
    assert "uncooperative_workload" in result.stderr
    assert "UNREACHABLE" not in result.stdout


@pytest.mark.integration
def test_unexpected_http_error_logs_traceback_without_exposing_it(
    db,
    dsn,
    records,
):
    app = create_app(
        "api",
        dsn,
    )

    with TestClient(
        app,
        raise_server_exceptions=False,
    ) as client:
        until(lambda: client.get("/health/ready").status_code == 200)

        def broken_job(_):
            raise RuntimeError("private implementation detail")

        app.state.jobs.job = broken_job

        response = client.get("/v1/jobs/" + str(uuid4()))

        # Clients receive a stable generic error instead of internal details.
        assert response.status_code == 500
        assert response.json()["message"] == "Internal server error"

        assert response.headers["x-request-id"] == response.json()["requestId"]

        assert "private implementation detail" not in response.text

        # Full diagnostics are still available to operators.
        log = next(
            json.loads(record.message) for record in records.records if "internal_error" in record.message
        )

        assert "broken_job" in log["traceback"]

        assert "RuntimeError: private implementation detail" in log["traceback"]


@pytest.mark.integration
def test_recovery_bug_withdraws_readiness_and_stops_retrying(
    db,
    dsn,
    monkeypatch,
    records,
):
    release = Event()
    original = Recovery.sweep
    calls = []

    def broken_sweep(self):
        calls.append(1)

        if len(calls) == 1:
            return original(self)

        assert release.wait(5)

        raise RuntimeError("invariant failure")

    monkeypatch.setattr(
        Recovery,
        "sweep",
        broken_sweep,
    )

    app = create_app(
        "scheduler",
        dsn,
        Settings(recovery_interval_ms=20),
    )

    try:
        with TestClient(app) as client:
            # Initial recovery succeeds, so the scheduler becomes ready.
            until(lambda: client.get("/health/ready").status_code == 200)

            release.set()

            # A fatal recovery failure removes readiness but keeps liveness.
            until(lambda: client.get("/health/ready").status_code == 503)

            assert client.get("/health/live").status_code == 200

            # Scheduler operations are rejected while readiness is withdrawn.
            assert (
                client.post(
                    "/internal/v1/claims",
                    json={
                        "workerId": str(uuid4()),
                        "claimRequestId": str(uuid4()),
                    },
                ).status_code
                == 503
            )

            logs = [
                json.loads(record.message)
                for record in records.records
                if "recovery_fatal_error" in record.message
            ]

            assert len(logs) == 1

            assert "RuntimeError: invariant failure" in logs[0]["traceback"]

    finally:
        release.set()

    # Fatal recovery failures stop the retry loop instead of spinning forever.
    assert len(calls) == 2


@pytest.mark.integration
def test_checksum_failure_is_fatal_at_startup(
    db,
    dsn,
    records,
):
    original = db.run(
        lambda c: c.execute(
            """
            SELECT checksum
            FROM schema_migrations
            ORDER BY version
            LIMIT 1
            """
        ).fetchone()["checksum"]
    )

    try:
        db.run(
            lambda c: c.execute(
                """
                UPDATE schema_migrations
                SET checksum = %s
                WHERE version = '001_initial.sql'
                """,
                ("0" * 64,),
            )
        )

        # A modified historical migration is a fatal startup error.
        with pytest.raises(
            RuntimeError,
            match="checksum mismatch",
        ):
            migrate(dsn)

        app = create_app(
            "api",
            dsn,
        )

        with TestClient(app) as client:
            until(lambda: any("startup_fatal_error" in record.message for record in records.records))

            assert client.get("/health/ready").status_code == 503

            # Process liveness remains separate from application readiness.
            assert client.get("/health/live").status_code == 200

        assert sum("startup_fatal_error" in record.message for record in records.records) == 1

    finally:
        db.run(
            lambda c: c.execute(
                """
                UPDATE schema_migrations
                SET checksum = %s
                WHERE version = '001_initial.sql'
                """,
                (original,),
            )
        )


@pytest.mark.integration
def test_database_lock_timeout_retries_are_bounded(db):
    calls = []

    with db.transaction() as blocker:
        blocker.execute("LOCK TABLE workers IN ACCESS EXCLUSIVE MODE")

        def blocked(c):
            calls.append(1)

            c.execute("SET LOCAL lock_timeout = '20ms'")

            c.execute("SELECT * FROM workers")

        # Lock contention is retried, but only a bounded number of times.
        with pytest.raises(psycopg.errors.LockNotAvailable):
            db.run(blocked)

    assert len(calls) == 3

    # Failed transactions must return clean connections to the pool.
    assert db.ready()


@pytest.mark.integration
def test_database_programming_error_is_not_retried(db):
    calls = []

    def invalid(c):
        calls.append(1)
        c.execute("SELECT 1/0")

    # Programming/data errors are not transient concurrency failures.
    with pytest.raises(psycopg.errors.DivisionByZero):
        db.run(invalid)

    assert len(calls) == 1
    assert db.ready()
