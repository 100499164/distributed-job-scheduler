"""Exercise an actual 001 installation, data preservation, checksums and upgrade."""

import hashlib
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import make_conninfo

from scheduler.config import Settings
from scheduler.control_plane.jobs import Jobs
from scheduler.persistence import database
from scheduler.protocol.models import CreateJob


@pytest.mark.integration
def test_upgrade_original_schema_with_existing_job(dsn, tmp_path, monkeypatch):
    schema = "migration_" + uuid4().hex
    with psycopg.connect(dsn, autocommit=True) as connection:
        connection.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
    isolated = make_conninfo(dsn, options=f"-c search_path={schema}")
    migrations = database.MIGRATIONS
    original = (migrations / "001_initial.sql").read_bytes()
    assert (
        hashlib.sha256(original).hexdigest()
        == "cc41456ebc1374f87f2451363114773c578e50f0622aaebff8ae6b1a46da2983"
    )
    (tmp_path / "001_initial.sql").write_bytes(original)
    db = None
    try:
        monkeypatch.setattr(database, "MIGRATIONS", tmp_path)
        database.migrate(isolated)
        db = database.Database(isolated)
        jobs = Jobs(db, Settings())
        request = CreateJob(name="existing", taskCount=2, payload={"fromInclusive": 2, "toExclusive": 100})
        job, _ = jobs.create(request, "existing")
        tasks = jobs.list_tasks(job["id"], 200)
        before = db.run(lambda c: c.execute("SELECT * FROM schema_migrations").fetchall())
        with pytest.raises(psycopg.errors.CheckViolation):
            db.run(lambda c: c.execute("UPDATE jobs SET operation='RANGE_SUM' WHERE id=%s", (job["id"],)))
        monkeypatch.setattr(database, "MIGRATIONS", migrations)
        database.migrate(isolated)
        database.migrate(isolated)
        assert db.ready()
        assert jobs.create(request, "existing") == (job, False)
        assert jobs.list_tasks(job["id"], 200) == tasks
        assert (
            db.run(
                lambda c: c.execute(
                    "SELECT * FROM schema_migrations WHERE version='001_initial.sql'"
                ).fetchall()
            )
            == before
        )
        for payload in (
            {"operation": "RANGE_SUM", "fromInclusive": 1, "toExclusive": 10},
            {"operation": "MONTE_CARLO_PI", "samples": 10, "seed": 0},
        ):
            jobs.create(CreateJob(name="new", taskCount=2, payload=payload), payload["operation"])
        with pytest.raises(psycopg.errors.CheckViolation):
            db.run(lambda c: c.execute("UPDATE jobs SET operation='UNKNOWN' WHERE id=%s", (job["id"],)))
    finally:
        if db:
            db.close()
        with psycopg.connect(dsn, autocommit=True) as connection:
            connection.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))


@pytest.mark.integration
def test_upgrade_002_preserves_existing_worker_and_active_assignment(dsn, tmp_path, monkeypatch):
    from scheduler.control_plane.scheduling import Scheduler
    from scheduler.protocol.models import KNOWN_OPERATIONS, Completion, Register
    from scheduler.worker.workload import execute

    schema = "migration_" + uuid4().hex
    with psycopg.connect(dsn, autocommit=True) as connection:
        connection.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
    isolated = make_conninfo(dsn, options=f"-c search_path={schema}")
    migrations = database.MIGRATIONS
    for name in ("001_initial.sql", "002_workloads.sql"):
        (tmp_path / name).write_bytes((migrations / name).read_bytes())
    assert (
        hashlib.sha256((migrations / "002_workloads.sql").read_bytes()).hexdigest()
        == "cddb5d54f0d9c20db15ca04ddba7222ba09db36714ea95bd023025556d215bc7"
    )
    db = None
    try:
        monkeypatch.setattr(database, "MIGRATIONS", tmp_path)
        database.migrate(isolated)
        db = database.Database(isolated)
        jobs = Jobs(db, Settings())
        job, _ = jobs.create(
            CreateJob(
                name="old", taskCount=1, payload={"operation": "MONTE_CARLO_PI", "samples": 1000, "seed": 2}
            ),
            "old",
        )
        task = jobs.list_tasks(job["id"], 200)["items"][0]
        worker, attempt = uuid4(), uuid4()

        def old_assignment(c):
            c.execute(
                "INSERT INTO workers(id,hostname,version,status,capacity) VALUES (%s,'legacy','1','ONLINE',1)",
                (worker,),
            )
            c.execute(
                """INSERT INTO task_attempts(id,task_id,worker_id,attempt_number,claim_request_id,status,lease_expires_at)
                         VALUES (%s,%s,%s,1,%s,'ASSIGNED',clock_timestamp()+interval '60 seconds')""",
                (attempt, task["id"], worker, uuid4()),
            )
            c.execute("UPDATE tasks SET status='ASSIGNED',attempt_count=1 WHERE id=%s", (task["id"],))
            c.execute(
                "UPDATE jobs SET status='RUNNING',started_at=clock_timestamp() WHERE id=%s", (job["id"],)
            )

        db.run(old_assignment)
        before_task, before_attempts = jobs.task(task["id"]), jobs.attempts(task["id"])
        before_migrations = db.run(
            lambda c: c.execute("SELECT * FROM schema_migrations ORDER BY version").fetchall()
        )
        monkeypatch.setattr(database, "MIGRATIONS", migrations)
        database.migrate(isolated)
        database.migrate(isolated)
        assert db.ready()
        assert jobs.task(task["id"]) == before_task
        assert jobs.attempts(task["id"]) == before_attempts
        assert jobs.worker(worker)["supportedOperations"] == list(KNOWN_OPERATIONS)
        assert (
            db.run(
                lambda c: c.execute(
                    "SELECT * FROM schema_migrations WHERE version<'003' ORDER BY version"
                ).fetchall()
            )
            == before_migrations
        )
        scheduler = Scheduler(db, Settings())
        scheduler.register(Register(workerId=worker, hostname="legacy", version="1", capacity=1))
        scheduler.start(attempt, worker)
        scheduler.complete(
            attempt, Completion(workerId=worker, outcome="SUCCEEDED", result=execute(task["payload"]))
        )
        assert jobs.job(job["id"])["status"] == "COMPLETED"
    finally:
        if db:
            db.close()
        with psycopg.connect(dsn, autocommit=True) as connection:
            connection.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))
