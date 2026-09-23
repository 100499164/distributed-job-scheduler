import psycopg
import pytest

pytestmark = pytest.mark.integration


def test_schema_constraints_and_readiness(db, dsn):
    assert db.ready()
    from scheduler.persistence.database import migrate

    migrate(dsn)
    with pytest.raises(psycopg.errors.CheckViolation):
        db.run(
            lambda c: c.execute("""INSERT INTO workers(id,hostname,version,status,capacity)
            VALUES (gen_random_uuid(),'test','1','ONLINE',0)""")
        )
    assert (
        db.run(lambda c: c.execute("SHOW transaction_isolation").fetchone())["transaction_isolation"]
        == "read committed"
    )
    indexes = db.run(
        lambda c: c.execute("SELECT indexname FROM pg_indexes WHERE schemaname='public'").fetchall()
    )
    names = {r["indexname"] for r in indexes}
    assert {"attempts_one_active_per_task", "attempts_one_success_per_task", "tasks_eligible"} <= names
