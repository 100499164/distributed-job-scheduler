"""Inspect scheduling query plans in a temporary PostgreSQL database."""

import argparse
import json
import sys
from pathlib import Path
from uuid import uuid4

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from testcontainers.postgres import PostgresContainer

from scheduler.config import Settings
from scheduler.control_plane.jobs import Jobs
from scheduler.control_plane.scheduling import Scheduler
from scheduler.control_plane.selection import ELIGIBLE_JOBS, ELIGIBLE_TASK
from scheduler.persistence.database import Database, migrate
from scheduler.protocol.models import KNOWN_OPERATIONS, Claim, CreateJob, Register


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path("verification/scheduling-plans.json"))
    args = parser.parse_args()
    with PostgresContainer("postgres:17.6-alpine3.22") as postgres:
        dsn = postgres.get_connection_url().replace("postgresql+psycopg2://", "postgresql://")
        migrate(dsn)
        db = Database(dsn)
        try:
            jobs, scheduler = Jobs(db, Settings()), Scheduler(db, Settings())
            created = []
            for index in range(20):
                created.append(
                    jobs.create(
                        CreateJob(
                            name=f"explain-{index}",
                            taskCount=1000,
                            payload={"operation": "RANGE_SUM", "fromInclusive": 0, "toExclusive": 1000000},
                        ),
                        str(uuid4()),
                    )[0]
                )
            for _ in range(4):
                worker = uuid4()
                scheduler.register(Register(workerId=worker, hostname="explain", version="1", capacity=50))
                for _ in range(50):
                    scheduler.claim(Claim(workerId=worker, claimRequestId=uuid4()))
            db.run(lambda c: [c.execute("ANALYZE " + table) for table in ("jobs", "tasks", "task_attempts")])

            def plans(c):
                return {
                    name: [
                        next(iter(row.values()))
                        for row in c.execute("EXPLAIN (ANALYZE, BUFFERS) " + query, params)
                    ]
                    for name, query, params in [
                        ("rankJobs", ELIGIBLE_JOBS, (list(KNOWN_OPERATIONS),)),
                        ("lockTask", ELIGIBLE_TASK, (created[-1]["id"],)),
                    ]
                }

            result = {"fixture": {"jobs": 20, "tasks": 20000, "attempts": 200}, "withIndex": db.run(plans)}
            # Drop the index inside a rolled-back transaction for comparison.
            with db.pool.connection() as c:
                with c.transaction(force_rollback=True):
                    c.execute("DROP INDEX tasks_eligible_per_job")
                    result["withoutIndex"] = plans(c)
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(json.dumps(result, indent=2))
            print(json.dumps(result, indent=2))
        finally:
            db.close()


if __name__ == "__main__":
    main()
