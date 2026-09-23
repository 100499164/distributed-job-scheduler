"""Measure task-selection latency as the number of queued jobs grows."""

import argparse
import json
import math
import os
import platform
import statistics
import sys
import time
from pathlib import Path
from typing import cast

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import psycopg
from psycopg.rows import dict_row
from testcontainers.postgres import PostgresContainer

from scheduler.control_plane.selection import ELIGIBLE_JOBS, select_task
from scheduler.persistence.database import Row, migrate


class CountedConnection:
    """Wrap a connection to count executed queries."""

    def __init__(self, connection):
        self.connection = connection
        self.queries = 0

    def execute(self, *args, **kwargs):
        self.queries += 1
        return self.connection.execute(*args, **kwargs)


def fixture(c, jobs, tasks_per_job, history):
    c.execute("TRUNCATE task_attempts,tasks,jobs,workers CASCADE")
    c.execute(
        """INSERT INTO jobs(id,name,operation,status,payload,task_count,idempotency_key,request_hash)
        SELECT md5('job-'||n)::uuid,'bench-'||n,'RANGE_SUM','QUEUED',
        jsonb_build_object('operation','RANGE_SUM','fromInclusive',0,'toExclusive',%s),
        %s,'bench-'||n,repeat('0',64) FROM generate_series(1,%s) n""",
        (tasks_per_job * 10, tasks_per_job, jobs),
    )
    c.execute(
        """INSERT INTO tasks(id,job_id,partition_index,status,payload,max_retries)
        SELECT md5(j.id::text||':'||p)::uuid,j.id,p,'QUEUED',
        jsonb_build_object('operation','RANGE_SUM','fromInclusive',p*10,'toExclusive',(p+1)*10),3
        FROM jobs j CROSS JOIN generate_series(0,%s-1) p""",
        (tasks_per_job,),
    )
    if history:
        # One canonical completed task per job gives ranking a real attempt history.
        c.execute("""INSERT INTO workers(id,hostname,version,status,capacity)
            VALUES (md5('worker')::uuid,'fixture','1','ONLINE',1)""")
        c.execute("""UPDATE jobs SET status='RUNNING',started_at=clock_timestamp(),completed_tasks=1""")
        c.execute("""UPDATE tasks SET status='COMPLETED',attempt_count=1,
            first_started_at=clock_timestamp(),finished_at=clock_timestamp() WHERE partition_index=0""")
        c.execute("""INSERT INTO task_attempts(id,task_id,worker_id,attempt_number,claim_request_id,status,
            assigned_at,started_at,finished_at,lease_expires_at,execution_deadline_at,result,completion_hash)
            SELECT md5(t.id::text||'attempt')::uuid,t.id,md5('worker')::uuid,1,t.id,'SUCCEEDED',
            t.first_started_at,t.first_started_at,t.finished_at,
            t.first_started_at+interval '30 seconds',t.first_started_at+interval '60 seconds',
            '{"rangeSum":45}'::jsonb,repeat('0',64) FROM tasks t WHERE partition_index=0""")
    for table in ("jobs", "tasks", "task_attempts"):
        c.execute("ANALYZE " + table)


def measure(c, blocker, jobs, samples, warmups, locked_jobs):
    with blocker.transaction(force_rollback=True):
        if locked_jobs:
            blocker.execute(
                "SELECT id FROM tasks WHERE job_id=ANY(%s) AND status='QUEUED' FOR UPDATE",
                ([j["id"] for j in jobs[:locked_jobs]],),
            ).fetchall()
        durations, queries = [], []
        for index in range(warmups + samples):
            with c.transaction(force_rollback=True):
                counted = CountedConnection(c)
                started = time.perf_counter_ns()
                # Adapter delegates the exact execute contract; never mocks results.
                selected = select_task(cast(psycopg.Connection[Row], counted), ["RANGE_SUM"])
                duration = (time.perf_counter_ns() - started) / 1_000_000
                assert selected is not None and selected["job_id"] == jobs[locked_jobs]["id"]
                assert counted.queries == locked_jobs + 2
            if index >= warmups:
                durations.append(duration)
                queries.append(counted.queries)
        return {
            "locked_jobs": locked_jobs,
            "samples": samples,
            "warmups": warmups,
            "p50_ms": statistics.median(durations),
            "p95_ms": sorted(durations)[math.ceil(0.95 * samples) - 1],
            "queries_per_selection": sorted(set(queries)),
            "samples_ms": durations,
        }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--jobs", type=int, nargs="+", default=[10, 100, 1000, 5000])
    parser.add_argument("--tasks-per-job", type=int, default=10)
    parser.add_argument("--samples", type=int, default=20)
    parser.add_argument("--warmups", type=int, default=3)
    parser.add_argument("--output", type=Path, default=Path("verification/selection-scaling.json"))
    args = parser.parse_args()
    if min(args.jobs) < 2 or args.tasks_per_job < 2 or args.samples < 2 or args.warmups < 0:
        parser.error("Require >=2 jobs, tasks/job and samples; warmups >=0")
    image = "postgres:17.6-alpine3.22"
    report = {
        "environment": {
            "python": platform.python_version(),
            "platform": platform.platform(),
            "cpu_count": os.cpu_count(),
            "postgres_image": image,
            "selector_clients": 1,
            "blocker_connections": 1,
            "executing_workers": 0,
        },
        "results": [],
    }
    with PostgresContainer(image) as postgres:
        dsn = postgres.get_connection_url().replace("postgresql+psycopg2://", "postgresql://")
        migrate(dsn)
        with (
            psycopg.connect(dsn, autocommit=True, row_factory=dict_row) as c,
            psycopg.connect(dsn, autocommit=True, row_factory=dict_row) as blocker,
        ):
            report["environment"]["postgres_version"] = c.execute("SELECT version() AS v").fetchone()["v"]
            report["environment"]["settings"] = {
                name: c.execute("SHOW " + name).fetchone()[name]
                for name in ("shared_buffers", "work_mem", "max_connections")
            }
            for history in (False, True):
                for count in args.jobs:
                    fixture(c, count, args.tasks_per_job, history)
                    ordered = c.execute(ELIGIBLE_JOBS, (["RANGE_SUM"],)).fetchall()
                    for locked in sorted({0, min(10, count - 1), count - 1}):
                        result = {
                            "jobs": count,
                            "tasks": count * args.tasks_per_job,
                            "historical_attempts": count if history else 0,
                            **measure(c, blocker, ordered, args.samples, args.warmups, locked),
                        }
                        report["results"].append(result)
                        print(json.dumps({k: v for k, v in result.items() if k != "samples_ms"}), flush=True)
                        args.output.parent.mkdir(parents=True, exist_ok=True)
                        args.output.write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
