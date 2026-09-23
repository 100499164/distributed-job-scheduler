import contextvars
import json
import logging
import os
import traceback
from datetime import datetime, timezone

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram

from scheduler.persistence.database import Database

request_id: contextvars.ContextVar[str | None] = contextvars.ContextVar("request_id", default=None)
logger = logging.getLogger("scheduler")


def configure_logs(component: str) -> None:
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    os.environ["LOG_COMPONENT"] = component


def event(name: str, *, level: int = logging.INFO, **fields: object) -> None:
    logger.log(
        level,
        json.dumps(
            {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "component": os.getenv("LOG_COMPONENT", "test"),
                "event": name,
                "request_id": request_id.get(),
                **fields,
            },
            default=str,
            separators=(",", ":"),
        ),
    )


def exception_event(name: str, exc: Exception, **fields: object) -> None:
    """Log the full exception without exposing it in API responses."""
    event(
        name,
        level=logging.ERROR,
        reason=type(exc).__name__,
        traceback="".join(traceback.format_exception(exc)),
        **fields,
    )


class Metrics:
    def __init__(self) -> None:
        self.registry = CollectorRegistry()
        self.operations = Counter(
            "scheduler_operations_total",
            "Committed protocol operations",
            ["operation", "outcome"],
            registry=self.registry,
        )
        self.assignments = Counter(
            "scheduler_assignments_total",
            "New committed assignments by workload",
            ["operation"],
            registry=self.registry,
        )
        self.latency = Histogram(
            "scheduler_http_seconds", "HTTP request duration", ["route", "method"], registry=self.registry
        )
        self.expirations = Counter(
            "scheduler_expirations_total", "Expired attempts", ["reason"], registry=self.registry
        )
        self.retries = Counter("scheduler_retries_total", "Scheduled retries", registry=self.registry)
        self.db_errors = Counter("scheduler_database_errors_total", "Database errors", registry=self.registry)
        self.sweep = Histogram(
            "scheduler_recovery_seconds", "Recovery sweep duration", registry=self.registry
        )
        self.tasks = Gauge("scheduler_tasks", "Persisted tasks", ["state"], registry=self.registry)
        self.workers = Gauge("scheduler_workers_online", "Persisted online workers", registry=self.registry)
        self.slots = Gauge("scheduler_slots", "Worker slots", ["kind"], registry=self.registry)
        self.oldest = Gauge(
            "scheduler_oldest_eligible_seconds", "Oldest eligible task age", registry=self.registry
        )
        self.pool = Gauge(
            "scheduler_database_connections", "Connection pool", ["kind"], registry=self.registry
        )

    def refresh(self, db: Database) -> None:
        def read(c):
            tasks = c.execute("SELECT status,count(*) AS n FROM tasks GROUP BY status").fetchall()
            workers = c.execute(
                "SELECT count(*) AS n,coalesce(sum(capacity),0) AS capacity FROM workers WHERE status='ONLINE'"
            ).fetchone()
            occupied = c.execute(
                "SELECT count(*) AS n FROM task_attempts WHERE status IN ('ASSIGNED','RUNNING')"
            ).fetchone()["n"]
            age = c.execute("""SELECT coalesce(extract(epoch FROM clock_timestamp()-min(created_at)),0) AS age
                FROM tasks WHERE status IN ('QUEUED','RETRY_WAIT') AND available_at<=clock_timestamp()""").fetchone()[
                "age"
            ]
            return tasks, workers, occupied, age

        tasks, workers, occupied, age = db.run(read)
        counts = {r["status"]: r["n"] for r in tasks}
        for state in ("QUEUED", "ASSIGNED", "RUNNING", "RETRY_WAIT", "COMPLETED", "FAILED"):
            self.tasks.labels(state).set(counts.get(state, 0))
        self.workers.set(workers["n"])
        self.slots.labels("configured_online").set(workers["capacity"])
        self.slots.labels("occupied").set(occupied)
        self.oldest.set(float(age))
        for key in ("pool_size", "pool_available", "requests_waiting"):
            self.pool.labels(key).set(db.pool.get_stats().get(key, 0))
