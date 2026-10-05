import asyncio
import json
import os
import threading
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Annotated, Literal
from uuid import UUID, uuid4

import psycopg
from fastapi import FastAPI, Header, Query, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from psycopg_pool import PoolTimeout
from starlette.exceptions import HTTPException

from scheduler.config import Settings
from scheduler.control_plane.domain import Conflict
from scheduler.control_plane.jobs import Jobs
from scheduler.control_plane.recovery import Recovery
from scheduler.control_plane.scheduling import Scheduler
from scheduler.observability import (
    Metrics,
    configure_logs,
    event,
    exception_event,
    request_id,
)
from scheduler.persistence.database import Database, migrate
from scheduler.protocol.models import (
    Claim,
    Completion,
    CreateJob,
    Heartbeat,
    Owner,
    Register,
)


def error_response(code, message, status, identifier=None):
    # Reuse the current request ID so errors can be matched with their logs.
    identifier = identifier or request_id.get() or str(uuid4())

    return JSONResponse(
        {
            "code": code,
            "message": message,
            "requestId": identifier,
        },
        status_code=status,
        headers={"X-Request-Id": identifier},
    )


class ProtocolMiddleware:

    def __init__(self, app, limit, metrics):
        self.app = app
        self.limit = limit
        self.metrics = metrics

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)

        # Give each request an ID to correlate responses, metrics and logs.
        token = request_id.set(str(uuid4()))
        scope.setdefault("state", {})["request_id"] = request_id.get()

        started = time.monotonic()

        async def send_with_id(message):
            # Add the request ID unless the response already has one.
            if message["type"] == "http.response.start":
                has_request_id = any(
                    name.lower() == b"x-request-id"
                    for name, _ in message["headers"]
                )

                if not has_request_id:
                    message["headers"].append(
                        (
                            b"x-request-id",
                            (request_id.get() or "").encode(),
                        )
                    )

            await send(message)

        try:
            # Read the body here so we can validate it before FastAPI handles it.
            body = bytearray()

            while True:
                message = await receive()

                if message["type"] == "http.disconnect":
                    return

                body.extend(message.get("body", b""))

                if len(body) > self.limit:
                    return await error_response(
                        "BODY_TOO_LARGE",
                        "Request exceeds body limit",
                        413,
                    )(
                        scope,
                        receive,
                        send_with_id,
                    )

                if not message.get("more_body", False):
                    break

            if body:

                def unique(pairs):
                    # Reject duplicate keys instead of silently keeping one.
                    result = {}

                    for key, value in pairs:
                        if key in result:
                            raise ValueError("Duplicate JSON key")

                        result[key] = value

                    return result

                def invalid_constant(value):
                    # Reject values such as NaN or Infinity.
                    raise ValueError("Nonfinite number")

                try:
                    json.loads(
                        body,
                        object_pairs_hook=unique,
                        parse_constant=invalid_constant,
                    )

                except (ValueError, UnicodeError):
                    return await error_response(
                        "INVALID_JSON",
                        "Malformed or ambiguous JSON",
                        400,
                    )(
                        scope,
                        receive,
                        send_with_id,
                    )

            # We consumed the body, so replay it once for FastAPI.
            delivered = False

            async def replay():
                nonlocal delivered

                if not delivered:
                    delivered = True

                    return {
                        "type": "http.request",
                        "body": bytes(body),
                        "more_body": False,
                    }

                return await receive()

            await self.app(
                scope,
                replay,
                send_with_id,
            )

        finally:
            route = getattr(
                scope.get("route"),
                "path",
                "unmatched",
            )

            self.metrics.latency.labels(
                route,
                scope["method"],
            ).observe(
                time.monotonic() - started
            )

            request_id.reset(token)


def create_app(role=None, dsn=None, settings=None):
    role = role or os.getenv("ROLE", "api")

    if role not in {"api", "scheduler"}:
        raise ValueError("ROLE must be api or scheduler")

    dsn = dsn or os.environ["DATABASE_URL"]

    # Support passwords mounted as files instead of keeping them in the DSN.
    if os.getenv("DATABASE_PASSWORD_FILE"):
        from psycopg.conninfo import make_conninfo

        password = Path(
            os.environ["DATABASE_PASSWORD_FILE"]
        ).read_text().strip()

        dsn = make_conninfo(
            dsn,
            password=password,
        )

    settings = settings or Settings.from_env()

    configure_logs(role)

    metrics = Metrics()

    @asynccontextmanager
    async def lifespan(app):
        db = Database(dsn)

        app.state.db = db
        app.state.jobs = Jobs(
            db,
            settings,
        )
        app.state.scheduler = Scheduler(
            db,
            settings,
            metrics,
        )
        app.state.initialized = False

        stop = threading.Event()

        recovery = Recovery(
            app.state.scheduler,
            stop,
        )

        def initialize():
            # Keep retrying during startup while PostgreSQL becomes available.
            while not stop.is_set():
                try:
                    # The API owns migrations.
                    if role == "api":
                        migrate(dsn)

                    if db.ready():

                        # Recover stale work before the scheduler becomes ready.
                        if role == "scheduler":
                            recovery.sweep()

                        app.state.initialized = True

                        event(
                            "ready",
                            role=role,
                        )

                        break

                except (
                    psycopg.OperationalError,
                    psycopg.errors.UndefinedTable,
                    PoolTimeout,
                ) as exc:
                    event(
                        "startup_retry",
                        reason=type(exc).__name__,
                    )

                except Exception as exc:
                    # Unexpected startup errors should not retry forever.
                    exception_event(
                        "startup_fatal_error",
                        exc,
                    )

                    return

                stop.wait(1)

            if role == "scheduler":

                # Periodically recover work left behind by dead workers.
                while not stop.wait(
                    settings.recovery_interval_ms / 1000
                ):
                    try:
                        recovery.sweep()

                    except (
                        psycopg.OperationalError,
                        PoolTimeout,
                    ) as exc:
                        # A temporary DB error can simply be retried next time.
                        metrics.db_errors.inc()

                        exception_event(
                            "recovery_database_error",
                            exc,
                        )

                    except Exception as exc:
                        # Unexpected recovery failures make the service unsafe
                        # to consider ready.
                        app.state.initialized = False

                        exception_event(
                            "recovery_fatal_error",
                            exc,
                        )

                        return

        thread = threading.Thread(
            target=initialize,
            name="initialize",
            daemon=True,
        )

        thread.start()

        try:
            yield

        finally:
            stop.set()

            await asyncio.to_thread(
                thread.join,
                15,
            )

            db.close()

    app = FastAPI(
        title="Distributed Job Scheduler",
        lifespan=lifespan,
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )

    app.state.settings = settings
    app.state.metrics = metrics

    app.add_middleware(
        ProtocolMiddleware,
        limit=settings.body_limit,
        metrics=metrics,
    )

    # -------------------------------------------------------------------------
    # Errors
    # -------------------------------------------------------------------------

    @app.exception_handler(Conflict)
    async def conflict(request, exc):
        metrics.operations.labels(
            "request",
            exc.code,
        ).inc()

        event(
            "request_rejected",
            reason=exc.code,
        )

        return error_response(
            exc.code,
            exc.message,
            exc.status,
        )

    @app.exception_handler(RequestValidationError)
    async def validation(request, exc):
        # Keep validation errors stable instead of exposing Pydantic internals.
        return error_response(
            "INVALID_REQUEST",
            "Request fields, types or limits are invalid",
            400,
        )

    @app.exception_handler(HTTPException)
    async def http_error(request, exc):
        return error_response(
            "HTTP_ERROR",
            "Request cannot be handled",
            exc.status_code,
        )

    async def db_error(request, exc):
        metrics.db_errors.inc()

        exception_event(
            "database_error",
            exc,
        )

        return error_response(
            "DATABASE_UNAVAILABLE",
            "Database temporarily unavailable; retry the same logical request",
            503,
        )

    app.add_exception_handler(
        psycopg.Error,
        db_error,
    )

    app.add_exception_handler(
        PoolTimeout,
        db_error,
    )

    @app.exception_handler(Exception)
    async def unexpected(request, exc):
        # Preserve the request ID even when an unexpected error reaches here.
        identifier = request.scope.get(
            "state",
            {},
        ).get(
            "request_id"
        )

        exception_event(
            "internal_error",
            exc,
            request_id=identifier,
        )

        return error_response(
            "INTERNAL_ERROR",
            "Internal server error",
            500,
            identifier,
        )

    # -------------------------------------------------------------------------
    # Health and metrics
    # -------------------------------------------------------------------------

    @app.get("/health/live")
    def live():
        return {"status": "UP"}

    @app.get("/health/ready")
    def ready(request: Request):
        try:
            ok = (
                request.app.state.initialized
                and request.app.state.db.ready()
            )

        except (
            psycopg.Error,
            PoolTimeout,
        ):
            ok = False

        return JSONResponse(
            {
                "status": "UP" if ok else "DOWN",
            },
            status_code=200 if ok else 503,
        )

    @app.get("/metrics")
    def scrape(request: Request):
        # DB-backed metrics only make sense once startup has finished.
        if request.app.state.initialized:
            metrics.refresh(
                request.app.state.db
            )

        return Response(
            generate_latest(metrics.registry),
            headers={
                "Content-Type": CONTENT_TYPE_LATEST,
            },
        )

    def jobs(request):
        if not request.app.state.initialized:
            raise Conflict(
                "NOT_READY",
                "Schema is not ready",
                503,
            )

        return request.app.state.jobs

    # -------------------------------------------------------------------------
    # Public API
    # -------------------------------------------------------------------------

    if role == "api":

        @app.post("/v1/jobs")
        def create_job(
            request: Request,
            body: CreateJob,
            idempotency_key: Annotated[
                str | None,
                Header(),
            ] = None,
        ):
            result, created = jobs(request).create(
                body,
                idempotency_key,
            )

            event(
                "job_created" if created else "job_duplicate",
                job_id=result["id"],
                operation=body.payload.operation,
                new_state=result["status"],
            )

            return JSONResponse(
                jsonable_encoder(result),
                status_code=201 if created else 200,
                headers={
                    "Location": f"/v1/jobs/{result['id']}",
                },
            )

        @app.get("/v1/jobs")
        def list_jobs(
            request: Request,
            limit: Annotated[
                int,
                Query(ge=1, le=200),
            ] = 50,
            cursor: str | None = None,
            status: Literal[
                "QUEUED",
                "RUNNING",
                "COMPLETED",
                "FAILED",
            ]
            | None = None,
        ):
            return jobs(request).list_jobs(
                limit,
                cursor,
                status,
            )

        @app.get("/v1/jobs/{job_id}")
        def get_job(
            request: Request,
            job_id: UUID,
        ):
            return jobs(request).job(
                job_id
            )

        @app.get("/v1/jobs/{job_id}/tasks")
        def list_tasks(
            request: Request,
            job_id: UUID,
            limit: Annotated[
                int,
                Query(ge=1, le=200),
            ] = 50,
            cursor: str | None = None,
        ):
            return jobs(request).list_tasks(
                job_id,
                limit,
                cursor,
            )

        @app.get("/v1/tasks/{task_id}")
        def get_task(
            request: Request,
            task_id: UUID,
        ):
            return jobs(request).task(
                task_id
            )

        @app.get("/v1/tasks/{task_id}/attempts")
        def get_attempts(
            request: Request,
            task_id: UUID,
        ):
            return jobs(request).attempts(
                task_id
            )

        @app.get("/v1/workers")
        def list_workers(
            request: Request,
            limit: Annotated[
                int,
                Query(ge=1, le=200),
            ] = 50,
            cursor: str | None = None,
        ):
            return jobs(request).list_workers(
                limit,
                cursor,
            )

        @app.get("/v1/workers/{worker_id}")
        def get_worker(
            request: Request,
            worker_id: UUID,
        ):
            return jobs(request).worker(
                worker_id
            )

    # -------------------------------------------------------------------------
    # Internal scheduler API
    # -------------------------------------------------------------------------

    else:

        def scheduler(request):
            # Reuse the same readiness check as the public API.
            jobs(request)

            return request.app.state.scheduler

        @app.post("/internal/v1/workers/register")
        def register(
            request: Request,
            body: Register,
        ):
            return scheduler(request).register(
                body
            )

        @app.post(
            "/internal/v1/workers/{worker_id}/heartbeat"
        )
        def heartbeat(
            request: Request,
            worker_id: UUID,
            body: Heartbeat,
        ):
            return scheduler(request).heartbeat(
                worker_id,
                body,
            )

        @app.post("/internal/v1/claims")
        def claim(
            request: Request,
            body: Claim,
        ):
            result = scheduler(request).claim(
                body
            )

            # No assignment right now is not an error.
            return (
                result
                if result
                else Response(status_code=204)
            )

        @app.post(
            "/internal/v1/attempts/{attempt_id}/start"
        )
        def start(
            request: Request,
            attempt_id: UUID,
            body: Owner,
        ):
            return scheduler(request).start(
                attempt_id,
                body.worker_id,
            )

        @app.post(
            "/internal/v1/attempts/{attempt_id}/completion"
        )
        def complete(
            request: Request,
            attempt_id: UUID,
            body: Completion,
        ):
            return scheduler(request).complete(
                attempt_id,
                body,
            )

    return app