"""Worker runtime and communication with the scheduler."""

import json
import os
import random
import signal
import socket
import threading
import time
from collections.abc import Mapping, Sequence
from concurrent.futures import Future, ThreadPoolExecutor, wait
from dataclasses import dataclass, field
from typing import Any, TypedDict, cast
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from uuid import uuid4

from prometheus_client import CollectorRegistry, Gauge, Histogram, write_to_textfile
from pydantic import TypeAdapter, ValidationError

from scheduler.observability import configure_logs, event, exception_event
from scheduler.protocol.models import KNOWN_OPERATIONS, Operation, SupportedOperations
from scheduler.worker.workload import EXECUTORS, Cancelled, execute
from scheduler.workloads.catalog import UnsupportedOperation


def configured_operations(value: str | None) -> list[Operation]:
    """Parse the operations enabled for this worker."""
    try:
        return TypeAdapter(SupportedOperations).validate_python(
            list(KNOWN_OPERATIONS) if value is None else [part.strip() for part in value.split(",")]
        )
    except ValidationError as exc:
        raise ValueError("WORKER_OPERATIONS must be a nonempty, unique list of known operations") from exc


class RemoteError(Exception):
    def __init__(self, status: int, body: Mapping[str, object]) -> None:
        self.status, self.body = status, body
        self.code = body.get("code", "HTTP_ERROR")
        super().__init__(str(self.code))


class Client:
    def __init__(self, base_url: str) -> None:
        self.base_url = base_url.rstrip("/")

    def post(self, path: str, body: Mapping[str, object]) -> dict[str, Any] | None:
        request = Request(
            self.base_url + path,
            data=json.dumps(body).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urlopen(request, timeout=3) as response:
                return json.load(response) if response.status != 204 else None
        except HTTPError as exc:
            try:
                body = json.load(exc)
            except (ValueError, OSError):
                body = {"code": "HTTP_ERROR"}
            raise RemoteError(exc.code, body) from None


class Assignment(TypedDict):
    """Decoded scheduler response; UUIDs and timestamps are strings over HTTP."""

    taskId: str
    attemptId: str
    attemptNumber: int
    jobId: str
    status: str
    leaseExpiresAt: str
    payload: dict[str, object]


@dataclass
class Slot:
    assignment: Assignment
    cancel: threading.Event = field(default_factory=threading.Event)
    future: Future[dict[str, int]] | None = None
    completion: dict[str, object] | None = None
    invalid: bool = False


class Worker:
    def __init__(
        self,
        url: str,
        capacity: int = 1,
        client: Client | None = None,
        operations: Sequence[str] | None = None,
        drain_timeout: float = 30,
    ) -> None:
        if not 1 <= capacity <= 64:
            raise ValueError("WORKER_CAPACITY must be in [1,64]")
        self.operations = TypeAdapter(SupportedOperations).validate_python(
            list(KNOWN_OPERATIONS) if operations is None else operations
        )
        if not set(self.operations) <= EXECUTORS.keys():
            raise ValueError("Configured operation has no local executor")
        if not 1 <= drain_timeout <= 300:
            raise ValueError("WORKER_DRAIN_TIMEOUT_SECONDS must be in [1,300]")
        self.drain_timeout = drain_timeout
        self.drain_started: float | None = None
        self.draining = threading.Event()
        self.id = str(uuid4())
        self.capacity = capacity
        self.client = client or Client(url)
        self.stop = threading.Event()
        self.lock = threading.Lock()
        self.semaphore = threading.BoundedSemaphore(capacity)
        self.slots: dict[str, Slot] = {}
        self.pending_claim: str | None = None
        self.settings: dict[str, int] | None = None
        self.executor = ThreadPoolExecutor(max_workers=capacity, thread_name_prefix="workload")
        self.heartbeat_thread: threading.Thread | None = None
        self.metric_registry = CollectorRegistry()
        self.compute_seconds = Histogram(
            "worker_calculation_seconds", "Local calculation duration", registry=self.metric_registry
        )
        self.pending_results = Gauge(
            "worker_results_pending", "Completed results awaiting ACK", registry=self.metric_registry
        )
        self.occupied_slots = Gauge(
            "worker_slots_occupied", "Local reserved slots", registry=self.metric_registry
        )
        self.metrics_file = os.getenv("WORKER_METRICS_FILE")
        self.last_metrics = 0.0

    def _error(self, exc: Exception) -> None:
        if isinstance(exc, RemoteError) and exc.code in ("SESSION_EXPIRED", "REGISTRATION_CONFLICT"):
            event("worker_session_expired", worker_id=self.id)
            self.stop.set()
        elif isinstance(exc, RemoteError) and exc.status in (400, 404):
            event("worker_protocol_error", worker_id=self.id, reason=exc.code)
            self.stop.set()

    def _register(self) -> bool:
        delay = 0.5
        while not self.stop.is_set() and not self.draining.is_set():
            try:
                response = self.client.post(
                    "/internal/v1/workers/register",
                    {
                        "workerId": self.id,
                        "hostname": socket.gethostname(),
                        "version": "1",
                        "capacity": self.capacity,
                        "supportedOperations": self.operations,
                    },
                )
                assert response is not None
                self.settings = response["settings"]
                event(
                    "worker_registered",
                    worker_id=self.id,
                    capacity=self.capacity,
                    supported_operations=self.operations,
                )
                return True
            except (RemoteError, URLError, OSError, TimeoutError) as exc:
                self._error(exc)
                self.stop.wait(delay)
                delay = min(5, delay * 2)
        return False

    def _claim(self) -> bool:
        if self.pending_claim is None:
            if self.draining.is_set() or self.stop.is_set():
                return False
            if not self.semaphore.acquire(blocking=False):
                return False
            self.pending_claim = str(uuid4())
        try:
            assignment = self.client.post(
                "/internal/v1/claims", {"workerId": self.id, "claimRequestId": self.pending_claim}
            )
        except RemoteError as exc:
            self._error(exc)
            if exc.status == 409:
                self.pending_claim = None
                self.semaphore.release()
                if exc.code == "CAPACITY_EXHAUSTED":
                    event("worker_capacity_reconciliation", worker_id=self.id, local_slots=len(self.slots))
                return False
            raise
        self.pending_claim = None
        if assignment is None:
            self.semaphore.release()
            return False
        with self.lock:
            attempt_id = assignment["attemptId"]
            if attempt_id in self.slots:
                self.semaphore.release()
            else:
                self.slots[attempt_id] = Slot(cast(Assignment, assignment))
                event("worker_claim", worker_id=self.id, attempt_id=attempt_id, task_id=assignment["taskId"])
        return True

    def _calculate(self, slot: Slot) -> dict[str, int]:
        started = time.monotonic()
        p = slot.assignment["payload"]
        if p.get("operation", "PRIME_COUNT") not in self.operations:
            raise UnsupportedOperation("Assignment outside advertised capabilities")
        result = execute(p, slot.cancel.is_set)
        self.compute_seconds.observe(time.monotonic() - started)
        event(
            "calculation_finished",
            worker_id=self.id,
            attempt_id=slot.assignment["attemptId"],
            task_id=slot.assignment["taskId"],
            operation=p.get("operation", "PRIME_COUNT"),
            duration_ms=round((time.monotonic() - started) * 1000, 3),
        )
        return result

    def _advance(self, attempt_id: str, slot: Slot) -> None:
        if slot.invalid:
            slot.cancel.set()
            if slot.future is None or slot.future.done():
                self._release(attempt_id)
            return
        try:
            if slot.future is None:
                self.client.post(f"/internal/v1/attempts/{attempt_id}/start", {"workerId": self.id})
                # Start local execution only after the scheduler confirms the attempt.
                slot.future = self.executor.submit(self._calculate, slot)
            if slot.future.done():
                if slot.completion is None:
                    try:
                        result = slot.future.result()
                        slot.completion = {"workerId": self.id, "outcome": "SUCCEEDED", "result": result}
                    except Cancelled:
                        slot.invalid = True
                        return
                    except (UnsupportedOperation, ValidationError) as exc:
                        code = (
                            "UNSUPPORTED_OPERATION"
                            if isinstance(exc, UnsupportedOperation)
                            else "INVALID_PAYLOAD"
                        )
                        slot.completion = self._failure(code)
                    except Exception as exc:
                        # Unexpected executor failures are reported to the scheduler and logged locally.
                        exception_event("execution_error", exc, worker_id=self.id, attempt_id=attempt_id)
                        slot.completion = self._failure("EXECUTION_ERROR")
                self.client.post(f"/internal/v1/attempts/{attempt_id}/completion", slot.completion)
                self._release(attempt_id)
        except RemoteError as exc:
            self._error(exc)
            if exc.status in (400, 404, 409):
                slot.invalid = True
                slot.cancel.set()
            else:
                raise

    def _failure(self, code: str) -> dict[str, object]:
        return {
            "workerId": self.id,
            "outcome": "FAILED",
            "error": {"code": code, "message": "Workload execution failed"},
        }

    def _release(self, attempt_id: str) -> None:
        with self.lock:
            if self.slots.pop(attempt_id, None) is not None:
                self.semaphore.release()

    def _heartbeats(self) -> None:
        assert self.settings is not None
        interval = self.settings["heartbeatIntervalMs"] / 1000
        while not self.stop.wait(interval):
            with self.lock:
                active = [
                    aid for aid, slot in self.slots.items() if slot.future is not None and not slot.invalid
                ]
            try:
                response = self.client.post(
                    f"/internal/v1/workers/{self.id}/heartbeat", {"activeAttemptIds": active}
                )
                assert response is not None
                with self.lock:
                    for rejected in response["rejected"]:
                        slot = self.slots.get(rejected["attemptId"])
                        # Keep completed attempts so their result can still be acknowledged.
                        if slot and slot.completion is None:
                            slot.invalid = True
                            slot.cancel.set()
            except (RemoteError, URLError, OSError, TimeoutError) as exc:
                self._error(exc)

    def _start_heartbeats(self) -> None:
        self.heartbeat_thread = threading.Thread(target=self._heartbeats, name="heartbeat", daemon=True)
        self.heartbeat_thread.start()

    def _write_metrics(self) -> None:
        if not self.metrics_file or time.monotonic() - self.last_metrics < 1:
            return
        with self.lock:
            self.occupied_slots.set(len(self.slots) + int(self.pending_claim is not None))
            self.pending_results.set(sum(slot.completion is not None for slot in self.slots.values()))
        write_to_textfile(self.metrics_file, self.metric_registry)
        self.last_metrics = time.monotonic()

    def request_drain(self) -> None:
        if not self.draining.is_set():
            self.drain_started = time.monotonic()
            self.draining.set()
            event("worker_draining", worker_id=self.id, timeout_seconds=self.drain_timeout)

    def _drain_finished(self) -> bool:
        if not self.draining.is_set():
            return False
        with self.lock:
            empty = not self.slots and self.pending_claim is None
        if empty:
            event("worker_drained", worker_id=self.id)
            return True
        assert self.drain_started is not None
        if time.monotonic() - self.drain_started >= self.drain_timeout:
            event("worker_drain_timeout", worker_id=self.id)
            return True
        return False

    def run(self) -> None:
        try:
            if not self._register():
                return
            assert self.settings is not None
            self._start_heartbeats()
            delay = 0.5
            while not self.stop.is_set() and not self._drain_finished():
                try:
                    self._write_metrics()
                    with self.lock:
                        snapshot = list(self.slots.items())
                    for attempt_id, slot in snapshot:
                        if self._drain_finished():
                            break
                        self._advance(attempt_id, slot)
                    claimed = self._claim()
                    delay = 0.5
                    if not claimed:
                        poll_seconds = self.settings["pollIntervalMs"] / 1000
                        if self.slots:
                            poll_seconds = min(0.05, poll_seconds)
                        else:
                            poll_seconds *= random.uniform(0.8, 1.2)
                        self.stop.wait(poll_seconds)
                except (RemoteError, URLError, OSError, TimeoutError) as exc:
                    self._error(exc)
                    self.stop.wait(delay)
                    delay = min(5, delay * 2)
        finally:
            self.stop.set()
            if self.heartbeat_thread:
                self.heartbeat_thread.join(timeout=4)
            with self.lock:
                for slot in self.slots.values():
                    slot.cancel.set()
                futures = [s.future for s in self.slots.values() if s.future]
            if futures:
                _, unfinished = wait(futures, timeout=5)
                if unfinished:
                    event("uncooperative_workload", worker_id=self.id)
                    # Python cannot stop a running thread, so force process exit after the drain timeout.
                    # Persisted leases allow the scheduler to recover the unfinished attempt.
                    os._exit(2)
            self.executor.shutdown(wait=True, cancel_futures=True)


def main() -> None:
    configure_logs("worker")
    worker = Worker(
        os.environ.get("SCHEDULER_URL", "http://127.0.0.1:8081"),
        int(os.environ.get("WORKER_CAPACITY", "1")),
        operations=configured_operations(os.environ.get("WORKER_OPERATIONS")),
        drain_timeout=int(os.environ.get("WORKER_DRAIN_TIMEOUT_SECONDS", "30")),
    )
    signal.signal(signal.SIGINT, lambda *_: worker.request_drain())
    signal.signal(signal.SIGTERM, lambda *_: worker.request_drain())
    worker.run()


if __name__ == "__main__":
    main()
