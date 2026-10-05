from urllib.error import URLError

from scheduler.worker.runtime import Worker


class AmbiguousClient:
    def __init__(self):
        self.keys = []

    def post(self, path, body):
        self.keys.append(
            body["claimRequestId"]
        )

        if len(self.keys) == 1:
            raise URLError(
                "lost response"
            )

        return {
            "attemptId": "same",
            "taskId": "task",
            "payload": {
                "fromInclusive": 2,
                "toExclusive": 100,
            },
        }


def test_ambiguous_claim_holds_slot_and_reuses_key():
    client = AmbiguousClient()
    worker = Worker(
        "unused",
        client=client,
    )

    try:
        try:
            worker._claim()
        except URLError:
            pass

        # A lost response leaves the claim unresolved and keeps the slot reserved.
        assert worker.pending_claim is not None
        assert not worker.semaphore.acquire(
            blocking=False
        )

        # Retrying must reuse the same claim request ID.
        assert worker._claim()

        assert (
            client.keys[0]
            == client.keys[1]
        )

        assert len(
            worker.slots
        ) == 1

        # Capacity is exhausted while the recovered assignment holds the slot.
        assert not worker._claim()

        # Releasing the same attempt twice must not over-release the semaphore.
        worker._release("same")
        worker._release("same")

        assert worker.semaphore.acquire(
            blocking=False
        )

        assert not worker.semaphore.acquire(
            blocking=False
        )

    finally:
        worker.executor.shutdown(
            wait=True
        )


def test_generic_worker_reports_defensive_errors():
    from concurrent.futures import Future

    from pydantic import ValidationError

    from scheduler.worker.runtime import Slot
    from scheduler.worker.workload import execute

    class CapturingClient:
        def __init__(self):
            self.completions = []

        def post(self, path, body):
            self.completions.append(
                body
            )

    client = CapturingClient()
    worker = Worker(
        "unused",
        client=client,
    )

    try:
        for payload, code in [
            (
                {"operation": "UNKNOWN"},
                "UNSUPPORTED_OPERATION",
            ),
            (
                {"operation": "RANGE_SUM"},
                "INVALID_PAYLOAD",
            ),
        ]:
            future = Future()

            try:
                execute(
                    payload
                )
            except (
                ValueError,
                ValidationError,
            ) as exc:
                future.set_exception(
                    exc
                )

            slot = Slot(
                {
                    "attemptId": "attempt",
                    "taskId": "task",
                    "payload": payload,
                },
                future=future,
            )

            worker._advance(
                "attempt",
                slot,
            )

            # Worker-side validation errors are translated into protocol errors.
            assert (
                client.completions[-1]["error"]["code"]
                == code
            )

    finally:
        worker.executor.shutdown(
            wait=True
        )


def test_draining_resolves_existing_ambiguous_claim_but_never_starts_new_one():
    client = AmbiguousClient()
    worker = Worker(
        "unused",
        client=client,
    )

    try:
        try:
            worker._claim()
        except URLError:
            pass

        worker.request_drain()

        # Draining must wait for an already-reserved ambiguous claim to resolve.
        assert not worker._drain_finished()

        assert worker._claim()

        # The retry still uses the original claim request ID.
        assert (
            client.keys[0]
            == client.keys[1]
        )

        worker._release(
            "same"
        )

        # Once draining has begun, no new claim may be started.
        assert not worker._claim()

        assert len(
            client.keys
        ) == 2

        assert worker._drain_finished()

    finally:
        worker.executor.shutdown(
            wait=True
        )