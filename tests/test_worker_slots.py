from urllib.error import URLError

from scheduler.worker.runtime import Worker


class AmbiguousClient:
    def __init__(self):
        self.keys = []

    def post(self, path, body):
        self.keys.append(body["claimRequestId"])
        if len(self.keys) == 1:
            raise URLError("lost response")
        return {"attemptId": "same", "taskId": "task", "payload": {"fromInclusive": 2, "toExclusive": 100}}


def test_ambiguous_claim_holds_slot_and_reuses_key():
    client = AmbiguousClient()
    worker = Worker("unused", client=client)
    try:
        try:
            worker._claim()
        except URLError:
            pass
        assert worker.pending_claim is not None
        assert not worker.semaphore.acquire(blocking=False)
        assert worker._claim()
        assert client.keys[0] == client.keys[1]
        assert len(worker.slots) == 1
        assert not worker._claim()
        worker._release("same")
        worker._release("same")
        assert worker.semaphore.acquire(blocking=False)
        assert not worker.semaphore.acquire(blocking=False)
    finally:
        worker.executor.shutdown(wait=True)


def test_generic_worker_reports_defensive_errors():
    from concurrent.futures import Future

    from pydantic import ValidationError

    from scheduler.worker.runtime import Slot
    from scheduler.worker.workload import execute

    class CapturingClient:
        def __init__(self):
            self.completions = []

        def post(self, path, body):
            self.completions.append(body)

    client = CapturingClient()
    worker = Worker("unused", client=client)
    try:
        for payload, code in [
            ({"operation": "UNKNOWN"}, "UNSUPPORTED_OPERATION"),
            ({"operation": "RANGE_SUM"}, "INVALID_PAYLOAD"),
        ]:
            future = Future()
            try:
                execute(payload)
            except (ValueError, ValidationError) as exc:
                future.set_exception(exc)
            slot = Slot({"attemptId": "attempt", "taskId": "task", "payload": payload}, future=future)
            worker._advance("attempt", slot)
            assert client.completions[-1]["error"]["code"] == code
    finally:
        worker.executor.shutdown(wait=True)


def test_draining_resolves_existing_ambiguous_claim_but_never_starts_new_one():
    client = AmbiguousClient()
    worker = Worker("unused", client=client)
    try:
        try:
            worker._claim()
        except URLError:
            pass
        worker.request_drain()
        assert not worker._drain_finished()  # the reserved slot is still unresolved
        assert worker._claim()
        assert client.keys[0] == client.keys[1]
        worker._release("same")
        assert not worker._claim()
        assert len(client.keys) == 2
        assert worker._drain_finished()
    finally:
        worker.executor.shutdown(wait=True)
