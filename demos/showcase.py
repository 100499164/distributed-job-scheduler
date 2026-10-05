"""Mixed workloads and idempotency showcase. Standard library only; run after Compose."""

import argparse
import json
import math
import time
from concurrent.futures import ThreadPoolExecutor
from urllib.error import HTTPError
from uuid import uuid4

from demo import call, sequential

CASES = [
    {
        "name": "showcase-primes",
        "taskCount": 32,
        "payload": {
            "operation": "PRIME_COUNT",
            "fromInclusive": 2,
            "toExclusive": 1_000_000,
        },
    },
    {
        "name": "showcase-sum",
        "taskCount": 16,
        "payload": {
            "operation": "RANGE_SUM",
            "fromInclusive": 1,
            "toExclusive": 1_000_001,
        },
    },
    {
        "name": "showcase-pi",
        "taskCount": 32,
        "payload": {
            "operation": "MONTE_CARLO_PI",
            "samples": 1_000_000,
            "seed": 123456,
        },
    },
]


def page_all(api, path):
    items, cursor = [], None

    while True:
        page = call(
            api,
            path + "?limit=200" + ("&cursor=" + cursor if cursor else ""),
        )
        items.extend(page["items"])
        cursor = page["nextCursor"]

        if not cursor:
            return items


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def verify(case, job, tasks):
    payload = case["payload"]
    result = job["result"]

    require(job["status"] == "COMPLETED", "Job did not complete")
    require(len(tasks) == case["taskCount"], "Task count changed")
    require(
        all(task["status"] == "COMPLETED" for task in tasks),
        "Noncanonical task state",
    )

    operation = payload["operation"]

    if operation == "PRIME_COUNT":
        expected = sequential(payload["toExclusive"])
        require(
            result == {"totalPrimeCount": expected},
            "Prime reference mismatch",
        )

    elif operation == "RANGE_SUM":
        start = payload["fromInclusive"]
        end = payload["toExclusive"]
        expected = (end - 1) * end // 2 - (start - 1) * start // 2

        require(
            result == {"totalSum": expected},
            "Sum reference mismatch",
        )

    else:
        samples = sum(
            task["result"]["samples"]
            for task in tasks
        )
        inside = sum(
            task["result"]["insideCircle"]
            for task in tasks
        )

        require(
            all(
                task["result"]["samples"] == task["payload"]["samples"]
                and 0
                <= task["result"]["insideCircle"]
                <= task["result"]["samples"]
                for task in tasks
            ),
            "Invalid sample accounting",
        )

        require(
            samples == payload["samples"],
            "Samples lost or duplicated",
        )

        require(
            result
            == {
                "samples": samples,
                "insideCircle": inside,
                "piEstimate": 4 * inside / samples,
            },
            "Pi reduction mismatch",
        )

        # Fixed seeds and partition count make this reproducible.
        require(
            math.isfinite(result["piEstimate"])
            and abs(result["piEstimate"] - math.pi) < 0.02,
            "Pi outside demonstration tolerance",
        )


def run(api, timeout=600):
    print(
        "A — Mixed workloads: submitting three jobs concurrently",
        flush=True,
    )

    keys = [
        str(uuid4())
        for _ in CASES
    ]

    with ThreadPoolExecutor(max_workers=3) as pool:
        jobs = list(
            pool.map(
                lambda item: call(
                    api,
                    "/v1/jobs",
                    item[0],
                    item[1],
                ),
                zip(CASES, keys),
            )
        )

    for case, job in zip(CASES, jobs):
        print(
            f"  {case['payload']['operation']:16} {job['id']}",
            flush=True,
        )

    print(
        "B — Idempotency: same key/body preserves identity and task IDs",
        flush=True,
    )

    for case, key, job in zip(CASES, keys, jobs):
        path = f"/v1/jobs/{job['id']}/tasks"
        before = {
            task["id"]
            for task in page_all(api, path)
        }

        duplicate = call(
            api,
            "/v1/jobs",
            case,
            key,
        )

        require(
            duplicate["id"] == job["id"],
            "Duplicate job created",
        )

        require(
            before
            == {
                task["id"]
                for task in page_all(api, path)
            }
            and len(before) == case["taskCount"],
            "Tasks duplicated",
        )

        try:
            call(
                api,
                "/v1/jobs",
                {
                    **case,
                    "name": case["name"] + "-changed",
                },
                key,
            )
        except HTTPError as exc:
            require(
                exc.code == 409
                and json.load(exc)["code"] == "IDEMPOTENCY_CONFLICT",
                "Wrong conflict response",
            )
        else:
            raise RuntimeError(
                "Changed content was accepted with the same key"
            )

        print(
            f"  {case['payload']['operation']}: "
            f"same job, {len(before)} unchanged tasks, "
            "conflicting body → 409",
            flush=True,
        )

    deadline = time.monotonic() + timeout
    last = {}
    pending = set(range(len(jobs)))
    worker_operations = {}

    while pending and time.monotonic() < deadline:
        for index in sorted(pending):
            case = CASES[index]
            initial = jobs[index]

            job = call(
                api,
                "/v1/jobs/" + initial["id"],
            )

            progress = (
                job["status"],
                job["completedTasks"],
                job["failedTasks"],
            )
            operation = case["payload"]["operation"]

            if last.get(index) != progress:
                print(
                    f"  {operation:16} {progress[0]:10} "
                    f"{progress[1]}/{job['taskCount']} completed, "
                    f"{progress[2]} failed",
                    flush=True,
                )
                last[index] = progress

            require(
                job["status"] != "FAILED",
                f"{operation} failed; inspect attempt history",
            )

            if job["status"] == "COMPLETED":
                tasks = page_all(
                    api,
                    f"/v1/jobs/{job['id']}/tasks",
                )

                verify(
                    case,
                    job,
                    tasks,
                )

                for task in tasks:
                    attempts = call(
                        api,
                        f"/v1/tasks/{task['id']}/attempts",
                    )["items"]

                    for attempt in attempts:
                        worker_operations.setdefault(
                            attempt["workerId"],
                            set(),
                        ).add(operation)

                print(
                    f"  VERIFIED {operation}: "
                    f"{json.dumps(job['result'])}",
                    flush=True,
                )

                pending.remove(index)

        if pending:
            time.sleep(0.25)

    require(
        not pending,
        "Showcase deadline exceeded",
    )

    print(
        "Shared worker pool (worker ID → observed operations):",
        flush=True,
    )

    for worker, operations in sorted(worker_operations.items()):
        print(
            f"  {worker}: {', '.join(sorted(operations))}",
            flush=True,
        )

    print(
        "All three results and idempotency checks verified.",
        flush=True,
    )


def main():
    parser = argparse.ArgumentParser(
        description=__doc__
    )
    parser.add_argument(
        "--api",
        default="http://127.0.0.1:8080",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=600,
    )

    args = parser.parse_args()

    run(
        args.api,
        args.timeout,
    )


if __name__ == "__main__":
    main()