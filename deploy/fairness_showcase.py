"""Show a later small job getting capacity before an older large job finishes."""

import argparse
import json
import time
from pathlib import Path
from uuid import uuid4

from demo import call, sequential
from showcase import page_all, require


def wait_for(predicate, timeout=120, description="condition"):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(0.1)
    raise TimeoutError(f"Deadline exceeded waiting for {description}")


def terminal(api, job_id, timeout=120):
    def check():
        job = call(api, "/v1/jobs/" + job_id)
        require(job["status"] != "FAILED", f"Job {job_id} failed")
        return job if job["status"] == "COMPLETED" else None

    return wait_for(check, timeout, "job completion")


def run_fairness(api, end=5_000_000, count=120, timeout=180):
    def submit(name, tasks, upper):
        return call(
            api,
            "/v1/jobs",
            {
                "name": name,
                "taskCount": tasks,
                "payload": {"operation": "PRIME_COUNT", "fromInclusive": 2, "toExclusive": upper},
            },
            str(uuid4()),
        )

    large = submit("fairness-large", count, end)
    wait_for(lambda: call(api, "/v1/jobs/" + large["id"])["startedAt"], timeout, "large job start")
    large_before = call(api, "/v1/jobs/" + large["id"])
    print(f"Large job {large['id']}: {large_before['completedTasks']}/{count} completed", flush=True)
    small = submit("fairness-small", 4, 100_000)
    print(f"Small job submitted: {small['id']} (same PRIME_COUNT pool)", flush=True)

    def small_started():
        return next((t for t in page_all(api, f"/v1/jobs/{small['id']}/tasks") if t["firstStartedAt"]), None)

    first = wait_for(small_started, timeout, "small job first task start")
    large_during = call(api, "/v1/jobs/" + large["id"])
    require(
        large_during["status"] != "COMPLETED",
        "Sample inconclusive: large job already finished; increase --end or reduce worker count",
    )
    print(
        f"Small job first task started: {first['id']}; large still incomplete "
        f"({large_during['completedTasks']}/{count}). Fairness verified.",
        flush=True,
    )
    small_final, large_final = terminal(api, small["id"], timeout), terminal(api, large["id"], timeout)
    require(small_final["result"] == {"totalPrimeCount": sequential(100_000)}, "Small job reference mismatch")
    require(large_final["result"] == {"totalPrimeCount": sequential(end)}, "Large job reference mismatch")
    return {
        "verified": True,
        "smallFirstTask": first,
        "largeWhenSmallStarted": large_during,
        "small": small_final,
        "large": large_final,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--api", default="http://127.0.0.1:8080")
    parser.add_argument("--end", type=int, default=5_000_000)
    parser.add_argument("--tasks", type=int, default=120)
    parser.add_argument("--timeout", type=float, default=180)
    parser.add_argument("--output", type=Path, default=Path("verification/fairness.json"))
    args = parser.parse_args()
    if not 3 <= args.end <= 50_000_000 or not 1 <= args.tasks <= min(10000, args.end - 2):
        parser.error("Use a nonempty task partition and --end <= 50000000 for the reference sieve")
    result = run_fairness(args.api, args.end, args.tasks, args.timeout)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
