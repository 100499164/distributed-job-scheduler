"""Measure observed recovery on a trusted local Compose deployment."""

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from demo import sequential
from fault_tools import docker, kill_registered_worker, reject_stale_completion
from run import call, page_all


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--api", default="http://127.0.0.1:8080")
    parser.add_argument("--fault", choices=["worker", "scheduler-short", "scheduler-long"], default="worker")
    parser.add_argument("--end", type=int, default=10000000)
    parser.add_argument("--tasks", type=int, default=32)
    parser.add_argument("--outage-seconds", type=float, default=35)
    parser.add_argument("--output", type=Path, default=Path("benchmarks/results/fault.json"))
    args = parser.parse_args()
    if not 3 <= args.end <= 50_000_000:
        parser.error("Independent sieve supports --end 3..50000000")
    print(f"Fault showcase: {args.fault}", flush=True)
    job = call(
        args.api,
        "/v1/jobs",
        {
            "name": "recovery-benchmark",
            "taskCount": args.tasks,
            "payload": {"operation": "PRIME_COUNT", "fromInclusive": 2, "toExclusive": args.end},
        },
        str(uuid4()),
    )
    original_tasks = page_all(args.api, f"/v1/jobs/{job['id']}/tasks")
    original_payloads = {t["id"]: t["payload"] for t in original_tasks}
    print(f"Job {job['id']}: {len(original_tasks)} persisted PRIME_COUNT tasks", flush=True)
    deadline, old = time.monotonic() + 600, None
    while time.monotonic() < deadline and old is None:
        for task in page_all(args.api, f"/v1/jobs/{job['id']}/tasks"):
            if task["status"] == "RUNNING":
                history = call(args.api, f"/v1/tasks/{task['id']}/attempts")["items"]
                old = next((a for a in history if a["status"] == "RUNNING"), None)
                if old:
                    break
        if not old:
            time.sleep(0.05)
    if old is None:
        raise RuntimeError("No running attempt found; calibrate workload")
    print("Original RUNNING attempt: " + json.dumps(old), flush=True)
    started = time.monotonic()
    fault_wall = datetime.now(timezone.utc).isoformat()
    if args.fault == "worker":
        kill_registered_worker(old["workerId"])
    elif args.fault == "scheduler-short":
        print("Restarting scheduler: live leases survive if renewal resumes before expiry.", flush=True)
        docker("restart", "scheduler")
    else:
        docker("stop", "scheduler")
        time.sleep(args.outage_seconds)  # Intentional, measured outage duration.
        docker("start", "scheduler")
    observed_reassignment = None
    while time.monotonic() < deadline:
        history = call(args.api, f"/v1/tasks/{old['taskId']}/attempts")["items"]
        if observed_reassignment is None and any(a["attemptNumber"] > old["attemptNumber"] for a in history):
            observed_reassignment = time.monotonic() - started
            print("Replacement attempt: " + json.dumps(history[-1]), flush=True)
        state = call(args.api, "/v1/jobs/" + job["id"])
        if state["status"] in ("COMPLETED", "FAILED"):
            output = {
                "fault": args.fault,
                "faultObservedAt": fault_wall,
                "originalAttempt": old,
                "observedReassignmentSeconds": observed_reassignment,
                "observedJobTerminalSeconds": time.monotonic() - started,
                "job": state,
                "attemptHistory": history,
                "pollIntervalSeconds": 0.1,
                "note": "Observation latency includes polling; not subtraction of unsynchronized host/DB clocks",
            }
            if state["status"] != "COMPLETED" or state["result"] != {"totalPrimeCount": sequential(args.end)}:
                raise RuntimeError("Recovery result does not match independent sieve")
            tasks = page_all(args.api, f"/v1/jobs/{job['id']}/tasks")
            if {t["id"]: t["payload"] for t in tasks} != original_payloads:
                raise RuntimeError("Task identities or payloads changed across recovery")
            if args.fault == "worker":
                if history[0]["status"] != "EXPIRED" or history[-1]["workerId"] == old["workerId"]:
                    raise RuntimeError("Sample invalid: original attempt did not expire onto another worker")
                # Use the trusted internal network; do not publish the scheduler port.
                output["staleCompletionRejected"] = reject_stale_completion(old)
                if call(args.api, "/v1/jobs/" + job["id"])["result"] != state["result"]:
                    raise RuntimeError("Stale completion changed canonical result")
                print("Stale completion rejected; canonical result unchanged.", flush=True)
            output["verified"] = True
            print(
                f"VERIFIED {state['status']}: {state['result']}; task identities and payloads preserved",
                flush=True,
            )
            print("Attempt history: " + json.dumps(history), flush=True)
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(json.dumps(output, indent=2))
            return
        time.sleep(0.1)
    raise TimeoutError("Recovery benchmark exceeded deadline")


if __name__ == "__main__":
    main()
