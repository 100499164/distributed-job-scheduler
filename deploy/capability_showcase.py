"""Build an isolated heterogeneous Compose pool; verify routing, recovery and fairness."""

import argparse
import json
import math
import os
import subprocess
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from uuid import uuid4

from demo import call
from fairness_showcase import run_fairness, terminal, wait_for
from fault_tools import ROOT, kill_registered_worker, reject_stale_completion
from showcase import CASES, page_all, require, verify


def audit_routing(api, job_ids):
    routing = []
    for job_id in job_ids:
        for task in page_all(api, f"/v1/jobs/{job_id}/tasks"):
            for attempt in call(api, f"/v1/tasks/{task['id']}/attempts")["items"]:
                worker = call(api, "/v1/workers/" + attempt["workerId"])
                operation = task["payload"]["operation"]
                require(operation in worker["supportedOperations"], "Incompatible routing detected")
                row = {
                    "jobId": job_id,
                    "taskId": task["id"],
                    "attemptId": attempt["id"],
                    "workerId": worker["id"],
                    "operation": operation,
                    "supportedOperations": worker["supportedOperations"],
                }
                routing.append(row)
                print(
                    f"  {operation:16} task={task['id']} → worker={worker['id']} "
                    f"supports={','.join(worker['supportedOperations'])}",
                    flush=True,
                )
    return routing


def recovery(api, compose):
    # Only prime and pi are started at this point; the later generalist is observable.
    payload = {"operation": "MONTE_CARLO_PI", "samples": 50_000_000, "seed": 123456}
    job = call(
        api, "/v1/jobs", {"name": "capability-recovery", "taskCount": 1, "payload": payload}, str(uuid4())
    )
    task = page_all(api, f"/v1/jobs/{job['id']}/tasks")[0]

    def history():
        return call(api, f"/v1/tasks/{task['id']}/attempts")["items"]

    old = wait_for(
        lambda: next((a for a in history() if a["status"] == "RUNNING"), None),
        description="pi worker RUNNING",
    )
    worker = call(api, "/v1/workers/" + old["workerId"])
    require(worker["supportedOperations"] == ["MONTE_CARLO_PI"], "Expected the dedicated pi worker")
    print("Original attempt: " + json.dumps(old), flush=True)
    kill_registered_worker(old["workerId"], compose, "worker-pi")
    expired = wait_for(
        lambda: next((a for a in history() if a["id"] == old["id"] and a["status"] == "EXPIRED"), None),
        description="expired pi attempt",
    )
    waiting = call(api, f"/v1/tasks/{task['id']}")
    require(waiting["status"] == "RETRY_WAIT", "Expected retry waiting without compatible workers")
    require(waiting["payload"] == task["payload"], "Retry changed samples/seed")
    print("After death: EXPIRED; task RETRY_WAIT, prime worker cannot receive it.", flush=True)
    compose("up", "-d", "--wait", "worker-general")
    new = wait_for(
        lambda: next((a for a in history() if a["attemptNumber"] > old["attemptNumber"]), None),
        description="generalist assignment",
    )
    require(new["workerId"] != old["workerId"], "Retry did not change worker")
    replacement = call(api, "/v1/workers/" + new["workerId"])
    require(
        set(replacement["supportedOperations"]) == {"PRIME_COUNT", "RANGE_SUM", "MONTE_CARLO_PI"},
        "Expected generalist",
    )
    print("Replacement attempt: " + json.dumps(new), flush=True)
    final = terminal(api, job["id"])
    result = final["result"]
    require(
        result["samples"] == payload["samples"] and 0 <= result["insideCircle"] <= result["samples"],
        "Invalid sample accounting",
    )
    require(
        result["piEstimate"] == 4 * result["insideCircle"] / result["samples"]
        and abs(result["piEstimate"] - math.pi) < 0.02,
        "Invalid pi estimate",
    )
    after = call(api, f"/v1/tasks/{task['id']}")
    require(after["payload"] == task["payload"], "Retry payload changed")
    require(
        after["result"] == {"samples": result["samples"], "insideCircle": result["insideCircle"]},
        "Canonical reduction mismatch",
    )
    stale = reject_stale_completion(old, {"samples": task["payload"]["samples"], "insideCircle": 0}, compose)
    require(
        history()[0]["status"] == "EXPIRED" and call(api, "/v1/jobs/" + job["id"])["result"] == result,
        "Stale completion changed canonical state",
    )
    print(
        f"Recovery verified: pi specialist → generalist, result={result}; stale completion rejected.",
        flush=True,
    )
    return {
        "verified": True,
        "job": final,
        "original": expired,
        "replacement": new,
        "history": history(),
        "staleCompletion": stale,
    }


def run(api, compose):
    recovered = recovery(api, compose)
    compose("up", "-d", "--wait", "worker-pi")
    print("Mixed routing across prime, pi and generalist workers:", flush=True)
    # Keep the task-by-task routing table short enough to present live.
    cases = [{**case, "taskCount": 4} for case in CASES]
    with ThreadPoolExecutor(max_workers=3) as pool:
        jobs = list(pool.map(lambda case: call(api, "/v1/jobs", case, str(uuid4())), cases))
    finals = [terminal(api, job["id"]) for job in jobs]
    for case, job in zip(cases, finals):
        verify(case, job, page_all(api, f"/v1/jobs/{job['id']}/tasks"))
    routing = audit_routing(api, [recovered["job"]["id"], *[j["id"] for j in jobs]])
    fair = run_fairness(api)
    print("Capabilities, compatible recovery and fairness verified.", flush=True)
    return {
        "verified": True,
        "recovery": recovered,
        "mixedJobs": finals,
        "routing": routing,
        "fairness": fair,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=8082)
    parser.add_argument("--output", type=Path, default=Path("verification/capability-showcase.json"))
    args = parser.parse_args()
    if not 1 <= args.port <= 65535:
        parser.error("--port must be in 1..65535")
    project = "capability-showcase-" + uuid4().hex[:8]

    def compose(*command):
        return subprocess.check_output(
            [
                "docker",
                "compose",
                "-p",
                project,
                "-f",
                str(ROOT / "compose.yaml"),
                "-f",
                str(ROOT / "deploy/compose.capabilities.yaml"),
                *command,
            ],
            cwd=ROOT,
            env={**os.environ, "CAPABILITY_DEMO_PORT": str(args.port)},
            text=True,
        )

    print(f"Isolated demo project {project}, API http://127.0.0.1:{args.port}", flush=True)
    try:
        compose("up", "--build", "-d", "--wait", "api", "scheduler", "worker-prime", "worker-pi")
        result = run(f"http://127.0.0.1:{args.port}", compose)
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(result, indent=2))
    finally:
        # Only resources from this invocation's unique project are removed.
        compose("down", "--volumes", "--remove-orphans")


if __name__ == "__main__":
    main()
