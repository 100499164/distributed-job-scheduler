"""Fixed work and granularity experiments against Compose. Never invent samples."""

import argparse
import hashlib
import json
import os
import platform
import statistics
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from uuid import uuid4

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "deploy"))
from demo import call


def percentile(values, fraction):
    values = sorted(values)
    index = (len(values) - 1) * fraction
    lo = int(index)
    return values[lo] + (values[min(lo + 1, len(values) - 1)] - values[lo]) * (index - lo)


def page_all(api, path):
    items, cursor = [], None
    while True:
        page = call(api, path + "?limit=200" + ("&cursor=" + cursor if cursor else ""))
        items.extend(page["items"])
        cursor = page["nextCursor"]
        if not cursor:
            return items


def run_job(api, end, count, timeout=600):
    started = time.monotonic()
    job = call(
        api,
        "/v1/jobs",
        {
            "name": "benchmark",
            "taskCount": count,
            "payload": {"operation": "PRIME_COUNT", "fromInclusive": 2, "toExclusive": end},
        },
        str(uuid4()),
    )
    while time.monotonic() - started < timeout:
        state = call(api, "/v1/jobs/" + job["id"])
        if state["status"] == "FAILED":
            raise RuntimeError("Benchmark job failed; do not report this as successful throughput")
        if state["status"] == "COMPLETED":
            elapsed = time.monotonic() - started
            tasks = page_all(api, f"/v1/jobs/{job['id']}/tasks")
            attempts = [
                attempt
                for task in tasks
                for attempt in call(api, f"/v1/tasks/{task['id']}/attempts")["items"]
            ]
            created = {t["id"]: datetime.fromisoformat(t["createdAt"]) for t in tasks}
            queue = [
                (datetime.fromisoformat(a["assignedAt"]) - created[a["taskId"]]).total_seconds()
                for a in attempts
                if a["attemptNumber"] == 1
            ]
            latency = [
                (datetime.fromisoformat(t["finishedAt"]) - created[t["id"]]).total_seconds() for t in tasks
            ]
            return {
                "job": state,
                "observedSeconds": elapsed,
                "tasks": tasks,
                "attempts": attempts,
                "usefulThroughput": count / elapsed,
                "attemptAmplification": len(attempts) / count,
                "queueSeconds": queue,
                "taskLatencySeconds": latency,
                "jobDatabaseSeconds": (
                    datetime.fromisoformat(state["finishedAt"]) - datetime.fromisoformat(state["createdAt"])
                ).total_seconds(),
            }
        time.sleep(0.05)
    raise TimeoutError("Benchmark exceeded deadline")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--api", default="http://127.0.0.1:8080")
    parser.add_argument("--workers", default="1,2,4,8")
    parser.add_argument(
        "--tasks", default="64", help="Comma-separated values perform the granularity experiment"
    )
    parser.add_argument("--end", type=int, default=1000000)
    parser.add_argument("--repetitions", type=int, default=5)
    parser.add_argument("--output", type=Path, default=Path("benchmarks/results"))
    args = parser.parse_args()
    if args.repetitions < 5:
        parser.error("At least five measured repetitions are required")
    root = Path(__file__).resolve().parents[1]
    args.output.mkdir(parents=True, exist_ok=True)
    revision = subprocess.run(["git", "rev-parse", "HEAD"], cwd=root, capture_output=True, text=True)
    metadata = {
        "revision": revision.stdout.strip() or "uncommitted",
        "platform": platform.platform(),
        "logicalCpus": os.cpu_count(),
        "python": platform.python_version(),
        "interval": [2, args.end],
        "capacity": 1,
        "repetitions": args.repetitions,
        "limitation": "One host; CPU quotas and physical hardware must be recorded by the operator",
    }
    info = json.loads(subprocess.check_output(["docker", "info", "--format", "{{json .}}"], text=True))
    metadata["dockerHost"] = {
        key: info.get(key) for key in ("NCPU", "MemTotal", "ServerVersion", "Architecture", "OperatingSystem")
    }
    metadata["sourceSha256"] = hashlib.sha256(
        b"".join(p.read_bytes() for p in sorted((root / "scheduler").rglob("*.py")))
    ).hexdigest()
    metadata["dependencyLock"] = (root / "requirements.lock").read_text()
    (args.output / "metadata.json").write_text(json.dumps(metadata, indent=2))
    summary = []
    for count in map(int, args.tasks.split(",")):
        baseline = None
        for workers in map(int, args.workers.split(",")):
            pending = (
                call(args.api, "/v1/jobs?status=RUNNING")["items"]
                + call(args.api, "/v1/jobs?status=QUEUED")["items"]
            )
            if pending:
                raise RuntimeError("Pending work exists; use a clean benchmark environment")
            subprocess.run(
                ["docker", "compose", "up", "-d", "--scale", f"worker={workers}", "--wait"],
                cwd=root,
                env={**os.environ, "WORKER_CAPACITY": "1"},
                check=True,
            )
            run_job(args.api, args.end, count)  # Explicit warm-up, excluded from samples.
            samples = []
            for repetition in range(args.repetitions):
                result = run_job(args.api, args.end, count)
                samples.append(result["observedSeconds"])
                target = args.output / f"tasks-{count}-workers-{workers}-run-{repetition}.json"
                target.write_text(json.dumps(result, indent=2))
                logs = subprocess.check_output(
                    [
                        "docker",
                        "compose",
                        "logs",
                        "--no-color",
                        "--since",
                        result["job"]["createdAt"],
                        "worker",
                    ],
                    cwd=root,
                    text=True,
                )
                target.with_suffix(".workers.log").write_text(logs)
            median = statistics.median(samples)
            if workers == 1:
                baseline = median
            row = {
                "workers": workers,
                "tasks": count,
                "medianSeconds": median,
                "minSeconds": min(samples),
                "maxSeconds": max(samples),
                "stdevSeconds": statistics.stdev(samples),
                "speedup": baseline / median if baseline else None,
                "efficiency": baseline / median / workers if baseline else None,
            }
            summary.append(row)
            print(json.dumps(row), flush=True)
    (args.output / "summary.json").write_text(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
