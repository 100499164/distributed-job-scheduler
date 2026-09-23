"""Measure control-plane overhead without benchmarking prime computation."""

import argparse
import json
import statistics
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from uuid import uuid4

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from run import percentile

from scheduler.worker.runtime import Client

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "deploy"))
from demo import call


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--api", default="http://127.0.0.1:8080")
    parser.add_argument("--scheduler", default="http://127.0.0.1:8081")
    parser.add_argument("--clients", type=int, default=8)
    parser.add_argument("--tasks", type=int, default=1000)
    parser.add_argument("--output", type=Path, default=Path("benchmarks/results/synthetic.json"))
    args = parser.parse_args()
    job = call(
        args.api,
        "/v1/jobs",
        {
            "name": "synthetic-control-plane",
            "taskCount": args.tasks,
            "payload": {"operation": "PRIME_COUNT", "fromInclusive": 2, "toExclusive": args.tasks + 2},
        },
        str(uuid4()),
    )

    def client_run(_):
        client, worker = Client(args.scheduler), str(uuid4())
        registration = client.post(
            "/internal/v1/workers/register",
            {"workerId": worker, "hostname": "benchmark", "capacity": 1, "version": "1"},
        )
        beat_at, samples = 0, []
        while True:
            if time.monotonic() >= beat_at:
                client.post(f"/internal/v1/workers/{worker}/heartbeat", {"activeAttemptIds": []})
                beat_at = time.monotonic() + registration["settings"]["heartbeatIntervalMs"] / 1000
            start = time.monotonic()
            a = client.post("/internal/v1/claims", {"workerId": worker, "claimRequestId": str(uuid4())})
            claimed = time.monotonic()
            if not a:
                return samples
            client.post(f"/internal/v1/attempts/{a['attemptId']}/start", {"workerId": worker})
            before = time.monotonic()
            # Return a minimal result so this benchmark measures protocol overhead only.
            client.post(
                f"/internal/v1/attempts/{a['attemptId']}/completion",
                {"workerId": worker, "outcome": "SUCCEEDED", "result": {"primeCount": 0}},
            )
            samples.append({"claimSeconds": claimed - start, "completionSeconds": time.monotonic() - before})

    started = time.monotonic()
    with ThreadPoolExecutor(max_workers=args.clients) as pool:
        samples = [s for batch in pool.map(client_run, range(args.clients)) for s in batch]
    elapsed = time.monotonic() - started
    if len(samples) != args.tasks:
        raise RuntimeError(
            "Competing real workers or unrelated work invalidated the sample; stop workers before this experiment"
        )
    output = {
        "kind": "synthetic-not-compute",
        "jobId": job["id"],
        "elapsedSeconds": elapsed,
        "confirmedClaimsPerSecond": len(samples) / elapsed,
        "confirmedCompletionsPerSecond": len(samples) / elapsed,
        "samples": samples,
    }
    for key in ("claimSeconds", "completionSeconds"):
        values = [s[key] for s in samples]
        output[key] = {
            "p50": statistics.median(values),
            "p95": percentile(values, 0.95),
            "p99": percentile(values, 0.99),
        }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(output, indent=2))


if __name__ == "__main__":
    main()
