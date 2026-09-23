"""Small Compose fault helpers shared by portfolio demos (trusted local deployment)."""

import json
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def docker(*args):
    return subprocess.check_output(["docker", "compose", *args], text=True, cwd=ROOT)


def kill_registered_worker(worker_id, compose=docker, service="worker"):
    for container in compose("ps", "-q", service).split():
        logs = subprocess.check_output(["docker", "logs", container], text=True, stderr=subprocess.STDOUT)
        for line in logs.splitlines():
            try:
                event = json.loads(line)
            except ValueError:
                continue
            if event.get("event") == "worker_registered" and event.get("worker_id") == worker_id:
                print(f"Killing worker {worker_id} ({container})", flush=True)
                subprocess.run(["docker", "kill", container], check=True)
                return container
    raise RuntimeError("Could not map the selected worker identity to a container")


def reject_stale_completion(attempt, result=None, compose=docker):
    """Probe the private scheduler through Compose without publishing its port."""
    script = """import json, sys
from urllib.request import Request, urlopen
from urllib.error import HTTPError
request = Request("http://127.0.0.1:8080/internal/v1/attempts/" + sys.argv[1] + "/completion",
    data=json.dumps({"workerId": sys.argv[2], "outcome": "SUCCEEDED", "result": json.loads(sys.argv[3])}).encode(),
    headers={"Content-Type": "application/json"}, method="POST")
try:
    with urlopen(request, timeout=5) as response:
        raise RuntimeError("Stale result unexpectedly accepted")
except HTTPError as exc:
    body = json.load(exc)
    if exc.code != 409 or body["code"] not in ("STALE_ATTEMPT", "SESSION_EXPIRED"):
        raise
    print(json.dumps(body))
"""
    return json.loads(
        compose(
            "exec",
            "-T",
            "scheduler",
            "python",
            "-c",
            script,
            attempt["id"],
            attempt["workerId"],
            json.dumps(result or {"primeCount": 0}),
        )
    )
