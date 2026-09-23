"""Run after docker compose up --build. Uses only the Python standard library."""

import argparse
import json
import time
from urllib.request import Request, urlopen
from uuid import uuid4


def call(base, path, body=None, key=None):
    headers = {"Content-Type": "application/json"}
    if key:
        headers["Idempotency-Key"] = key
    request = Request(
        base + path,
        data=json.dumps(body).encode() if body is not None else None,
        headers=headers,
        method="POST" if body is not None else "GET",
    )
    with urlopen(request, timeout=10) as response:
        return json.load(response)


def sequential(end):
    sieve = bytearray(b"\x01") * end
    sieve[:2] = b"\x00\x00"
    for n in range(2, int(end**0.5) + 1):
        if sieve[n]:
            sieve[n * n : end : n] = b"\x00" * len(range(n * n, end, n))
    return sum(sieve)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--api", default="http://127.0.0.1:8080")
    parser.add_argument("--end", type=int, default=1000000)
    parser.add_argument("--tasks", type=int, default=48)
    parser.add_argument("--timeout", type=float, default=600)
    args = parser.parse_args()
    if not 3 <= args.end <= 50_000_000:
        parser.error("Demo sieve supports --end 3..50000000; the service allows up to 1e9")
    body = {
        "name": "python-mvp-demo",
        "taskCount": args.tasks,
        "payload": {"operation": "PRIME_COUNT", "fromInclusive": 2, "toExclusive": args.end},
    }
    key = str(uuid4())
    job = call(args.api, "/v1/jobs", body, key)
    assert call(args.api, "/v1/jobs", body, key)["id"] == job["id"]
    print(json.dumps({"jobId": job["id"], "idempotencyVerified": True}), flush=True)
    deadline = time.monotonic() + args.timeout
    last = None
    while time.monotonic() < deadline:
        job = call(args.api, "/v1/jobs/" + job["id"])
        progress = (job["status"], job["completedTasks"], job["failedTasks"])
        if progress != last:
            print(json.dumps(job), flush=True)
            last = progress
        if job["status"] == "FAILED":
            raise SystemExit("Job failed; inspect task attempts and their errorCode")
        if job["status"] == "COMPLETED":
            expected = sequential(args.end)
            assert job["result"]["totalPrimeCount"] == expected
            print(json.dumps({"verified": True, "totalPrimeCount": expected}), flush=True)
            return
        time.sleep(0.25)
    raise SystemExit("Demo deadline exceeded")


if __name__ == "__main__":
    main()
