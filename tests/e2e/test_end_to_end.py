import json
from urllib.request import Request, urlopen

import pytest
from conftest import eventually

pytestmark = [pytest.mark.integration, pytest.mark.e2e]


def get(url):
    with urlopen(url, timeout=3) as response:
        return json.load(response)


def create(url, count=48, end=300000):
    body = {
        "name": "real-http",
        "taskCount": count,
        "payload": {
            "operation": "PRIME_COUNT",
            "fromInclusive": 2,
            "toExclusive": end,
        },
    }

    request = Request(
        url + "/v1/jobs",
        data=json.dumps(body).encode(),
        headers={
            "Content-Type": "application/json",
            "Idempotency-Key": "e2e",
        },
        method="POST",
    )

    with urlopen(request, timeout=3) as response:
        assert response.status == 201
        return json.load(response)


def sieve_count(end):
    values = bytearray(b"\x01") * end
    values[:2] = b"\x00\x00"

    for number in range(2, int(end**0.5) + 1):
        if values[number]:
            values[number * number : end : number] = b"\x00" * len(range(number * number, end, number))

    return sum(values)


def test_three_real_worker_processes_match_independent_sieve(cluster, db):
    for index in range(3):
        cluster.worker(f"worker-{index}")

    eventually(lambda: db.run(lambda c: c.execute("SELECT count(*) AS n FROM workers").fetchone())["n"] == 3)

    job = create(cluster.api_url)

    def finished():
        result = get(cluster.api_url + "/v1/jobs/" + job["id"])

        assert result["status"] != "FAILED"

        return result if result["status"] == "COMPLETED" else None

    result = eventually(
        finished,
        40,
    )

    # Cross-check the distributed result against an independent algorithm.
    assert result["result"]["totalPrimeCount"] == sieve_count(300000)

    assert result["completedTasks"] == 48

    workers = db.run(
        lambda c: c.execute(
            """
            SELECT count(DISTINCT worker_id) AS n
            FROM task_attempts
            """
        ).fetchone()
    )["n"]

    # More than one real worker must have participated.
    assert workers >= 2

    assert (
        db.run(
            lambda c: c.execute(
                """
                SELECT count(*) AS n
                FROM tasks
                WHERE status <> 'COMPLETED'
                """
            ).fetchone()
        )["n"]
        == 0
    )
