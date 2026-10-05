import time

import pytest
from fastapi.testclient import TestClient

from scheduler.control_plane.app import create_app


def await_ready(client):
    end = time.monotonic() + 10

    while time.monotonic() < end:
        if client.get("/health/ready").status_code == 200:
            return

        # Retry until the app becomes ready or the timeout expires.
        time.sleep(0.01)

    raise AssertionError("App did not become ready")


@pytest.mark.integration
def test_http_contracts(db, dsn):
    with TestClient(create_app("api", dsn)) as client:
        await_ready(client)

        body = {
            "name": "http",
            "taskCount": 3,
            "payload": {
                "operation": "PRIME_COUNT",
                "fromInclusive": 2,
                "toExclusive": 100,
            },
        }

        # Creating a job requires an idempotency key.
        invalid = client.post(
            "/v1/jobs",
            json=body,
        )

        assert invalid.status_code == 400
        assert (
            invalid.json()["requestId"]
            == invalid.headers["x-request-id"]
        )

        response = client.post(
            "/v1/jobs",
            json=body,
            headers={"Idempotency-Key": "http"},
        )

        assert response.status_code == 201
        assert (
            response.headers["location"]
            == "/v1/jobs/" + response.json()["id"]
        )

        # Replaying the same idempotent request returns the existing job.
        assert (
            client.post(
                "/v1/jobs",
                json=body,
                headers={"Idempotency-Key": "http"},
            ).status_code
            == 200
        )

        assert (
            client.get(
                response.headers["location"]
            ).json()["taskCount"]
            == 3
        )

        # Public API validation and protocol boundaries.
        assert client.get(
            "/v1/jobs?limit=201"
        ).status_code == 400

        assert client.post(
            "/v1/jobs",
            content=b"x" * 65537,
        ).status_code == 413

        assert client.post(
            "/v1/jobs",
            content='{"name":"a","name":"b"}',
        ).status_code == 400

        assert client.get(
            "/v1/jobs/not-a-uuid"
        ).status_code == 400

        # Internal scheduler endpoints are not exposed by the API role.
        assert client.post(
            "/internal/v1/claims",
            json={},
        ).status_code == 404

        assert client.get(
            "/metrics"
        ).status_code == 200