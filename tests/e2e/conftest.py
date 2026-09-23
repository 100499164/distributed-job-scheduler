import os
import socket
import subprocess
import sys
import time
from pathlib import Path
from urllib.error import URLError
from urllib.request import urlopen

import pytest


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def eventually(predicate, timeout=20):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = predicate()
        if result:
            return result
        time.sleep(0.02)
    raise AssertionError("Condition deadline exceeded")


class Cluster:
    def __init__(self, dsn, logs, settings=None):
        self.dsn, self.logs, self.settings = dsn, logs, settings or {}
        self.processes, self.handles = [], []
        self.root = Path(__file__).resolve().parents[2]
        self.api_port, self.scheduler_port = free_port(), free_port()
        self.api_url = f"http://127.0.0.1:{self.api_port}"
        self.scheduler_url = f"http://127.0.0.1:{self.scheduler_port}"

    def spawn(self, name, args, extra=None):
        env = {
            **os.environ,
            "PYTHONPATH": str(self.root),
            "DATABASE_URL": self.dsn,
            "SCHEDULER_URL": self.scheduler_url,
            **self.settings,
            **(extra or {}),
        }
        log = (self.logs / f"{name}-{len(self.processes)}.log").open("w", encoding="utf-8")
        process = subprocess.Popen(
            [sys.executable, *args], cwd=self.root, env=env, stdout=log, stderr=subprocess.STDOUT
        )
        self.processes.append(process)
        self.handles.append(log)
        return process

    def start_api(self):
        self.api = self.spawn(
            "api",
            [
                "-m",
                "uvicorn",
                "scheduler.control_plane.app:create_app",
                "--factory",
                "--host",
                "127.0.0.1",
                "--port",
                str(self.api_port),
                "--no-access-log",
            ],
            {"ROLE": "api"},
        )
        self.ready(self.api_url)

    def start_scheduler(self):
        self.scheduler = self.spawn(
            "scheduler",
            [
                "-m",
                "uvicorn",
                "scheduler.control_plane.app:create_app",
                "--factory",
                "--host",
                "127.0.0.1",
                "--port",
                str(self.scheduler_port),
                "--no-access-log",
            ],
            {"ROLE": "scheduler"},
        )
        self.ready(self.scheduler_url)

    def worker(self, name="worker"):
        return self.spawn(name, ["-m", "scheduler.worker.runtime"], {"WORKER_CAPACITY": "1"})

    @staticmethod
    def ready(url):
        def check():
            try:
                with urlopen(url + "/health/ready", timeout=1) as response:
                    return response.status == 200
            except (URLError, OSError):
                return False

        eventually(check)

    def close(self):
        for process in reversed(self.processes):
            if process.poll() is None:
                process.terminate()
        for process in self.processes:
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
        for handle in self.handles:
            handle.close()


@pytest.fixture
def cluster(db, dsn, tmp_path, request):
    processes = Cluster(dsn, tmp_path, getattr(request, "param", None))
    try:
        processes.start_api()
        processes.start_scheduler()
        yield processes
    finally:
        processes.close()
