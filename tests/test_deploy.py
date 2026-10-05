import tomllib
from pathlib import Path


def test_package_and_deployment_files_are_complete():
    root = Path(__file__).resolve().parents[1]

    project = tomllib.loads((root / "pyproject.toml").read_text())

    # Keep the declared Python version aligned with the project runtime.
    assert project["project"]["requires-python"] == ">=3.12"

    compose = (root / "compose.yaml").read_text()

    # Deployment should remain portable and avoid mutable image tags.
    assert "container_name" not in compose
    assert ":latest" not in compose

    # The default compose setup should expose the API and run multiple workers.
    assert "replicas: 3" in compose
    assert "127.0.0.1:8080:8080" in compose

    # Secrets must not be hard-coded into the compose file.
    assert "POSTGRES_PASSWORD:" not in compose

    # Reproducible runtime dependencies must stay checked in.
    assert (root / "requirements.lock").is_file()
