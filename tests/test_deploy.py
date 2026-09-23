import tomllib
from pathlib import Path


def test_package_and_deployment_files_are_complete():
    root = Path(__file__).resolve().parents[1]
    project = tomllib.loads((root / "pyproject.toml").read_text())
    assert project["project"]["requires-python"] == ">=3.12"
    compose = (root / "compose.yaml").read_text()
    assert "container_name" not in compose
    assert ":latest" not in compose
    assert "replicas: 3" in compose
    assert "127.0.0.1:8080:8080" in compose
    assert "POSTGRES_PASSWORD:" not in compose
    assert (root / "requirements.lock").is_file()
