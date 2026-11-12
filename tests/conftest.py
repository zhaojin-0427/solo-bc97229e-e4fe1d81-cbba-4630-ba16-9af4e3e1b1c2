"""Shared pytest fixtures: isolated data dir, test client, synthetic files."""
from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))


@pytest.fixture()
def data_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    target = (tmp_path / "data").resolve()
    # Keep the env var and every module-level path binding in sync. All
    # modules read these attributes at call time.
    monkeypatch.setenv("DICOM_INGEST_DATA_DIR", str(target))
    from app import config, db  # noqa: F401
    storage = target / "objects"
    # db.get_connection() reads config.DB_PATH at call time, so patching the
    # config module redirects request threads and background rebuild threads.
    monkeypatch.setattr(config, "DATA_DIR", target)
    monkeypatch.setattr(config, "DB_PATH", target / "ingest.db")
    monkeypatch.setattr(config, "STORAGE_DIR", storage)
    yield target


@pytest.fixture()
def client(data_dir: Path):
    from fastapi.testclient import TestClient

    from app import main
    main.init_db()
    with TestClient(main.app) as test_client:
        yield test_client
    # Wait for any background rebuild threads spawned during the test so they
    # never keep running against a torn-down temp data directory afterwards.
    import threading
    for thread in list(threading.enumerate()):
        if thread.name.startswith("bodypart-rebuild-") and thread.is_alive():
            thread.join(timeout=30)
            assert not thread.is_alive(), f"{thread.name} did not finish"


@pytest.fixture(scope="session")
def samples_dir(tmp_path_factory) -> Path:
    """Generate the synthetic DICOM samples once per test session."""
    import generate_samples

    root = tmp_path_factory.mktemp("samples")
    generate_samples.generate(root)
    return root


def upload(client, center: str, paths: list[Path]):
    files = [
        ("files", (p.name, p.read_bytes(), "application/dicom"))
        for p in paths
    ]
    return client.post(
        "/api/v1/batches",
        data={"center_id": center},
        files=files,
    )
