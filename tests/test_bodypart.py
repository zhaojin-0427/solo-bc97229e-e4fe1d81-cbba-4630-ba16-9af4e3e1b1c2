"""Tests for the replaceable body-part index subsystem.

Covers: not-ready query, exact-match classification, UNCLASSIFIED for empty /
unmapped values, background rebuild + atomic cutover, ingests during a rebuild
being included in the switch, single-flight replacement, read-failure rollback
keeping the old index, restart recovery, and no duplicate index rows on
retransmit.
"""
from __future__ import annotations

import time
from pathlib import Path

import pytest

from app import bodypart
from app.bodypart import UNCLASSIFIED

from conftest import upload


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def bp_dir(samples_dir: Path, center: str) -> list[Path]:
    return sorted((samples_dir / "bp-dict" / center).glob("*.dcm"))


def v1_entries() -> list[dict]:
    """First dictionary: maps every known raw value to standard labels."""
    return [
        {"center_id": "center-a", "original_value": "CHEST",
         "standard_body_part": "CHEST"},
        {"center_id": "center-a", "original_value": "NECK",
         "standard_body_part": "HEAD-NECK"},
        {"center_id": "center-b", "original_value": "XIONG",
         "standard_body_part": "CHEST"},
        {"center_id": "center-b", "original_value": "HN",
         "standard_body_part": "HEAD-NECK"},
        {"center_id": "center-c", "original_value": "chest",
         "standard_body_part": "CHEST"},
    ]


def v2_entries() -> list[dict]:
    """Second dictionary with a deliberately different cohort semantics."""
    return [
        {"center_id": "center-a", "original_value": "CHEST",
         "standard_body_part": "THORAX"},
        {"center_id": "center-b", "original_value": "XIONG",
         "standard_body_part": "THORAX"},
        {"center_id": "center-c", "original_value": "chest",
         "standard_body_part": "THORAX"},
        {"center_id": "center-a", "original_value": "NECK",
         "standard_body_part": "HEAD-NECK"},
        {"center_id": "center-b", "original_value": "HN",
         "standard_body_part": "HEAD-NECK"},
    ]


def submit_dict(client, label: str, entries: list[dict]):
    return client.post(
        "/api/v1/body-part/dictionaries",
        json={"version_label": label, "entries": entries},
    )


def wait_terminal(client, rebuild_id: int, timeout: float = 10.0) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        body = client.get(f"/api/v1/body-part/rebuilds/{rebuild_id}").json()
        if body["status"] in ("active", "failed", "superseded"):
            return body
        time.sleep(0.02)
    raise AssertionError(f"rebuild {rebuild_id} did not finish: {body}")


def ingest_all_bp_samples(client, samples_dir: Path) -> None:
    for center in ("center-a", "center-b", "center-c"):
        resp = upload(client, center, bp_dir(samples_dir, center))
        assert resp.status_code == 201, resp.text


def query(client, center: str, standard: str):
    return client.get(
        "/api/v1/body-part/instances",
        params={"center_id": center, "standard_body_part": standard},
    )


# --------------------------------------------------------------------------- #
# Readiness
# --------------------------------------------------------------------------- #
def test_query_before_first_dictionary_is_not_ready(client):
    assert query(client, "center-a", "CHEST").status_code == 503
    assert client.get("/api/v1/body-part/active").status_code == 503


def test_active_endpoint_reports_dictionary_once_ready(client, samples_dir):
    ingest_all_bp_samples(client, samples_dir)
    resp = submit_dict(client, "bp-v1", v1_entries())
    assert resp.status_code == 202, resp.text
    rebuild = wait_terminal(client, resp.json()["id"])
    assert rebuild["status"] == "active"

    active = client.get("/api/v1/body-part/active").json()
    assert active["version_label"] == "bp-v1"
    # 3 + 2 + 3 = 8 synthetic objects.
    assert active["indexed_instances"] == 8


# --------------------------------------------------------------------------- #
# Exact-match classification and UNCLASSIFIED
# --------------------------------------------------------------------------- #
def test_exact_match_classification_and_unclassified(client, samples_dir):
    ingest_all_bp_samples(client, samples_dir)
    rebuild = wait_terminal(
        client, submit_dict(client, "bp-v1", v1_entries()).json()["id"]
    )
    assert rebuild["status"] == "active"

    # center-a CHEST -> CHEST; center-b XIONG -> CHEST (unified cohort).
    chest_a = query(client, "center-a", "CHEST").json()
    chest_b = query(client, "center-b", "CHEST").json()
    assert {r["new_sop_uid"] for r in chest_a} and len(chest_a) == 1
    assert len(chest_b) == 1
    # Original raw value is echoed for audit.
    assert chest_a[0]["original_body_part"] == "CHEST"
    assert chest_b[0]["original_body_part"] == "XIONG"

    # HEAD-NECK cohort spans NECK (a) and HN (b).
    assert len(query(client, "center-a", "HEAD-NECK").json()) == 1
    assert len(query(client, "center-b", "HEAD-NECK").json()) == 1

    # Unmapped raw value (TORSO), empty value and missing attribute all land in
    # UNCLASSIFIED: 1 (a) + 2 (c) = ... center-c has chest + empty + missing.
    un_a = query(client, "center-a", UNCLASSIFIED).json()
    un_c = query(client, "center-c", UNCLASSIFIED).json()
    assert len(un_a) == 1 and un_a[0]["original_body_part"] == "TORSO"
    assert len(un_c) == 2
    assert all(r["original_body_part"] is None for r in un_c)


def test_matching_is_case_sensitive(client, samples_dir):
    # center-c stores lowercase "chest"; an uppercase dictionary key must not
    # match it (exact match), so it stays UNCLASSIFIED.
    upload(client, "center-c", bp_dir(samples_dir, "center-c"))
    entries = [
        {"center_id": "center-c", "original_value": "CHEST",
         "standard_body_part": "CHEST"},
    ]
    rebuild = wait_terminal(
        client, submit_dict(client, "case-sense", entries).json()["id"]
    )
    assert rebuild["status"] == "active"
    assert query(client, "center-c", "CHEST").json() == []
    unclassified = query(client, "center-c", UNCLASSIFIED).json()
    raw_values = {r["original_body_part"] for r in unclassified}
    assert "chest" in raw_values


def test_download_url_points_at_real_object(client, samples_dir):
    ingest_all_bp_samples(client, samples_dir)
    wait_terminal(
        client, submit_dict(client, "bp-v1", v1_entries()).json()["id"]
    )
    row = query(client, "center-b", "CHEST").json()[0]
    assert row["download_url"].endswith(
        f"/api/v1/instances/{row['instance_id']}/download"
    )
    dl = client.get(row["download_url"])
    assert dl.status_code == 200
    assert dl.headers["content-type"] == "application/dicom"


# --------------------------------------------------------------------------- #
# Ingestion while no/active dictionary exists
# --------------------------------------------------------------------------- #
def test_ingest_before_first_dict_then_rebuild_indexes_everything(
    client, samples_dir
):
    ingest_all_bp_samples(client, samples_dir)
    # No active dictionary yet: index table stays empty, ingest still works.
    rebuild = wait_terminal(
        client, submit_dict(client, "bp-v1", v1_entries()).json()["id"]
    )
    assert rebuild["status"] == "active"
    assert len(query(client, "center-a", "CHEST").json()) == 1


def test_ingest_after_dict_is_indexed_in_same_commit(client, samples_dir):
    # Activate a dictionary on an empty/partial corpus first.
    upload(client, "center-a", bp_dir(samples_dir, "center-a")[:1])
    wait_terminal(
        client, submit_dict(client, "bp-v1", v1_entries()).json()["id"]
    )
    # New center-b objects arrive after activation: indexed immediately.
    upload(client, "center-b", bp_dir(samples_dir, "center-b"))
    assert len(query(client, "center-b", "CHEST").json()) == 1
    assert len(query(client, "center-b", "HEAD-NECK").json()) == 1


def test_retransmit_creates_no_duplicate_index_row(client, samples_dir):
    files = bp_dir(samples_dir, "center-a")
    upload(client, "center-a", files)
    wait_terminal(
        client, submit_dict(client, "bp-v1", v1_entries()).json()["id"]
    )
    before = len(query(client, "center-a", "CHEST").json())
    # Identical-byte retransmit is reused and must not duplicate index rows.
    second = upload(client, "center-a", files)
    assert second.json()["reused_count"] == len(files)
    after = len(query(client, "center-a", "CHEST").json())
    assert before == after == 1


# --------------------------------------------------------------------------- #
# Atomic replacement semantics
# --------------------------------------------------------------------------- #
def test_second_dictionary_replaces_cohort_semantics_atomically(
    client, samples_dir
):
    ingest_all_bp_samples(client, samples_dir)
    first = wait_terminal(
        client, submit_dict(client, "bp-v1", v1_entries()).json()["id"]
    )
    assert first["status"] == "active"
    assert len(query(client, "center-a", "CHEST").json()) == 1

    second = wait_terminal(
        client, submit_dict(client, "bp-v2", v2_entries()).json()["id"]
    )
    assert second["status"] == "active"

    # Old label disappears for every query; new label is served exclusively.
    assert query(client, "center-a", "CHEST").json() == []
    thorax = query(client, "center-a", "THORAX").json()
    assert len(thorax) == 1 and thorax[0]["dict_id"] == second["dict_id"]
    # HEAD-NECK mapping survives in the new dictionary as well.
    assert len(query(client, "center-b", "HEAD-NECK").json()) == 1

    # History: previous active rebuild is superseded, never deleted.
    history = client.get("/api/v1/body-part/rebuilds").json()
    statuses = {r["id"]: r["status"] for r in history}
    assert statuses[first["id"]] == "superseded"
    assert statuses[second["id"]] == "active"


def test_ingests_during_rebuild_are_included_in_switch(client, samples_dir,
                                                      monkeypatch):
    upload(client, "center-a", bp_dir(samples_dir, "center-a")[:1])
    wait_terminal(
        client, submit_dict(client, "bp-v1", v1_entries()).json()["id"]
    )

    # Gate the background DICOM reads so the rebuild is provably in flight
    # while a new instance is ingested.
    gate = _Gate()
    monkeypatch.setattr(
        "app.bodypart.dicom_io.read_body_part_examined", gate.wrap(
            bodypart.dicom_io.read_body_part_examined
        )
    )
    resp = submit_dict(client, "bp-v2", v2_entries())
    assert resp.status_code == 202
    gate.wait_entered()

    # The old dictionary still serves queries while the rebuild runs.
    assert len(query(client, "center-a", "CHEST").json()) == 1
    # A brand-new instance is committed (with old-dict index) mid-rebuild.
    upload(client, "center-b", bp_dir(samples_dir, "center-b"))
    assert len(query(client, "center-b", "CHEST").json()) == 1

    gate.release()
    rebuild = wait_terminal(client, resp.json()["id"])
    assert rebuild["status"] == "active", rebuild["error_detail"]

    # The mid-rebuild instance is covered by the new dictionary too.
    assert len(query(client, "center-b", "THORAX").json()) == 1
    assert len(query(client, "center-b", "HEAD-NECK").json()) == 1


# --------------------------------------------------------------------------- #
# Single-flight replacement
# --------------------------------------------------------------------------- #
def test_concurrent_replacement_is_rejected(client, samples_dir, monkeypatch):
    upload(client, "center-a", bp_dir(samples_dir, "center-a")[:1])
    gate = _Gate()
    monkeypatch.setattr(
        "app.bodypart.dicom_io.read_body_part_examined", gate.wrap(
            bodypart.dicom_io.read_body_part_examined
        )
    )
    first = submit_dict(client, "bp-v1", v1_entries())
    assert first.status_code == 202
    gate.wait_entered()
    try:
        second = submit_dict(client, "bp-v1b", v1_entries())
        assert second.status_code == 409
        assert "already in progress" in second.text
    finally:
        gate.release()
    rebuild = wait_terminal(client, first.json()["id"])
    assert rebuild["status"] == "active"


# --------------------------------------------------------------------------- #
# Read failure: replacement fails, previous index is preserved
# --------------------------------------------------------------------------- #
def test_read_failure_aborts_replacement_and_keeps_old_index(
    client, samples_dir, monkeypatch
):
    ingest_all_bp_samples(client, samples_dir)
    first = wait_terminal(
        client, submit_dict(client, "bp-v1", v1_entries()).json()["id"]
    )
    assert first["status"] == "active"
    old_rows = query(client, "center-a", "CHEST").json()
    assert len(old_rows) == 1

    def failing_read(path):
        raise bodypart.dicom_io.DicomValidationError(f"simulated corruption {path}")

    real_read = bodypart.dicom_io.read_body_part_examined
    monkeypatch.setattr(
        "app.dicom_io.read_body_part_examined", failing_read
    )
    resp = submit_dict(client, "bp-broken", v2_entries())
    rebuild = wait_terminal(client, resp.json()["id"])
    assert rebuild["status"] == "failed"
    # Error detail exposes the offending instance identity and the reason.
    assert "instance id=" in rebuild["error_detail"]
    assert "simulated corruption" in rebuild["error_detail"]

    # Old dictionary/index remain fully in effect and downloadable.
    active = client.get("/api/v1/body-part/active").json()
    assert active["version_label"] == "bp-v1"
    assert query(client, "center-a", "CHEST").json()[0]["instance_id"] == (
        old_rows[0]["instance_id"]
    )
    # Staging rows of the failed attempt are cleaned up.
    from app.db import get_connection
    conn = get_connection()
    try:
        leftovers = conn.execute(
            "SELECT COUNT(*) AS n FROM body_part_index_staging"
        ).fetchone()["n"]
    finally:
        conn.close()
    assert leftovers == 0

    # After the failure a fresh replacement is allowed and succeeds. The
    # failing-read patch is removed by monkeypatch at teardown; for the retry
    # we restore the real function explicitly (without undo() which would also
    # revert the temp data-dir config).
    monkeypatch.setattr(
        "app.dicom_io.read_body_part_examined", real_read
    )
    retry = wait_terminal(
        client, submit_dict(client, "bp-v2", v2_entries()).json()["id"]
    )
    assert retry["status"] == "active"


# --------------------------------------------------------------------------- #
# Restart recovery
# --------------------------------------------------------------------------- #
def test_interrupted_building_rebuild_marked_failed_on_restart(
    client, samples_dir, monkeypatch
):
    upload(client, "center-a", bp_dir(samples_dir, "center-a")[:1])
    wait_terminal(
        client, submit_dict(client, "bp-v1", v1_entries()).json()["id"]
    )

    gate = _Gate()
    monkeypatch.setattr(
        "app.bodypart.dicom_io.read_body_part_examined", gate.wrap(
            bodypart.dicom_io.read_body_part_examined
        )
    )
    resp = submit_dict(client, "bp-stuck", v2_entries())
    gate.wait_entered()
    rebuild_id = resp.json()["id"]

    # Simulate a process restart while the rebuild is provably mid-flight:
    # mark the orphan 'building' row failed BEFORE releasing the gate, so the
    # background thread cannot complete a cutover in between.
    recovered = bodypart.recover_interrupted_rebuilds()
    assert recovered == 1
    gate.release()

    row = client.get(f"/api/v1/body-part/rebuilds/{rebuild_id}").json()
    assert row["status"] == "failed"
    assert "restart" in row["error_detail"]
    # Old active index survived and still answers queries.
    active = client.get("/api/v1/body-part/active").json()
    assert active["version_label"] == "bp-v1"
    assert len(query(client, "center-a", "CHEST").json()) == 1

    # A new replacement can now be started. Restore the real reader without
    # monkeypatch.undo() (which would also revert the temp data-dir config).
    monkeypatch.setattr(
        "app.bodypart.dicom_io.read_body_part_examined",
        bodypart.dicom_io.read_body_part_examined.__wrapped__,
    )
    retry = wait_terminal(
        client, submit_dict(client, "bp-v2", v2_entries()).json()["id"]
    )
    assert retry["status"] == "active"


def test_lifespan_recovers_orphan_rebuild(client, samples_dir):
    # Insert an orphan 'building' row directly, then re-run startup recovery
    # through a brand-new application instance.
    upload(client, "center-a", bp_dir(samples_dir, "center-a")[:1])
    from app.db import immediate_transaction
    with immediate_transaction() as conn:
        cur = conn.execute(
            "INSERT INTO body_part_dicts (version_label, entry_count, mapping_json)"
            " VALUES ('orphan', 1, '{}')"
        )
        conn.execute(
            "INSERT INTO rebuilds (dict_id, status) VALUES (?, 'building')",
            (cur.lastrowid,),
        )

    from fastapi.testclient import TestClient
    from app import main
    with TestClient(main.app) as fresh:
        resp = fresh.get("/api/v1/body-part/rebuilds")
        statuses = [r["status"] for r in resp.json()]
    assert "building" not in statuses
    assert "failed" in statuses


# --------------------------------------------------------------------------- #
# Payload validation
# --------------------------------------------------------------------------- #
def test_dictionary_validation_errors(client):
    # Empty entries.
    r = client.post(
        "/api/v1/body-part/dictionaries",
        json={"version_label": "empty", "entries": []},
    )
    assert r.status_code == 422

    # Duplicate (center, original_value) pair.
    r = submit_dict(client, "dup", [
        {"center_id": "c", "original_value": "CHEST",
         "standard_body_part": "CHEST"},
        {"center_id": "c", "original_value": "CHEST",
         "standard_body_part": "OTHER"},
    ])
    assert r.status_code == 422 and "duplicate" in r.text

    # Empty original value rejected at the boundary.
    r = submit_dict(client, "blank", [
        {"center_id": "c", "original_value": "  ",
         "standard_body_part": "CHEST"},
    ])
    assert r.status_code == 422 and "original_value" in r.text

    # Duplicate version label.
    submit_dict(client, "once", v1_entries()[:1])
    r = submit_dict(client, "once", v1_entries()[:1])
    assert r.status_code == 409


# --------------------------------------------------------------------------- #
# Minimal threading gate used to pin a rebuild inside its DICOM-read phase.
# --------------------------------------------------------------------------- #
class _Gate:
    def __init__(self) -> None:
        import threading
        self._in = threading.Event()
        self._go = threading.Event()
        self._entered = False

    def wrap(self, real):
        def wrapped(path):
            if not self._entered:
                self._entered = True
                self._in.set()
            self._go.wait(timeout=30)
            return real(path)
        wrapped.__wrapped__ = real
        return wrapped

    def wait_entered(self) -> None:
        assert self._in.wait(timeout=10)

    def release(self) -> None:
        self._go.set()
