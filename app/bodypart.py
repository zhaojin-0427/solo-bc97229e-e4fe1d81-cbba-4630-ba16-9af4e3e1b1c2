"""Body-part dictionary service: exact-match classification, background index
rebuild and atomic dictionary/index cutover.

Design overview
---------------
* A dictionary is the complete ``center + raw BodyPartExamined -> standard body
  part`` mapping. Matching is exact and case-sensitive; an empty/blank value or
  a raw value the dictionary does not cover lands in the :data:`UNCLASSIFIED`
  bucket.
* Submitting a dictionary inserts the mapping and a ``rebuilds`` row in the
  ``building`` state, then starts a background thread. A partial unique index
  guarantees at most one ``building`` row, therefore at most one replacement at
  a time, even across the API/thread boundary and across restarts.
* The rebuild reads every stored DICOM object *outside any writer transaction*
  (requirement: DICOM parsing must not occupy the production DB write lock),
  classifies it against the new dictionary and writes classified rows into the
  ``body_part_index_staging`` table through short transactions.
* Ingestion continues while the rebuild runs. Every newly committed instance
  is indexed against the *currently active* dictionary inside the same writer
  transaction (see :func:`index_instance`), so index and instance are committed
  together and retransmits never produce duplicate index rows.
* Cutover runs in one ``BEGIN IMMEDIATE`` transaction. It first back-fills any
  instances that were committed after the rebuild saw them, then verifies the
  staging set covers every instance exactly once, and only then replaces the
  whole index and flips the active-dictionary pointer. Readers therefore see
  either the old dictionary or the new dictionary for any single query -- never
  a mix -- and every instance successfully ingested before the flip is covered.
* If any DICOM object cannot be read, the replacement fails: staging rows are
  removed, the rebuild is marked ``failed`` with the offending instance and the
  reason, and the previously active index stays fully intact.
* On startup, any rebuild still in ``building`` (a process killed mid-rebuild)
  is marked ``failed``; the old active index and all confirmed ingests survive.
"""
from __future__ import annotations

import json
import sqlite3
import threading
from dataclasses import dataclass
from typing import Any

from . import config, dicom_io
from .db import get_connection, immediate_transaction

# Standard bucket for empty values and raw values not covered by a dictionary.
UNCLASSIFIED = "UNCLASSIFIED"

# Maximum number of entries accepted in one submitted dictionary.
MAX_DICT_ENTRIES = 20000

# Process-local guard so a second submission fails fast even before hitting the
# DB-level partial unique index.
_rebuild_lock = threading.Lock()


class BodyPartError(Exception):
    """Client-facing error from the dictionary subsystem."""

    def __init__(self, message: str, http_status: int = 422) -> None:
        super().__init__(message)
        self.http_status = http_status


# --------------------------------------------------------------------------- #
# Classification
# --------------------------------------------------------------------------- #
def normalize_raw_value(raw: str | None) -> str:
    """Normalize a raw BodyPartExamined value for exact matching.

    The mapping keys are normalized the same way at submission time, so matching
    stays exact (including case) while surrounding whitespace never causes a
    spurious miss. An empty/blank value becomes "" and is always unclassified.
    """
    return (raw or "").strip()


def classify(
    mapping: dict[str, dict[str, str]], center_id: str, raw_body_part: str | None
) -> tuple[str, str]:
    """Return ``(normalized_raw_value, standard_body_part)`` for one instance.

    Exact match only. A missing center, an empty value or an unmapped raw value
    yields :data:`UNCLASSIFIED`.
    """
    raw = normalize_raw_value(raw_body_part)
    if not raw:
        return "", UNCLASSIFIED
    standard = mapping.get(center_id, {}).get(raw)
    if standard is None:
        return raw, UNCLASSIFIED
    return raw, standard


def _load_mapping(mapping_json: str) -> dict[str, dict[str, str]]:
    loaded = json.loads(mapping_json)
    if not isinstance(loaded, dict):
        raise BodyPartError("stored mapping is malformed", http_status=500)
    return loaded


# --------------------------------------------------------------------------- #
# Active-dictionary helpers
# --------------------------------------------------------------------------- #
@dataclass
class ActiveDictionary:
    dict_id: int
    rebuild_id: int
    version_label: str
    mapping: dict[str, dict[str, str]]


def get_active_dictionary(conn: sqlite3.Connection) -> ActiveDictionary | None:
    """Return the active dictionary for the given connection, or None.

    ``None`` means no dictionary has ever been activated: the first index is
    not ready and body-part queries must report "not ready".
    """
    row = conn.execute(
        "SELECT state_value FROM app_state WHERE state_key = 'active_rebuild_id'"
    ).fetchone()
    if row is None:
        return None
    rebuild_id = int(row["state_value"])
    rebuild = conn.execute(
        """
        SELECT r.id, r.dict_id, d.version_label, d.mapping_json
        FROM rebuilds r JOIN body_part_dicts d ON d.dict_id = r.dict_id
        WHERE r.id = ? AND r.status = 'active'
        """,
        (rebuild_id,),
    ).fetchone()
    if rebuild is None:
        # Pointer exists but its rebuild is not active: treat as not ready.
        return None
    return ActiveDictionary(
        dict_id=rebuild["dict_id"],
        rebuild_id=rebuild["id"],
        version_label=rebuild["version_label"],
        mapping=_load_mapping(rebuild["mapping_json"]),
    )


def index_instance(
    conn: sqlite3.Connection,
    instance_id: int,
    center_id: str,
    raw_body_part: str | None,
) -> None:
    """Index one freshly ingested instance against the *active* dictionary.

    Must be called inside the ingestion writer transaction so the instance and
    its index row commit atomically. A no-op while no dictionary is active yet;
    the first rebuild will index the instance then.
    """
    active = get_active_dictionary(conn)
    if active is None:
        return
    raw, standard = classify(active.mapping, center_id, raw_body_part)
    conn.execute(
        """
        INSERT OR IGNORE INTO body_part_index
            (instance_id, center_id, dict_id, original_body_part,
             standard_body_part)
        VALUES (?, ?, ?, ?, ?)
        """,
        (instance_id, center_id, active.dict_id, raw or None, standard),
    )


# --------------------------------------------------------------------------- #
# Dictionary submission
# --------------------------------------------------------------------------- #
def validate_entries(entries: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    """Validate a submitted dictionary and build the nested mapping document."""
    if not entries:
        raise BodyPartError("entries must contain at least one mapping")
    if len(entries) > MAX_DICT_ENTRIES:
        raise BodyPartError(
            f"a dictionary may contain at most {MAX_DICT_ENTRIES} entries, "
            f"got {len(entries)}"
        )
    mapping: dict[str, dict[str, str]] = {}
    seen: set[tuple[str, str]] = set()
    for index, entry in enumerate(entries):
        center = (entry.get("center_id") or "").strip()
        raw = (entry.get("original_value") or "").strip()
        standard = (entry.get("standard_body_part") or "").strip()
        if not center:
            raise BodyPartError(f"entry #{index}: center_id is required")
        if not standard:
            raise BodyPartError(
                f"entry #{index}: standard_body_part is required"
            )
        # Empty raw values are valid input semantically (they classify to
        # UNCLASSIFIED), but an explicit empty mapping key is pointless and
        # ambiguous, so it is rejected at the API boundary.
        if not raw:
            raise BodyPartError(
                f"entry #{index}: original_value must not be empty; empty "
                f"values are classified as {UNCLASSIFIED} automatically"
            )
        key = (center, raw)
        if key in seen:
            raise BodyPartError(
                f"duplicate dictionary entry for center {center!r}, "
                f"original_value {raw!r}"
            )
        seen.add(key)
        mapping.setdefault(center, {})[raw] = standard
    return mapping


def submit_dictionary(
    entries: list[dict[str, str]], version_label: str
) -> dict[str, Any]:
    """Validate and persist a new dictionary, then schedule its rebuild.

    Raises :class:`BodyPartError` with 409 when another replacement is already
    running. Returns a serializable rebuild descriptor.
    """
    version_label = (version_label or "").strip()
    if not version_label:
        raise BodyPartError("version_label is required")
    mapping = validate_entries(entries)

    if not _rebuild_lock.acquire(blocking=False):
        raise BodyPartError(
            "another dictionary replacement is already in progress",
            http_status=409,
        )
    try:
        mapping_json = json.dumps(mapping, ensure_ascii=False, sort_keys=True)
        try:
            with immediate_transaction() as conn:
                existing = conn.execute(
                    "SELECT 1 FROM body_part_dicts WHERE version_label = ?",
                    (version_label,),
                ).fetchone()
                if existing is not None:
                    raise BodyPartError(
                        f"version_label {version_label!r} already exists",
                        http_status=409,
                    )
                building = conn.execute(
                    "SELECT id FROM rebuilds WHERE status = 'building'"
                ).fetchone()
                if building is not None:
                    raise BodyPartError(
                        "another dictionary replacement is already in progress",
                        http_status=409,
                    )
                cur = conn.execute(
                    """
                    INSERT INTO body_part_dicts
                        (version_label, entry_count, mapping_json)
                    VALUES (?, ?, ?)
                    """,
                    (version_label, len(entries), mapping_json),
                )
                dict_id = cur.lastrowid
                cur = conn.execute(
                    "INSERT INTO rebuilds (dict_id, status) VALUES (?, 'building')",
                    (dict_id,),
                )
                rebuild_id = cur.lastrowid
        except sqlite3.IntegrityError as exc:
            # Partial unique index / version uniqueness race.
            raise BodyPartError(
                "another dictionary replacement is already in progress "
                f"or the version label conflicts: {exc}",
                http_status=409,
            ) from exc

        thread = threading.Thread(
            target=run_rebuild_safely,
            args=(rebuild_id,),
            name=f"bodypart-rebuild-{rebuild_id}",
            daemon=True,
        )
        thread.start()
        return get_rebuild(rebuild_id)
    finally:
        # The thread performs the work; the lock only serializes submissions,
        # and is released once the building row is durably recorded. The
        # partial unique index remains the durable single-flight guarantee.
        _rebuild_lock.release()


# --------------------------------------------------------------------------- #
# Rebuild execution
# --------------------------------------------------------------------------- #
def _mark_failed(rebuild_id: int, detail: str) -> None:
    with immediate_transaction() as conn:
        conn.execute("DELETE FROM body_part_index_staging WHERE rebuild_id = ?",
                     (rebuild_id,))
        conn.execute(
            """
            UPDATE rebuilds
            SET status = 'failed', error_detail = ?,
                finished_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
            WHERE id = ? AND status = 'building'
            """,
            (detail, rebuild_id),
        )


def run_rebuild_safely(rebuild_id: int) -> None:
    """Thread entry point: never let an exception kill the thread silently."""
    try:
        run_rebuild(rebuild_id)
    except Exception as exc:  # noqa: BLE001 - every failure becomes a status row
        _mark_failed(rebuild_id, f"unexpected rebuild error: {exc!r}")


def run_rebuild(rebuild_id: int) -> None:
    """Rebuild the body-part index for ``rebuild_id`` and cut over atomically.

    DICOM parsing happens on this background thread with no DB writer
    transaction held; staging inserts use their own short transactions.
    """
    conn = get_connection()
    try:
        rebuild = conn.execute(
            "SELECT id, dict_id, status FROM rebuilds WHERE id = ?",
            (rebuild_id,),
        ).fetchone()
    finally:
        conn.close()
    if rebuild is None or rebuild["status"] != "building":
        return

    dict_id = rebuild["dict_id"]
    conn = get_connection()
    try:
        mapping = _load_mapping(
            conn.execute(
                "SELECT mapping_json FROM body_part_dicts WHERE dict_id = ?",
                (dict_id,),
            ).fetchone()["mapping_json"]
        )
    finally:
        conn.close()

    # ---- Phase 1: scan every stored object outside writer transactions. ----
    # Pagination by id keeps the read consistent enough for this purpose; any
    # instance committed after the scan is caught by the pre-cutover backfill.
    errors: list[str] = []
    last_id = 0
    page_size = 200
    while True:
        conn = get_connection()
        try:
            rows = conn.execute(
                """
                SELECT id, center_id, stored_path
                FROM instances WHERE id > ? ORDER BY id LIMIT ?
                """,
                (last_id, page_size),
            ).fetchall()
        finally:
            conn.close()
        if not rows:
            break

        staged: list[tuple] = []
        for row in rows:
            last_id = row["id"]
            absolute = str(config.STORAGE_DIR / row["stored_path"])
            try:
                # No writer transaction is open while the file is parsed.
                raw_value = dicom_io.read_body_part_examined(absolute)
            except Exception as exc:  # noqa: BLE001 - report instance + reason
                errors.append(
                    f"instance id={row['id']} center={row['center_id']} "
                    f"path={row['stored_path']}: {exc}"
                )
                continue
            normalized, standard = classify(
                mapping, row["center_id"], raw_value
            )
            staged.append(
                (rebuild_id, row["id"], row["center_id"], dict_id,
                 normalized or None, standard)
            )

        with immediate_transaction() as write_conn:
            write_conn.executemany(
                """
                INSERT OR REPLACE INTO body_part_index_staging
                    (rebuild_id, instance_id, center_id, dict_id,
                     original_body_part, standard_body_part)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                staged,
            )

    if errors:
        _mark_failed(
            rebuild_id,
            "DICOM read failed for "
            f"{len(errors)} instance(s); replacement aborted, previous index "
            "kept. First errors: " + " | ".join(errors[:10]),
        )
        return

    # ---- Phase 2: converge with concurrent ingestion and cut over. ---------
    # The backfill loop reads the gap without a writer lock, parses the missing
    # files, then takes one short writer transaction for staging. Cutover
    # repeats the check inside its own IMMEDIATE transaction and only commits
    # once staging covers exactly the current instance set.
    for _ in range(1000):  # hard safety bound; normally converges in 1-2 rounds
        with immediate_transaction() as check_conn:
            missing = check_conn.execute(
                """
                SELECT i.id, i.center_id, i.stored_path
                FROM instances i
                LEFT JOIN body_part_index_staging s
                  ON s.rebuild_id = ? AND s.instance_id = i.id
                WHERE s.instance_id IS NULL
                ORDER BY i.id
                """,
                (rebuild_id,),
            ).fetchall()
        if not missing:
            break
        backfill: list[tuple] = []
        for row in missing:
            absolute = str(config.STORAGE_DIR / row["stored_path"])
            try:
                raw_value = dicom_io.read_body_part_examined(absolute)
            except Exception as exc:  # noqa: BLE001
                errors.append(
                    f"instance id={row['id']} center={row['center_id']} "
                    f"path={row['stored_path']}: {exc}"
                )
                continue
            normalized, standard = classify(
                mapping, row["center_id"], raw_value
            )
            backfill.append(
                (rebuild_id, row["id"], row["center_id"], dict_id,
                 normalized or None, standard)
            )
        if errors:
            _mark_failed(
                rebuild_id,
                "DICOM read failed during backfill for "
                f"{len(errors)} instance(s); replacement aborted, previous "
                "index kept. First errors: " + " | ".join(errors[:10]),
            )
            return
        with immediate_transaction() as write_conn:
            write_conn.executemany(
                """
                INSERT OR REPLACE INTO body_part_index_staging
                    (rebuild_id, instance_id, center_id, dict_id,
                     original_body_part, standard_body_part)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                backfill,
            )

    _cutover(rebuild_id, dict_id)


def _cutover(rebuild_id: int, dict_id: int) -> None:
    """Atomically replace the active index and flip the dictionary pointer.

    Convergence / coverage problems mark this rebuild ``failed`` (old index
    kept) and return; they never propagate out of the background thread.
    """
    with immediate_transaction() as conn:
        # A concurrent restart-recovery may have failed this rebuild while
        # the scan was in progress. Never cut over a non-building attempt.
        current = conn.execute(
            "SELECT status FROM rebuilds WHERE id = ?", (rebuild_id,)
        ).fetchone()
        if current is None or current["status"] != "building":
            return

        # Re-check under the writer lock: any instance committed since the
        # last backfill round makes the cutover ineligible this attempt.
        gap = conn.execute(
            """
            SELECT COUNT(*) AS n
            FROM instances i
            LEFT JOIN body_part_index_staging s
              ON s.rebuild_id = ? AND s.instance_id = i.id
            WHERE s.instance_id IS NULL
            """,
            (rebuild_id,),
        ).fetchone()["n"]
        instance_count = conn.execute(
            "SELECT COUNT(*) AS n FROM instances"
        ).fetchone()["n"]
        staged_count = conn.execute(
            "SELECT COUNT(*) AS n FROM body_part_index_staging "
            "WHERE rebuild_id = ?",
            (rebuild_id,),
        ).fetchone()["n"]

    if gap or staged_count != instance_count:
        _mark_failed(
            rebuild_id,
            f"index not convergent at cutover: gap={gap}, "
            f"staged={staged_count}, instances={instance_count}; replacement "
            "aborted, previous index kept",
        )
        return

    with immediate_transaction() as conn:
        # Re-read status under the writer lock in case recovery ran meanwhile.
        current = conn.execute(
            "SELECT status FROM rebuilds WHERE id = ?", (rebuild_id,)
        ).fetchone()
        if current is None or current["status"] != "building":
            return

        # Whole-index replacement inside this single transaction: old index
        # and old pointer are visible to readers right up to COMMIT, after
        # which every query uses the new dictionary only.
        conn.execute("DELETE FROM body_part_index")
        conn.execute(
            """
            INSERT INTO body_part_index
                (instance_id, center_id, dict_id, original_body_part,
                 standard_body_part)
            SELECT instance_id, center_id, dict_id, original_body_part,
                   standard_body_part
            FROM body_part_index_staging
            WHERE rebuild_id = ?
            """,
            (rebuild_id,),
        )
        conn.execute(
            "DELETE FROM body_part_index_staging WHERE rebuild_id = ?",
            (rebuild_id,),
        )
        conn.execute(
            "UPDATE rebuilds SET status = 'superseded' WHERE status = 'active'"
        )
        conn.execute(
            """
            UPDATE rebuilds
            SET status = 'active',
                finished_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
            WHERE id = ?
            """,
            (rebuild_id,),
        )
        conn.execute(
            """
            INSERT INTO app_state (state_key, state_value)
            VALUES ('active_rebuild_id', ?)
            ON CONFLICT (state_key) DO UPDATE SET state_value = excluded.state_value
            """,
            (str(rebuild_id),),
        )


# --------------------------------------------------------------------------- #
# Queries / lifecycle
# --------------------------------------------------------------------------- #
def get_rebuild(rebuild_id: int) -> dict[str, Any]:
    conn = get_connection()
    try:
        row = conn.execute(
            """
            SELECT r.id, r.dict_id, r.status, r.error_detail, r.created_at,
                   r.finished_at, d.version_label, d.entry_count
            FROM rebuilds r JOIN body_part_dicts d ON d.dict_id = r.dict_id
            WHERE r.id = ?
            """,
            (rebuild_id,),
        ).fetchone()
    finally:
        conn.close()
    if row is None:
        raise BodyPartError("rebuild not found", http_status=404)
    return dict(row)


def list_rebuilds(limit: int = 50, offset: int = 0) -> list[dict[str, Any]]:
    conn = get_connection()
    try:
        rows = conn.execute(
            """
            SELECT r.id, r.dict_id, r.status, r.error_detail, r.created_at,
                   r.finished_at, d.version_label, d.entry_count
            FROM rebuilds r JOIN body_part_dicts d ON d.dict_id = r.dict_id
            ORDER BY r.id DESC LIMIT ? OFFSET ?
            """,
            (limit, offset),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def get_active_dictionary_info() -> dict[str, Any] | None:
    conn = get_connection()
    try:
        active = get_active_dictionary(conn)
        if active is None:
            return None
        return {
            "dict_id": active.dict_id,
            "rebuild_id": active.rebuild_id,
            "version_label": active.version_label,
            "entry_count": conn.execute(
                "SELECT entry_count FROM body_part_dicts WHERE dict_id = ?",
                (active.dict_id,),
            ).fetchone()["entry_count"],
            "indexed_instances": conn.execute(
                "SELECT COUNT(*) AS n FROM body_part_index"
            ).fetchone()["n"],
        }
    finally:
        conn.close()


def recover_interrupted_rebuilds() -> int:
    """Mark rebuilds left ``building`` by a crashed process as failed.

    Runs once at startup. The old active index and all committed instances are
    untouched; only the orphaned in-flight attempt is closed out so a new
    replacement can be submitted.
    """
    with immediate_transaction() as conn:
        orphans = conn.execute(
            "SELECT id FROM rebuilds WHERE status = 'building'"
        ).fetchall()
        for orphan in orphans:
            rebuild_id = orphan["id"]
            conn.execute(
                "DELETE FROM body_part_index_staging WHERE rebuild_id = ?",
                (rebuild_id,),
            )
            conn.execute(
                """
                UPDATE rebuilds
                SET status = 'failed',
                    error_detail = ?,
                    finished_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
                WHERE id = ?
                """,
                (
                    "interrupted by process restart; marked failed "
                    "automatically, previous index kept",
                    rebuild_id,
                ),
            )
        return len(orphans)


def search_instances(
    center_id: str, standard_body_part: str, limit: int, offset: int
) -> list[dict[str, Any]]:
    """Return instance ids + download URLs for a center / standard body part."""
    conn = get_connection()
    try:
        active = get_active_dictionary(conn)
        if active is None:
            raise BodyPartError(
                "body-part index is not ready: no dictionary has been "
                "activated yet",
                http_status=503,
            )
        rows = conn.execute(
            """
            SELECT i.id AS instance_id, i.center_id, i.new_sop_uid,
                   b.original_body_part, b.standard_body_part, b.dict_id
            FROM body_part_index b
            JOIN instances i ON i.id = b.instance_id
            WHERE b.dict_id = ? AND b.center_id = ?
              AND b.standard_body_part = ?
            ORDER BY i.id LIMIT ? OFFSET ?
            """,
            (active.dict_id, center_id, standard_body_part, limit, offset),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()
