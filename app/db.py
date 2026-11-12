"""SQLite persistence layer.

Concurrency model
-----------------
* WAL journal + a 30 s busy timeout allow many concurrent readers alongside a
  single writer. Each HTTP request opens its own connection (sqlite3 objects
  must not be shared across threads) and every batch is committed in one
  ``BEGIN IMMEDIATE`` transaction, so two concurrent batches serialize cleanly
  and either the whole batch lands or nothing does.
* All identity/mapping rows carry a UNIQUE constraint on
  ``(center_id, original_uid)`` and ``(center_id, original_sop_uid, sha256)``;
  a conflicting insert is detected and turned into a batch-level rejection.
"""
from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from typing import Iterator

from . import config
from .config import BUSY_TIMEOUT_MS, ensure_dirs

SCHEMA = """
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS centers (
    center_id   TEXT PRIMARY KEY,
    created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

-- One row per (center, original UID) ever observed. The mapping value itself
-- is derivable with app.uid.remap_uid; the table exists so reference-first
-- uploads can reserve rows before the referenced image arrives.
CREATE TABLE IF NOT EXISTS uid_mappings (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    center_id         TEXT NOT NULL,
    original_uid      TEXT NOT NULL,
    new_uid           TEXT NOT NULL,
    uid_kind          TEXT NOT NULL,                 -- study | series | sop | other_ui
    present           INTEGER NOT NULL DEFAULT 0,    -- 1 once an object carrying this SOP exists
    first_seen_batch  TEXT,
    created_at        TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    UNIQUE (center_id, original_uid)
);
CREATE INDEX IF NOT EXISTS idx_uid_mappings_new
    ON uid_mappings (center_id, new_uid);

-- Ingested DICOM objects (one physical rewritten file per row).
CREATE TABLE IF NOT EXISTS instances (
    id                     INTEGER PRIMARY KEY AUTOINCREMENT,
    center_id              TEXT NOT NULL,
    batch_id               TEXT NOT NULL,
    original_sop_uid       TEXT NOT NULL,
    new_sop_uid            TEXT NOT NULL,
    original_study_uid     TEXT NOT NULL,
    new_study_uid          TEXT NOT NULL,
    original_series_uid    TEXT NOT NULL,
    new_series_uid         TEXT NOT NULL,
    modality               TEXT,
    transfer_syntax_uid    TEXT NOT NULL,
    sha256                 TEXT NOT NULL,
    byte_size              INTEGER NOT NULL,
    stored_path            TEXT NOT NULL,
    created_at             TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    UNIQUE (center_id, original_sop_uid, sha256),
    UNIQUE (new_sop_uid),
    FOREIGN KEY (center_id) REFERENCES centers (center_id)
);
CREATE INDEX IF NOT EXISTS idx_instances_batch ON instances (batch_id);
CREATE INDEX IF NOT EXISTS idx_instances_lookup
    ON instances (center_id, original_sop_uid);

-- Every reference encountered while rewriting, including not-yet-resolved ones.
CREATE TABLE IF NOT EXISTS instance_references (
    id                       INTEGER PRIMARY KEY AUTOINCREMENT,
    center_id                TEXT NOT NULL,
    source_instance_id       INTEGER NOT NULL,
    original_referenced_uid  TEXT NOT NULL,
    new_referenced_uid       TEXT NOT NULL,
    resolved                 INTEGER NOT NULL DEFAULT 0,
    created_at               TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    UNIQUE (source_instance_id, original_referenced_uid),
    FOREIGN KEY (source_instance_id) REFERENCES instances (id)
);
CREATE INDEX IF NOT EXISTS idx_references_pending
    ON instance_references (center_id, resolved);

CREATE TABLE IF NOT EXISTS batches (
    batch_id     TEXT PRIMARY KEY,
    center_id    TEXT NOT NULL,
    status       TEXT NOT NULL CHECK (status IN ('committed', 'rejected')),
    file_count   INTEGER NOT NULL,
    reason       TEXT,
    created_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    FOREIGN KEY (center_id) REFERENCES centers (center_id)
);
CREATE INDEX IF NOT EXISTS idx_batches_center ON batches (center_id);

-- ----------------------------------------------------------------------- --
-- Body-part dictionary / index subsystem
-- ----------------------------------------------------------------------- --

-- A complete "center + raw value -> standard body part" dictionary. The whole
-- mapping is stored as one JSON document so a dictionary is always applied as
-- an indivisible unit together with the index built from it.
CREATE TABLE IF NOT EXISTS body_part_dicts (
    dict_id       INTEGER PRIMARY KEY AUTOINCREMENT,
    version_label TEXT NOT NULL UNIQUE,
    entry_count   INTEGER NOT NULL,
    mapping_json  TEXT NOT NULL,
    created_at    TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

-- One row per replacement attempt. At most one row may be in 'building' at a
-- time (enforced by the partial unique index below); 'active' rows are kept
-- for history while older active rows become 'superseded'.
CREATE TABLE IF NOT EXISTS rebuilds (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    dict_id       INTEGER NOT NULL,
    status        TEXT NOT NULL
                  CHECK (status IN ('building', 'active', 'superseded', 'failed')),
    error_detail  TEXT,
    created_at    TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    finished_at   TEXT,
    FOREIGN KEY (dict_id) REFERENCES body_part_dicts (dict_id)
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_rebuilds_single_building
    ON rebuilds (status) WHERE status = 'building';
CREATE INDEX IF NOT EXISTS idx_rebuilds_status ON rebuilds (status);

-- The body-part index of the *currently active* dictionary only. Rows of a
-- dictionary under construction live in body_part_index_staging until the
-- atomic cutover; failed rebuilds never touch this table.
CREATE TABLE IF NOT EXISTS body_part_index (
    instance_id         INTEGER PRIMARY KEY,
    center_id           TEXT NOT NULL,
    dict_id             INTEGER NOT NULL,
    original_body_part  TEXT,
    standard_body_part  TEXT NOT NULL,
    FOREIGN KEY (instance_id) REFERENCES instances (id),
    FOREIGN KEY (dict_id) REFERENCES body_part_dicts (dict_id)
);
CREATE INDEX IF NOT EXISTS idx_bpi_lookup
    ON body_part_index (center_id, standard_body_part);

-- Staging area of a rebuild. The rebuild_id column lets a restarted/failed
-- attempt clean up its own rows without affecting any other attempt.
CREATE TABLE IF NOT EXISTS body_part_index_staging (
    rebuild_id          INTEGER NOT NULL,
    instance_id         INTEGER NOT NULL,
    center_id           TEXT NOT NULL,
    dict_id             INTEGER NOT NULL,
    original_body_part  TEXT,
    standard_body_part  TEXT NOT NULL,
    PRIMARY KEY (rebuild_id, instance_id),
    FOREIGN KEY (rebuild_id) REFERENCES rebuilds (id)
);
CREATE INDEX IF NOT EXISTS idx_bpis_rebuild
    ON body_part_index_staging (rebuild_id, instance_id);

-- Single-row key/value table for small cross-restart state (currently only the
-- active dictionary/rebuild pointer).
CREATE TABLE IF NOT EXISTS app_state (
    state_key   TEXT PRIMARY KEY,
    state_value TEXT NOT NULL
);
"""


def get_connection() -> sqlite3.Connection:
    """Open a SQLite connection with safe pragmas and row access.

    ``config.DB_PATH`` is read at call time (not bound at import) so tests can
    redirect the data directory via ``monkeypatch`` for every thread, including
    background rebuild threads.
    """
    ensure_dirs()
    conn = sqlite3.connect(
        config.DB_PATH,
        timeout=BUSY_TIMEOUT_MS / 1000.0,
        isolation_level=None,  # transactions managed explicitly below
        check_same_thread=False,
    )
    conn.row_factory = sqlite3.Row
    conn.execute(f"PRAGMA busy_timeout = {BUSY_TIMEOUT_MS}")
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    return conn


def init_db() -> None:
    conn = get_connection()
    try:
        conn.executescript(SCHEMA)
    finally:
        conn.close()


@contextmanager
def immediate_transaction() -> Iterator[sqlite3.Connection]:
    """Acquire a writer lock up front; commit on success, roll back on error."""
    conn = get_connection()
    try:
        conn.execute("BEGIN IMMEDIATE")
        yield conn
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.close()
