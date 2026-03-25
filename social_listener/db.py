from __future__ import annotations

import sqlite3
from pathlib import Path

from flask import current_app, g


SCHEMA = """
CREATE TABLE IF NOT EXISTS search_queries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    raw_query TEXT NOT NULL,
    normalized_terms_json TEXT NOT NULL,
    platforms_json TEXT NOT NULL,
    requested_from TEXT,
    requested_to TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS collection_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    raw_query TEXT NOT NULL,
    platforms_json TEXT NOT NULL,
    requested_from TEXT,
    requested_to TEXT,
    status TEXT NOT NULL,
    result_count INTEGER NOT NULL DEFAULT 0,
    warnings_json TEXT NOT NULL,
    summary_json TEXT NOT NULL,
    started_at TEXT NOT NULL,
    finished_at TEXT
);

CREATE TABLE IF NOT EXISTS brand_profiles (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    query_text TEXT NOT NULL,
    platforms_json TEXT NOT NULL,
    requested_from TEXT,
    requested_to TEXT,
    last_result_count INTEGER NOT NULL DEFAULT 0,
    last_opened_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS content_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    platform TEXT NOT NULL,
    source_kind TEXT NOT NULL,
    content_type TEXT NOT NULL,
    external_id TEXT NOT NULL,
    source_name TEXT,
    author_name TEXT,
    title TEXT,
    body_text TEXT,
    normalized_text TEXT NOT NULL,
    thumbnail_url TEXT,
    view_count INTEGER,
    like_count INTEGER,
    dislike_count INTEGER,
    comment_count INTEGER,
    channel_subscriber_count INTEGER,
    content_url TEXT NOT NULL,
    permalink TEXT,
    language TEXT,
    published_at TEXT,
    is_read INTEGER NOT NULL DEFAULT 0,
    read_at TEXT,
    first_seen_at TEXT NOT NULL,
    last_seen_at TEXT NOT NULL,
    raw_payload_json TEXT NOT NULL,
    UNIQUE(platform, external_id)
);

CREATE INDEX IF NOT EXISTS idx_content_platform ON content_items(platform);
CREATE INDEX IF NOT EXISTS idx_content_published_at ON content_items(published_at);
CREATE INDEX IF NOT EXISTS idx_content_last_seen_at ON content_items(last_seen_at);
"""


def init_db_path(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)


def get_db() -> sqlite3.Connection:
    if "db" not in g:
        db_path = current_app.config["SETTINGS"].db_path
        init_db_path(db_path)
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        g.db = conn
    return g.db


def close_db(_: object | None = None) -> None:
    conn = g.pop("db", None)
    if conn is not None:
        conn.close()


def init_schema() -> None:
    db = get_db()
    db.executescript(SCHEMA)
    columns = {row["name"] for row in db.execute("PRAGMA table_info(content_items)").fetchall()}
    if "thumbnail_url" not in columns:
        db.execute("ALTER TABLE content_items ADD COLUMN thumbnail_url TEXT")
    if "view_count" not in columns:
        db.execute("ALTER TABLE content_items ADD COLUMN view_count INTEGER")
    if "like_count" not in columns:
        db.execute("ALTER TABLE content_items ADD COLUMN like_count INTEGER")
    if "dislike_count" not in columns:
        db.execute("ALTER TABLE content_items ADD COLUMN dislike_count INTEGER")
    if "comment_count" not in columns:
        db.execute("ALTER TABLE content_items ADD COLUMN comment_count INTEGER")
    if "channel_subscriber_count" not in columns:
        db.execute("ALTER TABLE content_items ADD COLUMN channel_subscriber_count INTEGER")
    if "is_read" not in columns:
        db.execute("ALTER TABLE content_items ADD COLUMN is_read INTEGER NOT NULL DEFAULT 0")
    if "read_at" not in columns:
        db.execute("ALTER TABLE content_items ADD COLUMN read_at TEXT")
    db.commit()
