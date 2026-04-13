from __future__ import annotations

import sqlite3
from pathlib import Path

from flask import current_app, g
from psycopg import IntegrityError as PsycopgIntegrityError
from psycopg import connect as pg_connect
from psycopg.rows import dict_row

from .config import Settings

INTEGRITY_ERRORS = (sqlite3.IntegrityError, PsycopgIntegrityError)

SQLITE_SCHEMA = """
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
    official_youtube_url TEXT,
    official_facebook_url TEXT,
    requested_from TEXT,
    requested_to TEXT,
    last_result_count INTEGER NOT NULL DEFAULT 0,
    last_opened_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS external_api_sources (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    platform TEXT NOT NULL,
    method TEXT NOT NULL DEFAULT 'GET',
    url_template TEXT NOT NULL,
    headers_json TEXT NOT NULL DEFAULT '{}',
    body_template TEXT,
    results_path TEXT,
    field_mapping_json TEXT NOT NULL,
    pagination_json TEXT NOT NULL DEFAULT '{}',
    is_enabled INTEGER NOT NULL DEFAULT 1,
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

POSTGRES_BOOTSTRAP_SCHEMA = """
CREATE TABLE IF NOT EXISTS search_queries (
    id BIGSERIAL PRIMARY KEY,
    raw_query TEXT NOT NULL,
    normalized_terms_json TEXT NOT NULL,
    platforms_json TEXT NOT NULL,
    requested_from TEXT,
    requested_to TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS collection_runs (
    id BIGSERIAL PRIMARY KEY,
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
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    query_text TEXT NOT NULL,
    platforms_json TEXT NOT NULL,
    official_youtube_url TEXT,
    official_facebook_url TEXT,
    requested_from TEXT,
    requested_to TEXT,
    last_result_count INTEGER NOT NULL DEFAULT 0,
    last_opened_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS external_api_sources (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    platform TEXT NOT NULL,
    method TEXT NOT NULL DEFAULT 'GET',
    url_template TEXT NOT NULL,
    headers_json TEXT NOT NULL DEFAULT '{}',
    body_template TEXT,
    results_path TEXT,
    field_mapping_json TEXT NOT NULL,
    pagination_json TEXT NOT NULL DEFAULT '{}',
    is_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS content_items (
    id BIGSERIAL PRIMARY KEY,
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
    is_read BOOLEAN NOT NULL DEFAULT FALSE,
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

POSTGRES_MIGRATIONS = """
ALTER TABLE brand_profiles ADD COLUMN IF NOT EXISTS official_youtube_url TEXT;
ALTER TABLE brand_profiles ADD COLUMN IF NOT EXISTS official_facebook_url TEXT;
ALTER TABLE external_api_sources ADD COLUMN IF NOT EXISTS pagination_json TEXT NOT NULL DEFAULT '{}';
ALTER TABLE content_items ADD COLUMN IF NOT EXISTS thumbnail_url TEXT;
ALTER TABLE content_items ADD COLUMN IF NOT EXISTS view_count INTEGER;
ALTER TABLE content_items ADD COLUMN IF NOT EXISTS like_count INTEGER;
ALTER TABLE content_items ADD COLUMN IF NOT EXISTS dislike_count INTEGER;
ALTER TABLE content_items ADD COLUMN IF NOT EXISTS comment_count INTEGER;
ALTER TABLE content_items ADD COLUMN IF NOT EXISTS channel_subscriber_count INTEGER;
ALTER TABLE content_items ADD COLUMN IF NOT EXISTS is_read BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE content_items ADD COLUMN IF NOT EXISTS read_at TEXT;
"""


def _convert_query(query: str, backend: str) -> str:
    if backend == "postgres":
        return query.replace("%", "%%").replace("?", "%s")
    return query


class DatabaseConnection:
    def __init__(self, raw_connection: object, backend: str) -> None:
        self.raw_connection = raw_connection
        self.backend = backend

    def execute(self, query: str, params: tuple[object, ...] | list[object] | None = None):
        bound_params = tuple(params or ())
        return self.raw_connection.execute(_convert_query(query, self.backend), bound_params)

    def executescript(self, script: str) -> None:
        if self.backend == "sqlite":
            self.raw_connection.executescript(script)
            return

        for statement in (piece.strip() for piece in script.split(";")):
            if statement:
                self.raw_connection.execute(statement)

    def insert_and_get_id(self, query: str, params: tuple[object, ...] | list[object] | None = None) -> int:
        if self.backend == "postgres":
            cursor = self.execute(f"{query.strip().rstrip(';')} RETURNING id", params)
            row = cursor.fetchone()
            if row is None:
                raise RuntimeError("RETURNING id sonucu bos geldi.")
            return int(row["id"])

        cursor = self.execute(query, params)
        return int(cursor.lastrowid)

    def commit(self) -> None:
        self.raw_connection.commit()

    def rollback(self) -> None:
        self.raw_connection.rollback()

    def close(self) -> None:
        self.raw_connection.close()


def init_db_path(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)


def _create_sqlite_connection(settings: Settings) -> DatabaseConnection:
    init_db_path(settings.db_path)
    conn = sqlite3.connect(settings.db_path)
    conn.row_factory = sqlite3.Row
    return DatabaseConnection(conn, "sqlite")


def _create_postgres_connection(settings: Settings) -> DatabaseConnection:
    if not settings.database_url:
        raise RuntimeError("Postgres secildi ancak APP_DATABASE_URL tanimli degil.")
    conn = pg_connect(
        settings.database_url,
        row_factory=dict_row,
        prepare_threshold=None,
    )
    conn.execute("SET statement_timeout TO 0")
    conn.execute("SET lock_timeout TO 0")
    return DatabaseConnection(conn, "postgres")


def get_db() -> DatabaseConnection:
    if "db" not in g:
        settings: Settings = current_app.config["SETTINGS"]
        if settings.database_backend == "postgres":
            g.db = _create_postgres_connection(settings)
        else:
            g.db = _create_sqlite_connection(settings)
    return g.db


def close_db(_: object | None = None) -> None:
    conn = g.pop("db", None)
    if conn is not None:
        conn.close()


def init_schema() -> None:
    db = get_db()
    if db.backend == "postgres":
        existing_content_items = db.execute(
            "SELECT to_regclass('public.content_items') AS content_items"
        ).fetchone()
        db.executescript(POSTGRES_BOOTSTRAP_SCHEMA)
        if existing_content_items and existing_content_items.get("content_items"):
            db.executescript(POSTGRES_MIGRATIONS)
    else:
        db.executescript(SQLITE_SCHEMA)
        columns = {row["name"] for row in db.execute("PRAGMA table_info(content_items)").fetchall()}
        brand_columns = {row["name"] for row in db.execute("PRAGMA table_info(brand_profiles)").fetchall()}
        source_columns = {row["name"] for row in db.execute("PRAGMA table_info(external_api_sources)").fetchall()}
        if "official_youtube_url" not in brand_columns:
            db.execute("ALTER TABLE brand_profiles ADD COLUMN official_youtube_url TEXT")
        if "official_facebook_url" not in brand_columns:
            db.execute("ALTER TABLE brand_profiles ADD COLUMN official_facebook_url TEXT")
        if "pagination_json" not in source_columns:
            db.execute("ALTER TABLE external_api_sources ADD COLUMN pagination_json TEXT NOT NULL DEFAULT '{}'")
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
