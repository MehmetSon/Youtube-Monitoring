from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from typing import Iterable

from .db import get_db


TURKISH_ASCII_MAP = str.maketrans(
    {
        "ç": "c",
        "ğ": "g",
        "ı": "i",
        "ö": "o",
        "ş": "s",
        "ü": "u",
        "Ç": "c",
        "Ğ": "g",
        "İ": "i",
        "I": "i",
        "Ö": "o",
        "Ş": "s",
        "Ü": "u",
    }
)


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


DEFAULT_BRAND_PROFILES = [
    {
        "name": "CarrefourSA",
        "query_text": "carrefoursa, karfur, carrefur",
        "platforms": ["youtube"],
        "official_youtube_url": "https://www.youtube.com/channel/UCGsFWBIV1mQBmOl919Q606w",
        "official_facebook_url": None,
        "requested_from": None,
        "requested_to": None,
    },
    {
        "name": "Trendyol",
        "query_text": "trendyol",
        "platforms": ["youtube"],
        "official_youtube_url": "https://www.youtube.com/channel/UCWUkAPLGDjfsH-_LCEOGOEQ",
        "official_facebook_url": None,
        "requested_from": None,
        "requested_to": None,
    },
]


def _normalize_search_text(value: str) -> str:
    return " ".join(value.lower().replace("\n", " ").replace("\t", " ").split())


def _search_tokens(value: str) -> list[str]:
    return re.findall(r"[0-9a-zçğıöşü]+", value)


def _term_like_patterns(term: str) -> list[str]:
    variants: list[str] = []

    normalized = _normalize_search_text(term)
    folded = normalized.translate(TURKISH_ASCII_MAP)
    for candidate in (normalized, folded):
        if candidate and candidate not in variants:
            variants.append(candidate)

    patterns: list[str] = []
    for candidate in variants:
        tokens = _search_tokens(candidate)
        if not tokens:
            continue

        wildcard_pattern = "%" + "%".join(tokens) + "%"
        for pattern in (f"%{candidate}%", wildcard_pattern):
            if pattern not in patterns:
                patterns.append(pattern)

    return patterns


def _normalize_platforms(platforms: Iterable[str] | None) -> list[str]:
    if not platforms:
        return ["youtube"]
    normalized: list[str] = []
    for platform in platforms:
        cleaned = str(platform).strip().lower()
        if cleaned and cleaned not in normalized:
            normalized.append(cleaned)
    return normalized or ["youtube"]


def _brand_row_to_dict(row: object | None) -> dict[str, object] | None:
    if row is None:
        return None
    payload = dict(row)
    payload["platforms"] = json.loads(payload.pop("platforms_json") or "[]")
    return payload


def _external_api_source_row_to_dict(row: object | None) -> dict[str, object] | None:
    if row is None:
        return None
    payload = dict(row)
    payload["headers"] = json.loads(payload.pop("headers_json") or "{}")
    payload["field_mapping"] = json.loads(payload.pop("field_mapping_json") or "{}")
    payload["pagination"] = json.loads(payload.pop("pagination_json") or "{}")
    payload["is_enabled"] = bool(payload.get("is_enabled"))
    return payload


def ensure_default_brand_profiles() -> None:
    db = get_db()
    now = utcnow_iso()
    for brand in DEFAULT_BRAND_PROFILES:
        db.execute(
            """
            INSERT INTO brand_profiles (
                name,
                query_text,
                platforms_json,
                official_youtube_url,
                official_facebook_url,
                requested_from,
                requested_to,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(name) DO UPDATE SET
                official_youtube_url = CASE
                    WHEN brand_profiles.official_youtube_url IS NULL OR brand_profiles.official_youtube_url = ''
                    THEN excluded.official_youtube_url
                    ELSE brand_profiles.official_youtube_url
                END,
                official_facebook_url = CASE
                    WHEN brand_profiles.official_facebook_url IS NULL OR brand_profiles.official_facebook_url = ''
                    THEN excluded.official_facebook_url
                    ELSE brand_profiles.official_facebook_url
                END
            """,
            (
                brand["name"],
                brand["query_text"],
                json.dumps(_normalize_platforms(brand["platforms"])),
                brand.get("official_youtube_url"),
                brand.get("official_facebook_url"),
                brand.get("requested_from"),
                brand.get("requested_to"),
                now,
                now,
            ),
        )
    db.commit()


def list_brand_profiles() -> list[dict[str, object]]:
    db = get_db()
    rows = db.execute(
        """
        SELECT
            id,
            name,
            query_text,
            platforms_json,
            official_youtube_url,
            official_facebook_url,
            requested_from,
            requested_to,
            last_result_count,
            last_opened_at,
            created_at,
            updated_at
        FROM brand_profiles
        ORDER BY lower(name) ASC
        """
    ).fetchall()
    return [_brand_row_to_dict(row) for row in rows if row is not None]


def get_brand_profile(brand_id: int) -> dict[str, object] | None:
    db = get_db()
    row = db.execute(
        """
        SELECT
            id,
            name,
            query_text,
            platforms_json,
            official_youtube_url,
            official_facebook_url,
            requested_from,
            requested_to,
            last_result_count,
            last_opened_at,
            created_at,
            updated_at
        FROM brand_profiles
        WHERE id = ?
        """,
        (brand_id,),
    ).fetchone()
    return _brand_row_to_dict(row)


def create_brand_profile(
    *,
    name: str,
    query_text: str,
    platforms: Iterable[str] | None,
    official_youtube_url: str | None,
    official_facebook_url: str | None,
    requested_from: str | None,
    requested_to: str | None,
) -> dict[str, object]:
    db = get_db()
    now = utcnow_iso()
    brand_id = db.insert_and_get_id(
        """
        INSERT INTO brand_profiles (
            name,
            query_text,
            platforms_json,
            official_youtube_url,
            official_facebook_url,
            requested_from,
            requested_to,
            created_at,
            updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            name.strip(),
            query_text.strip(),
            json.dumps(_normalize_platforms(platforms)),
            (official_youtube_url or "").strip() or None,
            (official_facebook_url or "").strip() or None,
            requested_from,
            requested_to,
            now,
            now,
        ),
    )
    db.commit()
    return get_brand_profile(brand_id) or {}


def update_brand_profile(
    *,
    brand_id: int,
    name: str,
    query_text: str,
    platforms: Iterable[str] | None,
    official_youtube_url: str | None,
    official_facebook_url: str | None,
    requested_from: str | None,
    requested_to: str | None,
) -> dict[str, object] | None:
    db = get_db()
    db.execute(
        """
        UPDATE brand_profiles
        SET
            name = ?,
            query_text = ?,
            platforms_json = ?,
            official_youtube_url = ?,
            official_facebook_url = ?,
            requested_from = ?,
            requested_to = ?,
            updated_at = ?
        WHERE id = ?
        """,
        (
            name.strip(),
            query_text.strip(),
            json.dumps(_normalize_platforms(platforms)),
            (official_youtube_url or "").strip() or None,
            (official_facebook_url or "").strip() or None,
            requested_from,
            requested_to,
            utcnow_iso(),
            brand_id,
        ),
    )
    db.commit()
    return get_brand_profile(brand_id)


def delete_brand_profile(brand_id: int) -> bool:
    db = get_db()
    cursor = db.execute(
        """
        DELETE FROM brand_profiles
        WHERE id = ?
        """,
        (brand_id,),
    )
    db.commit()
    return bool(getattr(cursor, "rowcount", 0))


def list_external_api_sources(*, platform: str | None = None, enabled_only: bool = False) -> list[dict[str, object]]:
    db = get_db()
    where_parts: list[str] = []
    params: list[object] = []
    if platform:
        where_parts.append("platform = ?")
        params.append(platform.strip().lower())
    if enabled_only:
        where_parts.append("is_enabled = ?")
        params.append(True)

    where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
    rows = db.execute(
        f"""
        SELECT
            id,
            name,
            platform,
            method,
            url_template,
            headers_json,
            body_template,
            results_path,
            field_mapping_json,
            pagination_json,
            is_enabled,
            created_at,
            updated_at
        FROM external_api_sources
        {where_sql}
        ORDER BY lower(platform) ASC, lower(name) ASC
        """,
        tuple(params),
    ).fetchall()
    return [_external_api_source_row_to_dict(row) for row in rows if row is not None]


def create_external_api_source(
    *,
    name: str,
    platform: str,
    method: str,
    url_template: str,
    headers: dict[str, object] | None,
    body_template: str | None,
    results_path: str | None,
    field_mapping: dict[str, object],
    pagination: dict[str, object] | None,
    is_enabled: bool,
) -> dict[str, object]:
    db = get_db()
    now = utcnow_iso()
    source_id = db.insert_and_get_id(
        """
        INSERT INTO external_api_sources (
            name,
            platform,
            method,
            url_template,
            headers_json,
            body_template,
            results_path,
            field_mapping_json,
            pagination_json,
            is_enabled,
            created_at,
            updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            name.strip(),
            platform.strip().lower(),
            method.strip().upper(),
            url_template.strip(),
            json.dumps(headers or {}, ensure_ascii=False),
            (body_template or "").strip() or None,
            (results_path or "").strip() or None,
            json.dumps(field_mapping or {}, ensure_ascii=False),
            json.dumps(pagination or {}, ensure_ascii=False),
            is_enabled,
            now,
            now,
        ),
    )
    db.commit()
    row = db.execute(
        """
        SELECT
            id,
            name,
            platform,
            method,
            url_template,
            headers_json,
            body_template,
            results_path,
            field_mapping_json,
            pagination_json,
            is_enabled,
            created_at,
            updated_at
        FROM external_api_sources
        WHERE id = ?
        """,
        (source_id,),
    ).fetchone()
    return _external_api_source_row_to_dict(row) or {}
    

def set_external_api_source_enabled(source_id: int, is_enabled: bool) -> dict[str, object] | None:
    db = get_db()
    db.execute(
        """
        UPDATE external_api_sources
        SET is_enabled = ?, updated_at = ?
        WHERE id = ?
        """,
        (is_enabled, utcnow_iso(), source_id),
    )
    db.commit()
    row = db.execute(
        """
        SELECT
            id,
            name,
            platform,
            method,
            url_template,
            headers_json,
            body_template,
            results_path,
            field_mapping_json,
            pagination_json,
            is_enabled,
            created_at,
            updated_at
        FROM external_api_sources
        WHERE id = ?
        """,
        (source_id,),
    ).fetchone()
    return _external_api_source_row_to_dict(row)


def delete_external_api_source(source_id: int) -> bool:
    db = get_db()
    cursor = db.execute(
        """
        DELETE FROM external_api_sources
        WHERE id = ?
        """,
        (source_id,),
    )
    db.commit()
    return bool(getattr(cursor, "rowcount", 0))


def touch_brand_profile(brand_id: int, result_count: int | None = None) -> dict[str, object] | None:
    db = get_db()
    if result_count is None:
        db.execute(
            """
            UPDATE brand_profiles
            SET last_opened_at = ?, updated_at = ?
            WHERE id = ?
            """,
            (utcnow_iso(), utcnow_iso(), brand_id),
        )
    else:
        db.execute(
            """
            UPDATE brand_profiles
            SET last_result_count = ?, last_opened_at = ?, updated_at = ?
            WHERE id = ?
            """,
            (result_count, utcnow_iso(), utcnow_iso(), brand_id),
        )
    db.commit()
    return get_brand_profile(brand_id)


def log_query(raw_query: str, normalized_terms: list[str], platforms: list[str], requested_from: str | None, requested_to: str | None) -> None:
    db = get_db()
    db.execute(
        """
        INSERT INTO search_queries (raw_query, normalized_terms_json, platforms_json, requested_from, requested_to, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            raw_query,
            json.dumps(normalized_terms),
            json.dumps(platforms),
            requested_from,
            requested_to,
            utcnow_iso(),
        ),
    )
    db.commit()


def start_collection_run(raw_query: str, platforms: list[str], requested_from: str | None, requested_to: str | None) -> int:
    db = get_db()
    run_id = db.insert_and_get_id(
        """
        INSERT INTO collection_runs (raw_query, platforms_json, requested_from, requested_to, status, warnings_json, summary_json, started_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            raw_query,
            json.dumps(platforms),
            requested_from,
            requested_to,
            "running",
            json.dumps([]),
            json.dumps({}),
            utcnow_iso(),
        ),
    )
    db.commit()
    return run_id


def finish_collection_run(run_id: int, status: str, result_count: int, warnings: list[str], summary: dict[str, object]) -> None:
    db = get_db()
    db.execute(
        """
        UPDATE collection_runs
        SET status = ?, result_count = ?, warnings_json = ?, summary_json = ?, finished_at = ?
        WHERE id = ?
        """,
        (
            status,
            result_count,
            json.dumps(warnings),
            json.dumps(summary),
            utcnow_iso(),
            run_id,
        ),
    )
    db.commit()


def set_item_read_state(item_id: int, is_read: bool) -> dict[str, object] | None:
    db = get_db()
    read_at = utcnow_iso() if is_read else None
    db.execute(
        """
        UPDATE content_items
        SET is_read = ?, read_at = ?
        WHERE id = ?
        """,
        (is_read, read_at, item_id),
    )
    db.commit()
    row = db.execute(
        """
        SELECT id, is_read, read_at
        FROM content_items
        WHERE id = ?
        """,
        (item_id,),
    ).fetchone()
    if row is None:
        return None
    return dict(row)


def upsert_content_items(items: Iterable[dict[str, object]]) -> int:
    db = get_db()
    affected = 0
    now = utcnow_iso()
    for item in items:
        db.execute(
            """
            INSERT INTO content_items (
                platform,
                source_kind,
                content_type,
                external_id,
                source_name,
                author_name,
                title,
                body_text,
                normalized_text,
                thumbnail_url,
                view_count,
                like_count,
                dislike_count,
                comment_count,
                channel_subscriber_count,
                content_url,
                permalink,
                language,
                published_at,
                first_seen_at,
                last_seen_at,
                raw_payload_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(platform, external_id)
            DO UPDATE SET
                source_kind = excluded.source_kind,
                content_type = excluded.content_type,
                source_name = excluded.source_name,
                author_name = excluded.author_name,
                title = excluded.title,
                body_text = excluded.body_text,
                normalized_text = excluded.normalized_text,
                thumbnail_url = excluded.thumbnail_url,
                view_count = excluded.view_count,
                like_count = excluded.like_count,
                dislike_count = excluded.dislike_count,
                comment_count = excluded.comment_count,
                channel_subscriber_count = excluded.channel_subscriber_count,
                content_url = excluded.content_url,
                permalink = excluded.permalink,
                language = excluded.language,
                published_at = excluded.published_at,
                last_seen_at = ?,
                raw_payload_json = excluded.raw_payload_json
            """,
            (
                item["platform"],
                item["source_kind"],
                item["content_type"],
                item["external_id"],
                item.get("source_name"),
                item.get("author_name"),
                item.get("title"),
                item.get("body_text"),
                item["normalized_text"],
                item.get("thumbnail_url"),
                item.get("view_count"),
                item.get("like_count"),
                item.get("dislike_count"),
                item.get("comment_count"),
                item.get("channel_subscriber_count"),
                item["content_url"],
                item.get("permalink"),
                item.get("language"),
                item.get("published_at"),
                item.get("first_seen_at", now),
                item.get("last_seen_at", now),
                json.dumps(item.get("raw_payload", {})),
                now,
            ),
        )
        affected += 1
    db.commit()
    return affected


def search_content(
    terms: list[str],
    platforms: list[str],
    requested_from: str | None,
    requested_to: str | None,
    include_demo: bool,
    limit: int = 100,
) -> list[dict[str, object]]:
    db = get_db()
    where_parts = []
    params: list[object] = []

    if terms:
        term_clauses = []
        for term in terms:
            variant_clauses = []
            for pattern in _term_like_patterns(term):
                variant_clauses.append("normalized_text LIKE ?")
                params.append(pattern)
            if variant_clauses:
                term_clauses.append("(" + " OR ".join(variant_clauses) + ")")
        if term_clauses:
            where_parts.append("((source_kind LIKE 'owned-%') OR (" + " OR ".join(term_clauses) + "))")

    if platforms:
        placeholders = ", ".join(["?"] * len(platforms))
        where_parts.append(f"platform IN ({placeholders})")
        params.extend(platforms)

    if requested_from:
        where_parts.append("published_at >= ?")
        params.append(requested_from)

    if requested_to:
        where_parts.append("published_at <= ?")
        params.append(requested_to)

    if not include_demo:
        where_parts.append("source_kind != ?")
        params.append("demo")

    where_sql = " WHERE " + " AND ".join(where_parts) if where_parts else ""
    params.append(limit)

    rows = db.execute(
        f"""
        SELECT
            id,
            platform,
            source_kind,
            content_type,
            external_id,
            source_name,
            author_name,
            title,
            body_text,
            thumbnail_url,
            view_count,
            like_count,
            dislike_count,
            comment_count,
            channel_subscriber_count,
            content_url,
            permalink,
            language,
            published_at,
            is_read,
            read_at,
            first_seen_at,
            last_seen_at
        FROM content_items
        {where_sql}
        ORDER BY COALESCE(published_at, last_seen_at) DESC
        LIMIT ?
        """,
        params,
    ).fetchall()

    return [dict(row) for row in rows]
