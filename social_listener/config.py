from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse


BASE_DIR = Path(__file__).resolve().parent.parent


def _load_dotenv() -> None:
    env_path = BASE_DIR / ".env"
    if not env_path.exists():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw.strip())
    except ValueError:
        return default


@dataclass(frozen=True)
class Settings:
    base_dir: Path
    db_path: Path
    database_url: str | None
    database_backend: str
    owned_youtube_channels_path: Path
    secret_key: str
    enable_demo_data: bool
    target_language: str
    target_region: str
    strict_language_filter: bool
    youtube_api_key: str | None
    youtube_max_results: int
    youtube_max_pages: int
    youtube_fetch_comments: bool
    youtube_comment_threads_per_video: int


def load_settings() -> Settings:
    _load_dotenv()
    database_url = (os.getenv("APP_DATABASE_URL") or os.getenv("DATABASE_URL") or "").strip() or None
    parsed_scheme = (urlparse(database_url).scheme if database_url else "").lower()
    database_backend = "postgres" if parsed_scheme in {"postgres", "postgresql"} else "sqlite"
    db_relative = os.getenv("APP_DB_PATH", "data/app.db")
    db_path = (BASE_DIR / db_relative).resolve()
    owned_youtube_channels_relative = os.getenv("APP_OWNED_YOUTUBE_CHANNELS_PATH", "data/owned_youtube_channels.json")
    owned_youtube_channels_path = (BASE_DIR / owned_youtube_channels_relative).resolve()
    return Settings(
        base_dir=BASE_DIR,
        db_path=db_path,
        database_url=database_url,
        database_backend=database_backend,
        owned_youtube_channels_path=owned_youtube_channels_path,
        secret_key=os.getenv("APP_SECRET_KEY", "dev-secret-key"),
        enable_demo_data=_env_bool("APP_ENABLE_DEMO_DATA", True),
        target_language=os.getenv("APP_TARGET_LANGUAGE", "tr").strip() or "tr",
        target_region=os.getenv("APP_TARGET_REGION", "TR").strip().upper() or "TR",
        strict_language_filter=_env_bool("APP_STRICT_LANGUAGE_FILTER", True),
        youtube_api_key=os.getenv("YOUTUBE_API_KEY") or None,
        youtube_max_results=_env_int("YOUTUBE_MAX_RESULTS", 12),
        youtube_max_pages=_env_int("YOUTUBE_MAX_PAGES", 3),
        youtube_fetch_comments=_env_bool("YOUTUBE_FETCH_COMMENTS", True),
        youtube_comment_threads_per_video=_env_int("YOUTUBE_COMMENT_THREADS_PER_VIDEO", 5),
    )
