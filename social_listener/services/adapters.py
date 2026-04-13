from __future__ import annotations

import hashlib
import html
import json
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qsl, quote, quote_plus, urlencode, urlparse, urlunparse
from urllib.request import Request, urlopen

from langdetect import DetectorFactory, LangDetectException, detect

from ..config import Settings
from ..repository import list_external_api_sources

DetectorFactory.seed = 0


def _read_http_error_body(exc: HTTPError) -> str:
    try:
        return exc.read().decode("utf-8", errors="replace")
    except Exception:
        return ""


def _build_apify_http_error(actor_id: str, exc: HTTPError, *, action: str) -> str:
    body = _read_http_error_body(exc)
    try:
        payload = json.loads(body) if body else {}
    except json.JSONDecodeError:
        payload = {}

    error = payload.get("error") if isinstance(payload, dict) else None
    error_type = str(error.get("type") or "").strip() if isinstance(error, dict) else ""
    error_message = str(error.get("message") or "").strip() if isinstance(error, dict) else ""
    normalized = f"{error_type} {error_message}".lower()

    if "monthly usage hard limit exceeded" in normalized:
        return f"Apify {actor_id}: aylik Apify limiti doldu."

    if "insufficient" in normalized or "forbidden" in normalized:
        return f"Apify {actor_id}: erisim izni reddedildi ({exc.code})."

    if error_message:
        return f"Apify {actor_id}: {action} basarisiz ({exc.code}) - {error_message}."

    return f"Apify {actor_id}: {action} basarisiz ({exc.code})."


def _is_apify_quota_warning(message: str) -> bool:
    return "aylik apify limiti doldu" in normalize_text(message)


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def isoformat(dt: datetime) -> str:
    return dt.replace(microsecond=0).isoformat()


def normalize_text(value: str) -> str:
    return " ".join(value.lower().replace("\n", " ").replace("\t", " ").split())


def build_normalized_text(*parts: str | None) -> str:
    joined = " ".join(part for part in parts if part)
    return normalize_text(joined)


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


def extract_thumbnail_url(thumbnails: dict[str, object] | None) -> str | None:
    if not thumbnails:
        return None
    for key in ("maxres", "standard", "high", "medium", "default"):
        candidate = thumbnails.get(key)
        if isinstance(candidate, dict) and candidate.get("url"):
            return str(candidate["url"])
    return None


def fold_for_match(value: str | None) -> str:
    if not value:
        return ""
    unescaped = html.unescape(str(value))
    return normalize_text(unescaped).translate(TURKISH_ASCII_MAP)


def build_match_text(item: dict[str, object]) -> str:
    return " ".join(
        str(part)
        for part in [
            item.get("title") or "",
            item.get("body_text") or "",
        ]
        if part
    )


def term_matches_text(term: str, text: str) -> bool:
    folded_term = fold_for_match(term)
    folded_text = fold_for_match(text)
    if not folded_term or not folded_text:
        return False

    tokens = re.findall(r"[a-z0-9]+", folded_term)
    if not tokens:
        return False

    if len(tokens) == 1:
        pattern = rf"(?<![a-z0-9]){re.escape(tokens[0])}(?![a-z0-9])"
    else:
        pattern = rf"(?<![a-z0-9])" + r"[\W_]+".join(re.escape(token) for token in tokens) + r"(?![a-z0-9])"

    return re.search(pattern, folded_text) is not None


def item_matches_terms(item: dict[str, object], terms: list[str]) -> bool:
    if not terms:
        return True
    source_kind = str(item.get("source_kind") or "")
    if source_kind.startswith("owned-"):
        return True

    match_text = build_match_text(item)
    if not match_text:
        return False

    return any(term_matches_text(term, match_text) for term in terms)


TURKISH_STOPWORDS = {
    "acaba",
    "ama",
    "aslinda",
    "az",
    "bazı",
    "belki",
    "beni",
    "benim",
    "beri",
    "bile",
    "bir",
    "biri",
    "birkaç",
    "birsey",
    "birçok",
    "biz",
    "bize",
    "bizi",
    "bu",
    "buna",
    "bunu",
    "burada",
    "cok",
    "çok",
    "çünkü",
    "da",
    "daha",
    "de",
    "defa",
    "diye",
    "en",
    "gibi",
    "hangi",
    "hani",
    "hep",
    "hepsi",
    "her",
    "hiç",
    "icin",
    "için",
    "ile",
    "ise",
    "işte",
    "kadar",
    "kendi",
    "kez",
    "ki",
    "kim",
    "madem",
    "mi",
    "mı",
    "mu",
    "mü",
    "nasıl",
    "neden",
    "ne",
    "nerede",
    "niye",
    "o",
    "olarak",
    "onlar",
    "sanki",
    "şey",
    "şu",
    "tabi",
    "tabii",
    "ve",
    "veya",
    "ya",
    "yani",
}

TURKISH_HINT_WORDS = {
    "alisveris",
    "alışveriş",
    "kampanya",
    "indirim",
    "market",
    "mağaza",
    "magaza",
    "fiyat",
    "yorum",
    "ürün",
    "urun",
}


def parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    cleaned = value.strip()
    if not cleaned:
        return None
    if re.fullmatch(r"\d{10}(?:\.\d+)?", cleaned):
        try:
            return datetime.fromtimestamp(float(cleaned), tz=timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None
    if re.fullmatch(r"\d{13}", cleaned):
        try:
            return datetime.fromtimestamp(int(cleaned) / 1000, tz=timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None
    if re.fullmatch(r"\d{8}", cleaned):
        try:
            return datetime.strptime(cleaned, "%Y%m%d").replace(tzinfo=timezone.utc)
        except ValueError:
            return None
    if cleaned.endswith("Z"):
        cleaned = cleaned[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(cleaned)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def is_between(
    value: str | None,
    requested_from: str | None,
    requested_to: str | None,
) -> bool:
    parsed_value = parse_datetime(value)
    if parsed_value is None:
        return True
    from_dt = parse_datetime(requested_from)
    to_dt = parse_datetime(requested_to)
    if from_dt and parsed_value < from_dt:
        return False
    if to_dt and parsed_value > to_dt:
        return False
    return True


def parse_count(value: object) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return None


def iter_chunks(values: list[str], chunk_size: int) -> list[list[str]]:
    if chunk_size <= 0:
        return [values]
    return [values[index : index + chunk_size] for index in range(0, len(values), chunk_size)]


def _language_matches(value: str | None, target_language: str) -> bool:
    if not value:
        return False
    cleaned = str(value).strip().lower()
    target = target_language.strip().lower()
    return cleaned == target or cleaned.startswith(f"{target}-")


def looks_like_turkish(text: str) -> bool:
    lowered = normalize_text(text)
    if not lowered:
        return False

    words = re.findall(r"[a-zA-ZçğıöşüÇĞİÖŞÜ]+", lowered)
    if not words:
        return False

    stopword_hits = sum(1 for word in words if word in TURKISH_STOPWORDS)
    hint_hits = sum(1 for word in words if word in TURKISH_HINT_WORDS)
    char_hits = sum(lowered.count(char) for char in "çğıöşü")

    if char_hits >= 1:
        return True
    if stopword_hits >= 2:
        return True
    if stopword_hits >= 1 and hint_hits >= 1:
        return True
    if hint_hits >= 2:
        return True
    return False


def detect_language(text: str) -> str | None:
    cleaned = normalize_text(text)
    if len(cleaned) < 12:
        return None
    try:
        return detect(cleaned)
    except LangDetectException:
        return None


def matches_target_language(
    *,
    language: str | None,
    text: str,
    target_language: str,
) -> bool:
    if _language_matches(language, target_language):
        return True

    detected = detect_language(text)
    if detected:
        return _language_matches(detected, target_language)

    if target_language.strip().lower() == "tr":
        return looks_like_turkish(text)

    return False


@dataclass
class AdapterResult:
    items: list[dict[str, object]]
    warnings: list[str]
    allow_demo_fallback: bool = False


@dataclass(frozen=True)
class OwnedYouTubeChannel:
    brand: str
    aliases: tuple[str, ...]
    channel_id: str
    title: str


@dataclass(frozen=True)
class ExternalApiSourceConfig:
    id: int
    name: str
    platform: str
    method: str
    url_template: str
    headers: dict[str, object]
    body_template: str | None
    results_path: str | None
    field_mapping: dict[str, object]
    pagination: dict[str, object]
    is_enabled: bool


def _resolve_path(payload: object, path: str | None) -> object | None:
    if payload is None:
        return None
    if not path:
        return payload

    current = payload
    for part in [piece for piece in path.split(".") if piece]:
        if isinstance(current, dict):
            current = current.get(part)
        elif isinstance(current, list):
            if not part.isdigit():
                return None
            index = int(part)
            if index < 0 or index >= len(current):
                return None
            current = current[index]
        else:
            return None
    return current


def _apply_template_string(value: str, context: dict[str, str]) -> str:
    rendered = value
    for key, replacement in context.items():
        rendered = rendered.replace(f"{{{key}}}", replacement)
    return rendered


def _apply_template_payload(value: object, context: dict[str, str]) -> object:
    if isinstance(value, str):
        return _apply_template_string(value, context)
    if isinstance(value, list):
        return [_apply_template_payload(item, context) for item in value]
    if isinstance(value, dict):
        return {str(key): _apply_template_payload(item, context) for key, item in value.items()}
    return value


def _extract_mapped_value(payload: object, mapping_value: object) -> object | None:
    if mapping_value is None:
        return None
    if isinstance(mapping_value, list):
        for candidate in mapping_value:
            resolved = _extract_mapped_value(payload, candidate)
            if resolved not in (None, "", [], {}):
                return resolved
        return None
    if isinstance(mapping_value, str) and mapping_value.startswith("literal:"):
        return mapping_value.removeprefix("literal:")
    if isinstance(mapping_value, str):
        return _resolve_path(payload, mapping_value)
    return mapping_value


def _stringify_value(value: object | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        text = value.strip()
        return text or None
    if isinstance(value, (int, float, bool)):
        return str(value)
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    return str(value)


def _append_query_params(url: str, extra_params: dict[str, str]) -> str:
    parsed = urlparse(url)
    existing = dict(parse_qsl(parsed.query, keep_blank_values=True))
    existing.update({key: value for key, value in extra_params.items() if value is not None})
    return urlunparse(parsed._replace(query=urlencode(existing, doseq=True)))


def _format_date_param(value: str | None, timezone_name: str = "Europe/Istanbul") -> str | None:
    parsed = parse_datetime(value)
    if parsed is None:
        return None
    try:
        localized = parsed.astimezone(ZoneInfo(timezone_name))
    except Exception:
        localized = parsed
    return localized.date().isoformat()


def _extract_facebook_page_markers(raw_url: str | None) -> dict[str, str | None]:
    if not raw_url:
        return {"url": None, "slug": None, "profile_id": None}

    try:
        parsed = urlparse(raw_url.strip())
    except ValueError:
        return {"url": None, "slug": None, "profile_id": None}

    host = (parsed.netloc or "").lower().removeprefix("www.")
    if "facebook.com" not in host:
        return {"url": None, "slug": None, "profile_id": None}

    path_parts = [part for part in parsed.path.split("/") if part]
    slug: str | None = None
    profile_id: str | None = None

    if path_parts:
        first = path_parts[0]
        if first == "profile.php":
            query_params = dict(parse_qsl(parsed.query, keep_blank_values=True))
            profile_id = query_params.get("id") or None
        elif first != "pages":
            slug = first
        elif len(path_parts) >= 3:
            slug = path_parts[1] or None
            profile_id = path_parts[2] or None

    canonical = None
    if slug:
        canonical = f"https://www.facebook.com/{slug}"
    elif profile_id:
        canonical = f"https://www.facebook.com/profile.php?id={profile_id}"

    return {
        "url": canonical,
        "slug": slug.lower() if slug else None,
        "profile_id": profile_id,
    }


def _facebook_author_matches(record: dict[str, object], brand_profile: dict[str, object] | None) -> bool:
    if not brand_profile:
        return False

    markers = _extract_facebook_page_markers(str(brand_profile.get("official_facebook_url") or "").strip())
    official_slug = (markers.get("slug") or "").lower()
    official_profile_id = str(markers.get("profile_id") or "").strip()
    official_canonical = (markers.get("url") or "").rstrip("/").lower()
    brand_name = normalize_text(str(brand_profile.get("name") or "")).lower()

    author = record.get("author") if isinstance(record.get("author"), dict) else {}
    author_url = str(author.get("url") or "").rstrip("/").lower()
    author_name = normalize_text(str(author.get("name") or "")).lower()
    author_id = str(author.get("id") or "").strip()

    if official_canonical and author_url == official_canonical:
        return True
    if official_slug and author_url.endswith(f"/{official_slug}"):
        return True
    if official_profile_id and author_id == official_profile_id:
        return True
    if official_slug and author_name == official_slug:
        return True
    if brand_name and author_name == brand_name:
        return True
    return False


def _first_attachment_media_url(record: dict[str, object]) -> str | None:
    attachments = record.get("attachments")
    if not isinstance(attachments, list):
        return None
    for attachment in attachments:
        if not isinstance(attachment, dict):
            continue
        url = _stringify_value(attachment.get("thumbnailUrl"))
        if url:
            return url
        url = _stringify_value(attachment.get("image"))
        if url:
            return url
        url = _stringify_value(attachment.get("url"))
        if url:
            return url
    return None


class BaseAdapter:
    platform = "base"
    source_kind = "official-api"

    def collect(
        self,
        terms: list[str],
        requested_from: str | None,
        requested_to: str | None,
        *,
        brand_profile: dict[str, object] | None = None,
    ) -> AdapterResult:
        raise NotImplementedError


class DemoAdapter(BaseAdapter):
    source_kind = "demo"

    def __init__(self, platform: str) -> None:
        self.platform = platform

    def collect(
        self,
        terms: list[str],
        requested_from: str | None,
        requested_to: str | None,
        *,
        brand_profile: dict[str, object] | None = None,
    ) -> AdapterResult:
        now = utcnow()
        term_text = ", ".join(terms) if terms else "brand monitor"
        items = []
        for index in range(1, 4):
            published = now - timedelta(hours=index * 3)
            title = f"{self.platform.title()} mention {index}"
            body = (
                f"Demo veri: {term_text} hakkinda tespit edilen ornek {self.platform} icerigi. "
                f"Bu kayit gercek connector yokken UI ve DB akisini test etmek icin uretilir."
            )
            items.append(
                {
                    "platform": self.platform,
                    "source_kind": self.source_kind,
                    "content_type": "post",
                    "external_id": f"demo-{self.platform}-{index}",
                    "source_name": f"{self.platform.title()} demo source",
                    "author_name": f"Demo author {index}",
                    "title": title,
                    "body_text": body,
                    "normalized_text": build_normalized_text(title, body, term_text, self.platform),
                    "thumbnail_url": None,
                    "content_url": f"https://example.com/{self.platform}/{index}",
                    "permalink": f"https://example.com/{self.platform}/{index}",
                    "language": "tr",
                    "published_at": isoformat(published),
                    "first_seen_at": isoformat(now),
                    "last_seen_at": isoformat(now),
                    "raw_payload": {"demo": True, "platform": self.platform, "terms": terms},
                }
            )
        return AdapterResult(
            items=items,
            warnings=[f"{self.platform}: gercek connector yok, demo veri kullanildi."],
        )


class YouTubeAdapter(BaseAdapter):
    platform = "youtube"

    def __init__(
        self,
        api_key: str | None,
        owned_channels_path: Path | None = None,
        target_language: str = "tr",
        target_region: str = "TR",
        strict_language_filter: bool = True,
        max_results: int = 12,
        max_pages: int = 3,
        fetch_comments: bool = True,
        comment_threads_per_video: int = 5,
    ) -> None:
        self.api_key = api_key
        self.owned_channels = self._load_owned_channels(owned_channels_path)
        self.target_language = target_language
        self.target_region = target_region
        self.strict_language_filter = strict_language_filter
        self.max_results = max(1, min(max_results, 50))
        self.max_pages = max(1, min(max_pages, 10))
        self.fetch_comments = fetch_comments
        self.comment_threads_per_video = max(0, min(comment_threads_per_video, 100))

    def _load_owned_channels(self, path: Path | None) -> list[OwnedYouTubeChannel]:
        if path is None or not path.exists():
            return []

        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return []

        records: list[OwnedYouTubeChannel] = []
        for brand_entry in payload.get("brands", []):
            brand = normalize_text(brand_entry.get("brand", ""))
            aliases = tuple(normalize_text(alias) for alias in brand_entry.get("aliases", []) if normalize_text(alias))
            for channel_entry in brand_entry.get("channels", []):
                channel_id = channel_entry.get("channel_id")
                title = channel_entry.get("title", "")
                if not brand or not aliases or not channel_id:
                    continue
                records.append(
                    OwnedYouTubeChannel(
                        brand=brand,
                        aliases=aliases,
                        channel_id=channel_id,
                        title=title,
                    )
                )
        return records

    def _matched_owned_channels(self, terms: list[str]) -> list[OwnedYouTubeChannel]:
        normalized_terms = {normalize_text(term) for term in terms}
        matched: list[OwnedYouTubeChannel] = []
        seen_channel_ids: set[str] = set()
        for channel in self.owned_channels:
            if normalized_terms.intersection(channel.aliases):
                if channel.channel_id not in seen_channel_ids:
                    matched.append(channel)
                    seen_channel_ids.add(channel.channel_id)
        return matched

    def _resolve_owned_channel_from_url(
        self,
        *,
        brand_profile: dict[str, object] | None,
        terms: list[str],
    ) -> tuple[OwnedYouTubeChannel | None, list[str]]:
        if not brand_profile:
            return None, []

        raw_url = str(brand_profile.get("official_youtube_url") or "").strip()
        if not raw_url:
            return None, []

        brand_name = normalize_text(str(brand_profile.get("name") or ""))
        aliases = [normalize_text(term) for term in terms if normalize_text(term)]
        if brand_name and brand_name not in aliases:
            aliases.append(brand_name)

        channel_id: str | None = None
        channel_title = ""
        warnings: list[str] = []

        parsed = urlparse(raw_url if "://" in raw_url else f"https://{raw_url}")
        host = parsed.netloc.lower()
        path = parsed.path.strip("/")

        def fetch_channel_by_id(value: str) -> tuple[str | None, str | None]:
            payload = self._fetch_json(
                "channels",
                {
                    "part": "id,snippet",
                    "id": value,
                },
            )
            items = payload.get("items", [])
            if not items:
                return None, None
            item = items[0]
            return str(item.get("id") or ""), str((item.get("snippet") or {}).get("title") or "")

        def fetch_channel_by_handle(value: str) -> tuple[str | None, str | None]:
            payload = self._fetch_json(
                "channels",
                {
                    "part": "id,snippet",
                    "forHandle": value.lstrip("@"),
                },
            )
            items = payload.get("items", [])
            if not items:
                return None, None
            item = items[0]
            return str(item.get("id") or ""), str((item.get("snippet") or {}).get("title") or "")

        def fetch_channel_by_username(value: str) -> tuple[str | None, str | None]:
            payload = self._fetch_json(
                "channels",
                {
                    "part": "id,snippet",
                    "forUsername": value,
                },
            )
            items = payload.get("items", [])
            if not items:
                return None, None
            item = items[0]
            return str(item.get("id") or ""), str((item.get("snippet") or {}).get("title") or "")

        def fetch_channel_by_search(value: str) -> tuple[str | None, str | None]:
            payload = self._fetch_json(
                "search",
                {
                    "part": "snippet",
                    "type": "channel",
                    "q": value,
                    "maxResults": "1",
                },
            )
            items = payload.get("items", [])
            if not items:
                return None, None
            item = items[0]
            item_id = item.get("id") or {}
            return str(item_id.get("channelId") or ""), str((item.get("snippet") or {}).get("title") or "")

        try:
            if re.fullmatch(r"UC[a-zA-Z0-9_-]{20,}", raw_url):
                channel_id, channel_title = fetch_channel_by_id(raw_url)
            elif raw_url.startswith("@"):
                channel_id, channel_title = fetch_channel_by_handle(raw_url)
            elif "youtube.com" in host or "youtu.be" in host or raw_url.startswith("www.youtube.com"):
                parts = [part for part in path.split("/") if part]
                if parts and parts[0].startswith("@"):
                    channel_id, channel_title = fetch_channel_by_handle(parts[0])
                elif len(parts) >= 2 and parts[0] == "channel":
                    channel_id, channel_title = fetch_channel_by_id(parts[1])
                elif len(parts) >= 2 and parts[0] == "user":
                    channel_id, channel_title = fetch_channel_by_username(parts[1])
                elif len(parts) >= 2 and parts[0] == "c":
                    channel_id, channel_title = fetch_channel_by_search(parts[1])
                elif parts:
                    channel_id, channel_title = fetch_channel_by_search(parts[-1])
            else:
                channel_id, channel_title = fetch_channel_by_search(raw_url)
        except HTTPError as exc:
            warnings.append(f"youtube resmi kanal: link cozumlenemedi ({exc.code}).")
            return None, warnings
        except URLError as exc:
            warnings.append(f"youtube resmi kanal: baglanti hatasi ({exc.reason}).")
            return None, warnings
        except RuntimeError as exc:
            warnings.append(f"youtube resmi kanal: {exc}")
            return None, warnings

        if not channel_id:
            warnings.append("youtube resmi kanal: verilen linkten kanal bulunamadi.")
            return None, warnings

        return (
            OwnedYouTubeChannel(
                brand=brand_name or normalize_text(channel_title),
                aliases=tuple(aliases or [normalize_text(channel_title)]),
                channel_id=channel_id,
                title=channel_title,
            ),
            warnings,
        )

    def _collect_owned_channel_targets(
        self,
        *,
        terms: list[str],
        brand_profile: dict[str, object] | None,
    ) -> tuple[list[OwnedYouTubeChannel], list[str]]:
        matched = self._matched_owned_channels(terms)
        dynamic_channel, warnings = self._resolve_owned_channel_from_url(
            brand_profile=brand_profile,
            terms=terms,
        )
        seen_channel_ids = {channel.channel_id for channel in matched}
        if dynamic_channel and dynamic_channel.channel_id not in seen_channel_ids:
            matched.append(dynamic_channel)
        return matched, warnings

    def _fetch_json(self, endpoint: str, params: dict[str, str]) -> dict[str, object]:
        if not self.api_key:
            raise RuntimeError("YouTube API key tanimli degil.")

        full_params = dict(params)
        full_params["key"] = self.api_key
        url = f"https://www.googleapis.com/youtube/v3/{endpoint}?" + urlencode(full_params)
        request = Request(url, headers={"Accept": "application/json"})
        with urlopen(request, timeout=20) as response:
            return json.loads(response.read().decode("utf-8"))

    def _build_video_item(self, entry: dict[str, object], query: str, seen_at: str) -> dict[str, object] | None:
        snippet = entry.get("snippet", {})
        video_id = (entry.get("id") or {}).get("videoId")
        if not video_id:
            return None
        title = snippet.get("title", "")
        description = snippet.get("description", "")
        channel = snippet.get("channelTitle", "")
        permalink = f"https://www.youtube.com/watch?v={video_id}"
        thumbnail_url = extract_thumbnail_url(snippet.get("thumbnails"))
        return {
            "platform": self.platform,
            "source_kind": self.source_kind,
            "content_type": "video",
            "external_id": video_id,
            "source_name": channel,
            "author_name": channel,
            "title": title,
            "body_text": description,
            "normalized_text": build_normalized_text(title, description, channel),
            "thumbnail_url": thumbnail_url,
            "view_count": None,
            "like_count": None,
            "dislike_count": None,
            "comment_count": None,
            "channel_subscriber_count": None,
            "content_url": permalink,
            "permalink": permalink,
            "language": snippet.get("defaultAudioLanguage"),
            "published_at": snippet.get("publishedAt"),
            "first_seen_at": seen_at,
            "last_seen_at": seen_at,
            "raw_payload": entry,
        }

    def _build_playlist_video_item(
        self,
        *,
        entry: dict[str, object],
        brand: str,
        channel_title: str,
        seen_at: str,
    ) -> dict[str, object] | None:
        snippet = entry.get("snippet", {})
        content_details = entry.get("contentDetails", {})
        resource = snippet.get("resourceId", {})
        video_id = content_details.get("videoId") or resource.get("videoId")
        if not video_id:
            return None
        title = snippet.get("title", "")
        description = snippet.get("description", "")
        permalink = f"https://www.youtube.com/watch?v={video_id}"
        thumbnail_url = extract_thumbnail_url(snippet.get("thumbnails"))
        return {
            "platform": self.platform,
            "source_kind": "owned-channel",
            "content_type": "video",
            "external_id": video_id,
            "source_name": channel_title or snippet.get("channelTitle", ""),
            "author_name": channel_title or snippet.get("channelTitle", ""),
            "title": title,
            "body_text": description,
            "normalized_text": build_normalized_text(title, description, channel_title, brand),
            "thumbnail_url": thumbnail_url,
            "view_count": None,
            "like_count": None,
            "dislike_count": None,
            "comment_count": None,
            "channel_subscriber_count": None,
            "content_url": permalink,
            "permalink": permalink,
            "language": None,
            "published_at": snippet.get("publishedAt"),
            "first_seen_at": seen_at,
            "last_seen_at": seen_at,
            "raw_payload": entry,
        }

    def _video_is_target_language(self, entry: dict[str, object]) -> bool:
        snippet = entry.get("snippet", {})
        text = " ".join(
            part
            for part in [
                snippet.get("title", ""),
                snippet.get("description", ""),
                snippet.get("channelTitle", ""),
            ]
            if part
        )
        return matches_target_language(
            language=snippet.get("defaultAudioLanguage") or snippet.get("defaultLanguage"),
            text=text,
            target_language=self.target_language,
        )

    def _build_comment_item(
        self,
        *,
        comment_id: str,
        video_id: str,
        video_title: str,
        channel: str,
        snippet: dict[str, object],
        seen_at: str,
        content_type: str,
        raw_payload: dict[str, object],
        thumbnail_url: str | None,
        video_metrics: dict[str, int | None] | None = None,
    ) -> dict[str, object]:
        author_name = snippet.get("authorDisplayName", "")
        body = snippet.get("textDisplay") or snippet.get("textOriginal") or ""
        permalink = f"https://www.youtube.com/watch?v={video_id}&lc={comment_id}"
        title = f"Comment on: {video_title}" if content_type == "comment" else f"Reply on: {video_title}"
        video_metrics = video_metrics or {}
        return {
            "platform": self.platform,
            "source_kind": self.source_kind,
            "content_type": content_type,
            "external_id": comment_id,
            "source_name": channel,
            "author_name": author_name,
            "title": title,
            "body_text": body,
            "normalized_text": build_normalized_text(title, body, author_name, channel),
            "thumbnail_url": thumbnail_url,
            "view_count": video_metrics.get("view_count"),
            "like_count": video_metrics.get("like_count"),
            "dislike_count": video_metrics.get("dislike_count"),
            "comment_count": video_metrics.get("comment_count"),
            "channel_subscriber_count": video_metrics.get("channel_subscriber_count"),
            "content_url": permalink,
            "permalink": permalink,
            "language": None,
            "published_at": snippet.get("publishedAt"),
            "first_seen_at": seen_at,
            "last_seen_at": seen_at,
            "raw_payload": raw_payload,
        }

    def _comment_is_target_language(self, snippet: dict[str, object]) -> bool:
        body = snippet.get("textDisplay") or snippet.get("textOriginal") or ""
        if not body:
            return False
        return matches_target_language(
            language=None,
            text=body,
            target_language=self.target_language,
        )

    def _fetch_comment_items(
        self,
        *,
        video_id: str,
        video_title: str,
        channel: str,
        terms: list[str],
        requested_from: str | None,
        requested_to: str | None,
        seen_at: str,
        thumbnail_url: str | None,
        video_metrics: dict[str, int | None] | None = None,
    ) -> tuple[list[dict[str, object]], list[str]]:
        if not self.fetch_comments or self.comment_threads_per_video <= 0:
            return [], []

        params = {
            "part": "snippet,replies",
            "videoId": video_id,
            "order": "time",
            "textFormat": "plainText",
            "maxResults": str(self.comment_threads_per_video),
        }
        if terms:
            params["searchTerms"] = " ".join(terms[:5])

        try:
            payload = self._fetch_json("commentThreads", params)
        except HTTPError as exc:
            if exc.code == 403:
                # YouTube cok sayida videoda yorumlari API'den kapatabiliyor;
                # bu durumda ana video sonucunu dusurmeden yorumu sessizce atliyoruz.
                return [], []
            return [], [f"youtube: {video_id} yorumlari alinamadi ({exc.code})."]
        except URLError as exc:
            return [], [f"youtube: {video_id} yorum baglantisi hatasi ({exc.reason})."]
        except RuntimeError as exc:
            return [], [f"youtube: {exc}"]

        items: list[dict[str, object]] = []
        warnings: list[str] = []
        for thread in payload.get("items", []):
            thread_snippet = thread.get("snippet", {})
            top_level = (thread_snippet.get("topLevelComment") or {})
            top_level_snippet = top_level.get("snippet", {})
            top_level_id = top_level.get("id")

            if top_level_id and is_between(top_level_snippet.get("publishedAt"), requested_from, requested_to):
                if not self.strict_language_filter or self._comment_is_target_language(top_level_snippet):
                    items.append(
                        self._build_comment_item(
                            comment_id=top_level_id,
                            video_id=video_id,
                            video_title=video_title,
                            channel=channel,
                            snippet=top_level_snippet,
                            seen_at=seen_at,
                            content_type="comment",
                            raw_payload=top_level,
                            thumbnail_url=thumbnail_url,
                            video_metrics=video_metrics,
                        )
                    )

            for reply in (thread.get("replies") or {}).get("comments", []):
                reply_id = reply.get("id")
                reply_snippet = reply.get("snippet", {})
                if not reply_id:
                    continue
                if not is_between(reply_snippet.get("publishedAt"), requested_from, requested_to):
                    continue
                if not self.strict_language_filter or self._comment_is_target_language(reply_snippet):
                    items.append(
                        self._build_comment_item(
                            comment_id=reply_id,
                            video_id=video_id,
                            video_title=video_title,
                            channel=channel,
                            snippet=reply_snippet,
                            seen_at=seen_at,
                            content_type="comment-reply",
                            raw_payload=reply,
                            thumbnail_url=thumbnail_url,
                            video_metrics=video_metrics,
                        )
                    )

        return items, warnings

    def _enrich_video_items(self, video_items: list[dict[str, object]]) -> list[str]:
        if not video_items:
            return []

        warnings: list[str] = []
        video_map = {
            str(item["external_id"]): item
            for item in video_items
            if item.get("content_type") == "video" and item.get("external_id")
        }
        if not video_map:
            return warnings

        channel_ids_by_video: dict[str, str] = {}
        for chunk in iter_chunks(list(video_map.keys()), 50):
            try:
                payload = self._fetch_json(
                    "videos",
                    {
                        "part": "snippet,statistics",
                        "id": ",".join(chunk),
                    },
                )
            except HTTPError as exc:
                warnings.append(f"youtube: video istatistikleri alinamadi ({exc.code}).")
                return warnings
            except URLError as exc:
                warnings.append(f"youtube: video istatistik baglanti hatasi ({exc.reason}).")
                return warnings

            for entry in payload.get("items", []):
                video_id = entry.get("id")
                if not video_id or video_id not in video_map:
                    continue

                item = video_map[video_id]
                statistics = entry.get("statistics", {})
                snippet = entry.get("snippet", {})
                item["view_count"] = parse_count(statistics.get("viewCount"))
                item["like_count"] = parse_count(statistics.get("likeCount"))
                item["dislike_count"] = parse_count(statistics.get("dislikeCount"))
                item["comment_count"] = parse_count(statistics.get("commentCount"))

                channel_id = snippet.get("channelId")
                if channel_id:
                    channel_ids_by_video[video_id] = str(channel_id)

                if snippet.get("channelTitle"):
                    item["source_name"] = snippet.get("channelTitle")
                    item["author_name"] = snippet.get("channelTitle")
                if not item.get("language"):
                    item["language"] = snippet.get("defaultAudioLanguage") or snippet.get("defaultLanguage")

        if not channel_ids_by_video:
            return warnings

        channel_subscribers: dict[str, int | None] = {}
        for chunk in iter_chunks(sorted(set(channel_ids_by_video.values())), 50):
            try:
                payload = self._fetch_json(
                    "channels",
                    {
                        "part": "statistics",
                        "id": ",".join(chunk),
                    },
                )
            except HTTPError as exc:
                warnings.append(f"youtube: kanal istatistikleri alinamadi ({exc.code}).")
                return warnings
            except URLError as exc:
                warnings.append(f"youtube: kanal istatistik baglanti hatasi ({exc.reason}).")
                return warnings

            for entry in payload.get("items", []):
                channel_id = entry.get("id")
                if not channel_id:
                    continue
                statistics = entry.get("statistics", {})
                if statistics.get("hiddenSubscriberCount"):
                    channel_subscribers[str(channel_id)] = None
                else:
                    channel_subscribers[str(channel_id)] = parse_count(statistics.get("subscriberCount"))

        for video_id, item in video_map.items():
            item["channel_subscriber_count"] = channel_subscribers.get(channel_ids_by_video.get(video_id))

        return warnings

    def _fetch_owned_channel_items(
        self,
        *,
        terms: list[str],
        requested_from: str | None,
        requested_to: str | None,
        seen_at: str,
        existing_video_ids: set[str],
        brand_profile: dict[str, object] | None = None,
    ) -> tuple[list[dict[str, object]], list[str]]:
        matched_channels, warnings = self._collect_owned_channel_targets(
            terms=terms,
            brand_profile=brand_profile,
        )
        if not matched_channels:
            return [], warnings

        items: list[dict[str, object]] = []
        for owned_channel in matched_channels:
            try:
                channel_payload = self._fetch_json(
                    "channels",
                    {
                        "part": "contentDetails,snippet",
                        "id": owned_channel.channel_id,
                    },
                )
            except HTTPError as exc:
                warnings.append(f"youtube owned-channel {owned_channel.title or owned_channel.brand}: kanal okunamadi ({exc.code}).")
                continue
            except URLError as exc:
                warnings.append(f"youtube owned-channel {owned_channel.title or owned_channel.brand}: baglanti hatasi ({exc.reason}).")
                continue

            channel_items = channel_payload.get("items", [])
            if not channel_items:
                warnings.append(f"youtube owned-channel {owned_channel.title or owned_channel.brand}: kanal bulunamadi.")
                continue

            channel_data = channel_items[0]
            uploads_playlist_id = ((channel_data.get("contentDetails") or {}).get("relatedPlaylists") or {}).get("uploads")
            channel_title = ((channel_data.get("snippet") or {}).get("title")) or owned_channel.title
            if not uploads_playlist_id:
                warnings.append(f"youtube owned-channel {channel_title or owned_channel.brand}: uploads playlist bulunamadi.")
                continue

            next_page_token: str | None = None
            for _ in range(self.max_pages):
                params = {
                    "part": "snippet,contentDetails",
                    "playlistId": uploads_playlist_id,
                    "maxResults": str(self.max_results),
                }
                if next_page_token:
                    params["pageToken"] = next_page_token

                try:
                    playlist_payload = self._fetch_json("playlistItems", params)
                except HTTPError as exc:
                    warnings.append(f"youtube owned-channel {channel_title or owned_channel.brand}: playlist okunamadi ({exc.code}).")
                    break
                except URLError as exc:
                    warnings.append(f"youtube owned-channel {channel_title or owned_channel.brand}: playlist baglanti hatasi ({exc.reason}).")
                    break

                stop_paging = False
                for entry in playlist_payload.get("items", []):
                    published_at = (entry.get("snippet") or {}).get("publishedAt")
                    if requested_to and not is_between(published_at, None, requested_to):
                        continue
                    if requested_from and not is_between(published_at, requested_from, None):
                        stop_paging = True
                        continue

                    video_item = self._build_playlist_video_item(
                        entry=entry,
                        brand=owned_channel.brand,
                        channel_title=channel_title or owned_channel.title,
                        seen_at=seen_at,
                    )
                    if not video_item:
                        continue
                    if video_item["external_id"] in existing_video_ids:
                        continue
                    existing_video_ids.add(video_item["external_id"])
                    items.append(video_item)

                if stop_paging:
                    break

                next_page_token = playlist_payload.get("nextPageToken")
                if not next_page_token:
                    break

        return items, warnings

    def collect(
        self,
        terms: list[str],
        requested_from: str | None,
        requested_to: str | None,
        *,
        brand_profile: dict[str, object] | None = None,
    ) -> AdapterResult:
        if not self.api_key:
            return AdapterResult(
                items=[],
                warnings=["youtube: API key tanimli degil."],
                allow_demo_fallback=True,
            )

        query = " OR ".join(terms) if terms else ""
        base_params = {
            "part": "snippet",
            "q": query,
            "type": "video",
            "order": "date",
            "maxResults": str(self.max_results),
            "regionCode": self.target_region,
            "relevanceLanguage": self.target_language,
        }
        if requested_from:
            base_params["publishedAfter"] = requested_from
        if requested_to:
            base_params["publishedBefore"] = requested_to

        items: list[dict[str, object]] = []
        video_items: list[dict[str, object]] = []
        seen_video_ids: set[str] = set()
        warnings: list[str] = []
        seen_at = isoformat(utcnow())
        next_page_token: str | None = None

        for _ in range(self.max_pages):
            params = dict(base_params)
            if next_page_token:
                params["pageToken"] = next_page_token

            try:
                payload = self._fetch_json("search", params)
            except HTTPError as exc:
                return AdapterResult(
                    items=items,
                    warnings=warnings + [f"youtube: arama cagrisi basarisiz ({exc.code})."],
                    allow_demo_fallback=not items,
                )
            except URLError as exc:
                return AdapterResult(
                    items=items,
                    warnings=warnings + [f"youtube: baglanti hatasi ({exc.reason})."],
                    allow_demo_fallback=not items,
                )

            for entry in payload.get("items", []):
                if self.strict_language_filter and not self._video_is_target_language(entry):
                    continue
                video_item = self._build_video_item(entry, query, seen_at)
                if not video_item:
                    continue
                if video_item["external_id"] in seen_video_ids:
                    continue
                seen_video_ids.add(video_item["external_id"])
                video_items.append(video_item)

            next_page_token = payload.get("nextPageToken")
            if not next_page_token:
                break

        owned_items, owned_warnings = self._fetch_owned_channel_items(
            terms=terms,
            requested_from=requested_from,
            requested_to=requested_to,
            seen_at=seen_at,
            existing_video_ids=seen_video_ids,
            brand_profile=brand_profile,
        )
        video_items.extend(owned_items)
        warnings.extend(owned_warnings)

        warnings.extend(self._enrich_video_items(video_items))
        items.extend(video_items)

        for video_item in video_items:
            video_metrics = {
                "view_count": video_item.get("view_count"),
                "like_count": video_item.get("like_count"),
                "dislike_count": video_item.get("dislike_count"),
                "comment_count": video_item.get("comment_count"),
                "channel_subscriber_count": video_item.get("channel_subscriber_count"),
            }
            comment_items, comment_warnings = self._fetch_comment_items(
                video_id=video_item["external_id"],
                video_title=video_item["title"] or "Untitled video",
                channel=video_item["source_name"] or "",
                terms=terms,
                requested_from=requested_from,
                requested_to=requested_to,
                seen_at=seen_at,
                thumbnail_url=video_item.get("thumbnail_url"),
                video_metrics=video_metrics,
            )
            items.extend(comment_items)
            warnings.extend(comment_warnings)

        return AdapterResult(items=items, warnings=warnings)


class CompositeAdapter(BaseAdapter):
    def __init__(self, platform: str, adapters: list[BaseAdapter]) -> None:
        self.platform = platform
        self.adapters = adapters

    def collect_iter(
        self,
        terms: list[str],
        requested_from: str | None,
        requested_to: str | None,
        *,
        brand_profile: dict[str, object] | None = None,
    ):
        for adapter in self.adapters:
            if hasattr(adapter, "collect_iter"):
                for result in adapter.collect_iter(
                    terms,
                    requested_from,
                    requested_to,
                    brand_profile=brand_profile,
                ):
                    yield result
                    if any(_is_apify_quota_warning(message) for message in result.warnings):
                        return
            else:
                result = adapter.collect(
                    terms,
                    requested_from,
                    requested_to,
                    brand_profile=brand_profile,
                )
                yield result
                if any(_is_apify_quota_warning(message) for message in result.warnings):
                    return

    def collect(
        self,
        terms: list[str],
        requested_from: str | None,
        requested_to: str | None,
        *,
        brand_profile: dict[str, object] | None = None,
    ) -> AdapterResult:
        items: list[dict[str, object]] = []
        warnings: list[str] = []
        allow_demo_fallback = False
        for result in self.collect_iter(
            terms,
            requested_from,
            requested_to,
            brand_profile=brand_profile,
        ):
            items.extend(result.items)
            warnings.extend(result.warnings)
            allow_demo_fallback = allow_demo_fallback or result.allow_demo_fallback
        return AdapterResult(
            items=items,
            warnings=warnings,
            allow_demo_fallback=allow_demo_fallback and not items,
        )


class ApifyFacebookSearchAdapter(BaseAdapter):
    platform = "facebook"
    source_kind = "custom-api"

    def __init__(
        self,
        *,
        token: str | None,
        actor_id: str,
        results_limit: int,
    ) -> None:
        self.token = (token or "").strip() or None
        self.actor_id = actor_id.strip()
        self.results_limit = max(1, min(results_limit, 100))

    def _run_actor(self, payload: dict[str, object]) -> tuple[list[dict[str, object]], list[str]]:
        if not self.token:
            return [], []

        actor_key = quote(self.actor_id.replace("/", "~"), safe="~")
        url = (
            f"https://api.apify.com/v2/acts/{actor_key}/run-sync-get-dataset-items"
            f"?token={quote_plus(self.token)}&clean=true&format=json"
        )
        request = Request(
            url,
            data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
            method="POST",
        )

        try:
            with urlopen(request, timeout=120) as response:
                raw_payload = json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            return [], [_build_apify_http_error(self.actor_id, exc, action="API cagrisi")]
        except URLError as exc:
            return [], [f"Apify {self.actor_id}: baglanti hatasi ({exc.reason})."]
        except json.JSONDecodeError:
            return [], [f"Apify {self.actor_id}: JSON cevabi okunamadi."]

        if isinstance(raw_payload, list):
            records = raw_payload
        elif isinstance(raw_payload, dict):
            records = [raw_payload]
        else:
            records = []
        return [record for record in records if isinstance(record, dict)], []

    def _build_item(
        self,
        record: dict[str, object],
        *,
        requested_from: str | None,
        requested_to: str | None,
        seen_at: str,
    ) -> dict[str, object] | None:
        published_raw = _stringify_value(record.get("timestamp"))
        if not is_between(published_raw, requested_from, requested_to):
            return None
        published_dt = parse_datetime(published_raw)
        author = record.get("author") if isinstance(record.get("author"), dict) else {}
        source_name = _stringify_value(author.get("name")) or "Facebook"
        content_url = _stringify_value(record.get("url"))
        if not content_url:
            return None
        external_id = _stringify_value(record.get("postId")) or content_url
        content_type = "post"
        attachments = record.get("attachments")
        if isinstance(attachments, list):
            for attachment in attachments:
                if not isinstance(attachment, dict):
                    continue
                attachment_type = _stringify_value(attachment.get("type"))
                if attachment_type in {"video", "reel"}:
                    content_type = "reel"
                    break
                if attachment_type in {"photo", "album"}:
                    content_type = "post"

        return {
            "platform": self.platform,
            "source_kind": self.source_kind,
            "content_type": content_type,
            "external_id": external_id,
            "source_name": source_name,
            "author_name": _stringify_value(author.get("name")) or source_name,
            "title": source_name,
            "body_text": _stringify_value(record.get("postText")),
            "normalized_text": build_normalized_text(
                _stringify_value(record.get("postText")),
                source_name,
                _stringify_value(author.get("profileUrl")),
            ),
            "thumbnail_url": _first_attachment_media_url(record) or _stringify_value(author.get("profilePicture")),
            "view_count": parse_count(record.get("views")),
            "like_count": parse_count(record.get("reactionsCount")),
            "dislike_count": None,
            "comment_count": parse_count(record.get("commentsCount")),
            "channel_subscriber_count": None,
            "content_url": content_url,
            "permalink": content_url,
            "language": _stringify_value(record.get("language")),
            "published_at": isoformat(published_dt) if published_dt else published_raw,
            "first_seen_at": seen_at,
            "last_seen_at": seen_at,
            "raw_payload": {"source": "Apify Facebook Search", "record": record},
        }

    def collect(
        self,
        terms: list[str],
        requested_from: str | None,
        requested_to: str | None,
        *,
        brand_profile: dict[str, object] | None = None,
    ) -> AdapterResult:
        if not self.token or not terms:
            return AdapterResult(items=[], warnings=[])

        seen_at = isoformat(utcnow())
        items: list[dict[str, object]] = []
        warnings: list[str] = []
        seen_external_ids: set[str] = set()
        start_date = _format_date_param(requested_from)
        end_date = _format_date_param(requested_to)

        for term in terms:
            payload: dict[str, object] = {
                "query": term,
                "resultsCount": self.results_limit,
                "searchType": "latest",
            }
            if start_date:
                payload["startDate"] = start_date
            if end_date:
                payload["endDate"] = end_date

            records, actor_warnings = self._run_actor(payload)
            warnings.extend(actor_warnings)
            if any(_is_apify_quota_warning(message) for message in actor_warnings):
                break
            for record in records:
                item = self._build_item(
                    record,
                    requested_from=requested_from,
                    requested_to=requested_to,
                    seen_at=seen_at,
                )
                if not item:
                    continue
                external_id = str(item.get("external_id") or "")
                if external_id in seen_external_ids:
                    continue
                seen_external_ids.add(external_id)
                items.append(item)

        return AdapterResult(items=items, warnings=warnings)


class ApifyFacebookOfficialAdapter(BaseAdapter):
    platform = "facebook"
    source_kind = "owned-page"

    def __init__(
        self,
        *,
        token: str | None,
        posts_actor_id: str,
        reels_actor_id: str,
        posts_limit: int,
        reels_limit: int,
    ) -> None:
        self.token = (token or "").strip() or None
        self.posts_actor_id = posts_actor_id.strip()
        self.reels_actor_id = reels_actor_id.strip()
        self.posts_limit = max(1, min(posts_limit, 100))
        self.reels_limit = max(1, min(reels_limit, 100))

    def _run_actor(self, actor_id: str, payload: dict[str, object]) -> tuple[list[dict[str, object]], list[str]]:
        if not self.token:
            return [], []

        actor_key = quote(actor_id.replace("/", "~"), safe="~")
        url = (
            f"https://api.apify.com/v2/acts/{actor_key}/run-sync-get-dataset-items"
            f"?token={quote_plus(self.token)}&clean=true&format=json"
        )
        request = Request(
            url,
            data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
            method="POST",
        )

        try:
            with urlopen(request, timeout=120) as response:
                raw_payload = json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            return [], [_build_apify_http_error(actor_id, exc, action="API cagrisi")]
        except URLError as exc:
            return [], [f"Apify {actor_id}: baglanti hatasi ({exc.reason})."]
        except json.JSONDecodeError:
            return [], [f"Apify {actor_id}: JSON cevabi okunamadi."]

        if isinstance(raw_payload, list):
            records = raw_payload
        elif isinstance(raw_payload, dict):
            records = [raw_payload]
        else:
            records = []
        return [record for record in records if isinstance(record, dict)], []

    def _start_actor_run(self, actor_id: str, payload: dict[str, object]) -> tuple[dict[str, object] | None, list[str]]:
        if not self.token:
            return None, []

        actor_key = quote(actor_id.replace("/", "~"), safe="~")
        url = f"https://api.apify.com/v2/acts/{actor_key}/runs?token={quote_plus(self.token)}"
        request = Request(
            url,
            data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
            method="POST",
        )
        try:
            with urlopen(request, timeout=120) as response:
                raw_payload = json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            return None, [_build_apify_http_error(actor_id, exc, action="actor baslatma")]
        except URLError as exc:
            return None, [f"Apify {actor_id}: baglanti hatasi ({exc.reason})."]
        except json.JSONDecodeError:
            return None, [f"Apify {actor_id}: run cevabi okunamadi."]

        data = raw_payload.get("data") if isinstance(raw_payload, dict) else None
        if not isinstance(data, dict):
            return None, [f"Apify {actor_id}: run bilgisi alinamadi."]
        return data, []

    def _get_actor_run(self, run_id: str) -> tuple[dict[str, object] | None, list[str]]:
        if not self.token:
            return None, []

        url = f"https://api.apify.com/v2/actor-runs/{quote(run_id)}?token={quote_plus(self.token)}"
        try:
            with urlopen(url, timeout=120) as response:
                raw_payload = json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            return None, [_build_apify_http_error(f"run {run_id}", exc, action="durum sorgusu")]
        except URLError as exc:
            return None, [f"Apify run {run_id}: baglanti hatasi ({exc.reason})."]
        except json.JSONDecodeError:
            return None, [f"Apify run {run_id}: JSON cevabi okunamadi."]

        data = raw_payload.get("data") if isinstance(raw_payload, dict) else None
        if not isinstance(data, dict):
            return None, [f"Apify run {run_id}: veri bulunamadi."]
        return data, []

    def _get_dataset_items(
        self,
        dataset_id: str,
        *,
        offset: int,
        limit: int,
    ) -> tuple[list[dict[str, object]], list[str]]:
        if not self.token:
            return [], []

        url = (
            f"https://api.apify.com/v2/datasets/{quote(dataset_id)}/items"
            f"?token={quote_plus(self.token)}&clean=true&format=json&offset={offset}&limit={limit}"
        )
        try:
            with urlopen(url, timeout=120) as response:
                raw_payload = json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            return [], [_build_apify_http_error(f"dataset {dataset_id}", exc, action="dataset okuma")]
        except URLError as exc:
            return [], [f"Apify dataset {dataset_id}: baglanti hatasi ({exc.reason})."]
        except json.JSONDecodeError:
            return [], [f"Apify dataset {dataset_id}: JSON cevabi okunamadi."]

        if isinstance(raw_payload, list):
            return [item for item in raw_payload if isinstance(item, dict)], []
        if isinstance(raw_payload, dict):
            return [raw_payload], []
        return [], []

    def _stream_actor_records(
        self,
        actor_id: str,
        payload: dict[str, object],
        *,
        batch_size: int = 10,
        poll_interval_seconds: float = 2.0,
    ):
        run, warnings = self._start_actor_run(actor_id, payload)
        if warnings:
            yield [], warnings
            return
        if not run:
            yield [], [f"Apify {actor_id}: actor baslatilamadi."]
            return

        run_id = _stringify_value(run.get("id"))
        dataset_id = _stringify_value(run.get("defaultDatasetId"))
        if not run_id or not dataset_id:
            yield [], [f"Apify {actor_id}: run veya dataset kimligi eksik."]
            return

        offset = 0
        terminal_statuses = {"SUCCEEDED", "FAILED", "ABORTED", "TIMED-OUT"}
        while True:
            items, item_warnings = self._get_dataset_items(dataset_id, offset=offset, limit=batch_size)
            if item_warnings:
                yield [], item_warnings
                return
            if items:
                offset += len(items)
                yield items, []

            run_state, run_warnings = self._get_actor_run(run_id)
            if run_warnings:
                yield [], run_warnings
                return

            status = _stringify_value((run_state or {}).get("status"))
            if status in terminal_statuses:
                final_items, final_warnings = self._get_dataset_items(dataset_id, offset=offset, limit=batch_size)
                if final_warnings:
                    yield [], final_warnings
                elif final_items:
                    yield final_items, []
                return

            time.sleep(poll_interval_seconds)

    def _brand_page_url(self, brand_profile: dict[str, object] | None) -> str | None:
        if not brand_profile:
            return None
        raw_url = str(brand_profile.get("official_facebook_url") or "").strip()
        if not raw_url:
            return None
        markers = _extract_facebook_page_markers(raw_url)
        return markers.get("url") or raw_url

    def _build_posts_payload(
        self,
        page_url: str,
        *,
        requested_from: str | None,
        requested_to: str | None,
    ) -> dict[str, object]:
        payload = {
            "startUrls": [{"url": page_url}],
            "resultsLimit": self.posts_limit,
        }
        start_date = _format_date_param(requested_from)
        end_date = _format_date_param(requested_to)
        if start_date:
            payload["startDate"] = start_date
        if end_date:
            payload["endDate"] = end_date
        return payload

    def _build_reels_payload(self, page_url: str) -> dict[str, object]:
        return {
            "startUrls": [{"url": page_url}],
            "resultsLimit": self.reels_limit,
        }

    def _build_post_item(
        self,
        record: dict[str, object],
        *,
        brand_profile: dict[str, object],
        requested_from: str | None,
        requested_to: str | None,
        seen_at: str,
    ) -> dict[str, object] | None:
        published_raw = _stringify_value(record.get("timestamp"))
        if not is_between(published_raw, requested_from, requested_to):
            return None
        published_dt = parse_datetime(published_raw)
        author = record.get("author") if isinstance(record.get("author"), dict) else {}
        source_name = (
            _stringify_value(author.get("name"))
            or _stringify_value(record.get("pageName"))
            or str(brand_profile.get("name") or "Facebook")
        )
        content_url = (
            _stringify_value(record.get("url"))
            or _stringify_value(record.get("postUrl"))
            or _stringify_value(record.get("permalink"))
        )
        if not content_url:
            return None
        if "facebook.com/reel/" in content_url and "www.facebook.com/reel/" not in content_url:
            content_url = content_url.replace("https://facebook.com/reel/", "https://www.facebook.com/reel/")
        external_id = _stringify_value(record.get("postId")) or content_url
        title = source_name
        body_text = (
            _stringify_value(record.get("postText"))
            or _stringify_value(record.get("text"))
            or _stringify_value(record.get("message"))
        )
        thumbnail_url = (
            _stringify_value(record.get("image"))
            or _stringify_value((record.get("author") or {}).get("profilePicture"))
        )
        return {
            "platform": self.platform,
            "source_kind": self.source_kind,
            "content_type": "reel" if "/reel/" in content_url else "post",
            "external_id": f"fbpost:{external_id}",
            "source_name": source_name,
            "author_name": _stringify_value(author.get("name")) or source_name,
            "title": title,
            "body_text": body_text,
            "normalized_text": build_normalized_text(title, body_text, source_name),
            "thumbnail_url": thumbnail_url,
            "view_count": parse_count(record.get("views")),
            "like_count": parse_count(record.get("reactionsCount") or record.get("likes")),
            "dislike_count": None,
            "comment_count": parse_count(record.get("commentsCount")),
            "channel_subscriber_count": None,
            "content_url": content_url,
            "permalink": content_url,
            "language": _stringify_value(record.get("language")),
            "published_at": isoformat(published_dt) if published_dt else published_raw,
            "first_seen_at": seen_at,
            "last_seen_at": seen_at,
            "raw_payload": {"source": "Apify Facebook Posts", "record": record},
        }

    def _build_reel_item(
        self,
        record: dict[str, object],
        *,
        brand_profile: dict[str, object],
        requested_from: str | None,
        requested_to: str | None,
        seen_at: str,
    ) -> dict[str, object] | None:
        published_raw = (
            _stringify_value(record.get("timestamp"))
            or _stringify_value(record.get("date_posted"))
            or _stringify_value(record.get("upload_date"))
            or _stringify_value(record.get("time"))
        )
        if published_raw and not is_between(published_raw, requested_from, requested_to):
            return None
        published_dt = parse_datetime(published_raw) if published_raw else None
        source_name = (
            _stringify_value(record.get("channel"))
            or _stringify_value(record.get("uploader"))
            or _stringify_value(record.get("pageName"))
            or str(brand_profile.get("name") or "Facebook")
        )
        content_url = (
            _stringify_value(record.get("webpage_url"))
            or _stringify_value(record.get("url"))
            or _stringify_value(record.get("original_url"))
            or _stringify_value(record.get("topLevelReelUrl"))
        )
        if not content_url:
            return None
        if "facebook.com/reel/" in content_url and "www.facebook.com/reel/" not in content_url:
            content_url = content_url.replace("https://facebook.com/reel/", "https://www.facebook.com/reel/")
        external_id = (
            _stringify_value(record.get("post_id"))
            or _stringify_value(record.get("id"))
            or content_url
        )
        title = (
            _stringify_value(record.get("title"))
            or _stringify_value(record.get("caption"))
            or source_name
        )
        body_text = (
            _stringify_value(record.get("description"))
            or _stringify_value(record.get("content"))
            or _stringify_value(record.get("text"))
        )
        thumbnail_url = (
            _stringify_value(record.get("thumbnail"))
            or _stringify_value(record.get("thumbnail_url"))
            or _stringify_value(record.get("image"))
        )
        return {
            "platform": self.platform,
            "source_kind": self.source_kind,
            "content_type": "reel",
            "external_id": f"fbreel:{external_id}",
            "source_name": source_name,
            "author_name": source_name,
            "title": title,
            "body_text": body_text,
            "normalized_text": build_normalized_text(title, body_text, source_name),
            "thumbnail_url": thumbnail_url,
            "view_count": parse_count(record.get("view_count") or record.get("video_view_count") or record.get("plays")),
            "like_count": parse_count(record.get("likes") or record.get("reactionsCount")),
            "dislike_count": None,
            "comment_count": parse_count(record.get("num_comments") or record.get("commentsCount")),
            "channel_subscriber_count": None,
            "content_url": content_url,
            "permalink": content_url,
            "language": _stringify_value(record.get("language")),
            "published_at": isoformat(published_dt) if published_dt else published_raw,
            "first_seen_at": seen_at,
            "last_seen_at": seen_at,
            "raw_payload": {"source": "Apify Facebook Reels", "record": record},
        }

    def collect(
        self,
        terms: list[str],
        requested_from: str | None,
        requested_to: str | None,
        *,
        brand_profile: dict[str, object] | None = None,
    ) -> AdapterResult:
        items: list[dict[str, object]] = []
        warnings: list[str] = []
        for result in self.collect_iter(
            terms,
            requested_from,
            requested_to,
            brand_profile=brand_profile,
        ):
            items.extend(result.items)
            warnings.extend(result.warnings)
        return AdapterResult(items=items, warnings=warnings)

    def collect_iter(
        self,
        terms: list[str],
        requested_from: str | None,
        requested_to: str | None,
        *,
        brand_profile: dict[str, object] | None = None,
    ):
        if not self.token:
            yield AdapterResult(items=[], warnings=[])
            return

        page_url = self._brand_page_url(brand_profile)
        if not page_url:
            yield AdapterResult(items=[], warnings=[])
            return

        seen_at = isoformat(utcnow())
        for reels_records, reel_warnings in self._stream_actor_records(
            self.reels_actor_id,
            self._build_reels_payload(page_url),
        ):
            reel_items: list[dict[str, object]] = []
            for record in reels_records:
                item = self._build_reel_item(
                    record,
                    brand_profile=brand_profile or {},
                    requested_from=requested_from,
                    requested_to=requested_to,
                    seen_at=seen_at,
                )
                if item:
                    reel_items.append(item)
            if reel_items or reel_warnings:
                yield AdapterResult(items=reel_items, warnings=reel_warnings)
            if any(_is_apify_quota_warning(message) for message in reel_warnings):
                return

        for posts_records, post_warnings in self._stream_actor_records(
            self.posts_actor_id,
            self._build_posts_payload(
                page_url,
                requested_from=requested_from,
                requested_to=requested_to,
            ),
        ):
            post_items: list[dict[str, object]] = []
            for record in posts_records:
                item = self._build_post_item(
                    record,
                    brand_profile=brand_profile or {},
                    requested_from=requested_from,
                    requested_to=requested_to,
                    seen_at=seen_at,
                )
                if item:
                    post_items.append(item)
            if post_items or post_warnings:
                yield AdapterResult(items=post_items, warnings=post_warnings)
            if any(_is_apify_quota_warning(message) for message in post_warnings):
                return


class ExternalApiPlatformAdapter(BaseAdapter):
    source_kind = "custom-api"

    def __init__(
        self,
        *,
        platform: str,
        sources: list[dict[str, object]],
    ) -> None:
        self.platform = platform
        self.sources = [
            ExternalApiSourceConfig(
                id=int(source["id"]),
                name=str(source["name"]),
                platform=str(source["platform"]),
                method=str(source["method"]).upper(),
                url_template=str(source["url_template"]),
                headers=dict(source.get("headers") or {}),
                body_template=_stringify_value(source.get("body_template")),
                results_path=_stringify_value(source.get("results_path")),
                field_mapping=dict(source.get("field_mapping") or {}),
                pagination=dict(source.get("pagination") or {}),
                is_enabled=bool(source.get("is_enabled")),
            )
            for source in sources
            if source.get("is_enabled")
        ]

    def _perform_request(
        self,
        *,
        source: ExternalApiSourceConfig,
        terms: list[str],
        requested_from: str | None,
        requested_to: str | None,
    ) -> tuple[list[dict[str, object]], list[str]]:
        query_text = " OR ".join(terms) if terms else ""
        raw_context = {
            "query": query_text,
            "from": requested_from or "",
            "to": requested_to or "",
        }
        url_context = {
            "query": quote_plus(query_text),
            "from": quote_plus(requested_from or ""),
            "to": quote_plus(requested_to or ""),
        }
        seen_at = isoformat(utcnow())
        items: list[dict[str, object]] = []
        warnings: list[str] = []
        pagination = dict(source.pagination or {})
        cursor_path = _stringify_value(pagination.get("cursor_path"))
        inject_into = (_stringify_value(pagination.get("inject_into")) or "query").lower()
        param_name = _stringify_value(pagination.get("param_name")) or "cursor"
        max_pages = max(1, parse_count(pagination.get("max_pages")) or 1)
        auto_cursor_pagination = not pagination and source.method == "GET"
        if auto_cursor_pagination:
            max_pages = 5
        current_cursor: str | None = None
        seen_cursors: set[str] = set()

        for page_index in range(max_pages):
            final_url = _apply_template_string(source.url_template, url_context)
            headers = _apply_template_payload(source.headers, raw_context)
            request_headers = {str(key): str(value) for key, value in (headers or {}).items()}
            rendered_body: object | None = None

            parsed_url = urlparse(final_url)
            existing_query = dict(parse_qsl(parsed_url.query, keep_blank_values=True))
            if (
                source.platform == "facebook"
                and "facebook-scraper3.p.rapidapi.com" in parsed_url.netloc
                and parsed_url.path == "/search/posts"
            ):
                facebook_params: dict[str, str] = {}
                from_date = _format_date_param(requested_from)
                to_date = _format_date_param(requested_to)
                if (requested_from or requested_to) and "recent_posts" not in existing_query:
                    facebook_params["recent_posts"] = "true"
                if from_date and "start_date" not in existing_query:
                    facebook_params["start_date"] = from_date
                if to_date and "end_date" not in existing_query:
                    facebook_params["end_date"] = to_date
                if facebook_params:
                    final_url = _append_query_params(final_url, facebook_params)
                    parsed_url = urlparse(final_url)
                    existing_query = dict(parse_qsl(parsed_url.query, keep_blank_values=True))

            if source.method == "POST" and source.body_template:
                try:
                    parsed_body = json.loads(source.body_template)
                except json.JSONDecodeError:
                    return [], [f"{source.name}: body JSON gecersiz."]
                rendered_body = _apply_template_payload(parsed_body, raw_context)

            if current_cursor:
                if inject_into == "body":
                    if rendered_body is None:
                        rendered_body = {}
                    if not isinstance(rendered_body, dict):
                        warnings.append(f"{source.name}: cursor sadece obje body ile calisir.")
                        break
                    rendered_body[param_name] = current_cursor
                else:
                    final_url = _append_query_params(final_url, {param_name: current_cursor})

            request_data: bytes | None = None
            if source.method == "POST":
                request_headers.setdefault("Content-Type", "application/json")
                if rendered_body is not None:
                    request_data = json.dumps(rendered_body, ensure_ascii=False).encode("utf-8")

            try:
                request = Request(
                    final_url,
                    data=request_data,
                    headers=request_headers,
                    method=source.method,
                )
                with urlopen(request, timeout=25) as response:
                    payload = json.loads(response.read().decode("utf-8"))
            except HTTPError as exc:
                warnings.append(f"{source.name}: API cagrisi basarisiz ({exc.code}).")
                break
            except URLError as exc:
                warnings.append(f"{source.name}: baglanti hatasi ({exc.reason}).")
                break
            except json.JSONDecodeError:
                warnings.append(f"{source.name}: JSON cevabi okunamadi.")
                break

            if auto_cursor_pagination and not cursor_path and isinstance(payload, dict) and payload.get("cursor"):
                cursor_path = "cursor"
                inject_into = "query"
                param_name = "cursor"

            records = _resolve_path(payload, source.results_path) if source.results_path else payload
            if isinstance(records, dict):
                records = [records]
            if not isinstance(records, list):
                warnings.append(f"{source.name}: sonuc yolu liste dondurmuyor.")
                break

            for index, record in enumerate(records):
                if not isinstance(record, dict):
                    continue

                mapped = {
                    key: _extract_mapped_value(record, mapping_value)
                    for key, mapping_value in source.field_mapping.items()
                }
                title = _stringify_value(mapped.get("title"))
                body_text = _stringify_value(mapped.get("body_text"))
                source_name = _stringify_value(mapped.get("source_name")) or source.name
                author_name = _stringify_value(mapped.get("author_name")) or source_name
                content_url = _stringify_value(mapped.get("content_url")) or _stringify_value(mapped.get("permalink")) or final_url
                permalink = _stringify_value(mapped.get("permalink")) or content_url
                published_at_raw = _stringify_value(mapped.get("published_at"))
                if not is_between(published_at_raw, requested_from, requested_to):
                    continue
                published_at_dt = parse_datetime(published_at_raw)
                published_at = isoformat(published_at_dt) if published_at_dt else published_at_raw

                external_id = _stringify_value(mapped.get("external_id"))
                if not external_id:
                    digest = hashlib.sha1(
                        json.dumps(record, ensure_ascii=False, sort_keys=True).encode("utf-8")
                    ).hexdigest()
                    external_id = f"{source.id}-{page_index}-{index}-{digest[:16]}"

                items.append(
                    {
                        "platform": self.platform,
                        "source_kind": self.source_kind,
                        "content_type": _stringify_value(mapped.get("content_type")) or "post",
                        "external_id": external_id,
                        "source_name": source_name,
                        "author_name": author_name,
                        "title": title,
                        "body_text": body_text,
                        "normalized_text": build_normalized_text(title, body_text, source_name, author_name),
                        "thumbnail_url": _stringify_value(mapped.get("thumbnail_url")),
                        "view_count": parse_count(mapped.get("view_count")),
                        "like_count": parse_count(mapped.get("like_count")),
                        "dislike_count": parse_count(mapped.get("dislike_count")),
                        "comment_count": parse_count(mapped.get("comment_count")),
                        "channel_subscriber_count": parse_count(mapped.get("channel_subscriber_count")),
                        "content_url": content_url,
                        "permalink": permalink,
                        "language": _stringify_value(mapped.get("language")),
                        "published_at": published_at,
                        "first_seen_at": seen_at,
                        "last_seen_at": seen_at,
                        "raw_payload": {"source": source.name, "record": record, "page": page_index + 1},
                    }
                )

            if not cursor_path or page_index + 1 >= max_pages:
                break

            next_cursor = _stringify_value(_resolve_path(payload, cursor_path))
            if not next_cursor or next_cursor in seen_cursors:
                break
            seen_cursors.add(next_cursor)
            current_cursor = next_cursor

        return items, warnings

    def _perform_official_facebook_request(
        self,
        *,
        source: ExternalApiSourceConfig,
        brand_profile: dict[str, object] | None,
        requested_from: str | None,
        requested_to: str | None,
    ) -> tuple[list[dict[str, object]], list[str]]:
        if self.platform != "facebook" or not brand_profile:
            return [], []

        raw_url = str(brand_profile.get("official_facebook_url") or "").strip()
        if not raw_url:
            return [], []

        markers = _extract_facebook_page_markers(raw_url)
        official_query = markers.get("slug") or normalize_text(str(brand_profile.get("name") or "")).replace(" ", "")
        if not official_query:
            return [], ["facebook resmi sayfa: linkten sayfa bilgisi cozumlenemedi."]

        source_items, source_warnings = self._perform_request(
            source=source,
            terms=[official_query],
            requested_from=requested_from,
            requested_to=requested_to,
        )

        filtered_items: list[dict[str, object]] = []
        for item in source_items:
            raw_record = ((item.get("raw_payload") or {}).get("record") or {})
            if not isinstance(raw_record, dict):
                continue
            if not _facebook_author_matches(raw_record, brand_profile):
                continue
            official_item = dict(item)
            official_item["source_kind"] = "owned-page"
            filtered_items.append(official_item)

        return filtered_items, source_warnings

    def _mark_matching_official_facebook_items(
        self,
        items: list[dict[str, object]],
        brand_profile: dict[str, object] | None,
    ) -> list[dict[str, object]]:
        if self.platform != "facebook" or not brand_profile:
            return items

        marked_items: list[dict[str, object]] = []
        for item in items:
            raw_record = ((item.get("raw_payload") or {}).get("record") or {})
            if isinstance(raw_record, dict) and _facebook_author_matches(raw_record, brand_profile):
                official_item = dict(item)
                official_item["source_kind"] = "owned-page"
                marked_items.append(official_item)
            else:
                marked_items.append(item)
        return marked_items

    def collect(
        self,
        terms: list[str],
        requested_from: str | None,
        requested_to: str | None,
        *,
        brand_profile: dict[str, object] | None = None,
    ) -> AdapterResult:
        items: list[dict[str, object]] = []
        warnings: list[str] = []
        for source in self.sources:
            source_items, source_warnings = self._perform_request(
                source=source,
                terms=terms,
                requested_from=requested_from,
                requested_to=requested_to,
            )
            source_items = self._mark_matching_official_facebook_items(source_items, brand_profile)
            items.extend(source_items)
            warnings.extend(source_warnings)
            official_items, official_warnings = self._perform_official_facebook_request(
                source=source,
                brand_profile=brand_profile,
                requested_from=requested_from,
                requested_to=requested_to,
            )
            items.extend(official_items)
            warnings.extend(official_warnings)
        return AdapterResult(items=items, warnings=warnings)


class StubPlatformAdapter(BaseAdapter):
    def __init__(self, platform: str, message: str) -> None:
        self.platform = platform
        self.message = message

    def collect(
        self,
        terms: list[str],
        requested_from: str | None,
        requested_to: str | None,
        *,
        brand_profile: dict[str, object] | None = None,
    ) -> AdapterResult:
        return AdapterResult(
            items=[],
            warnings=[f"{self.platform}: {self.message}"],
            allow_demo_fallback=True,
        )


def available_platforms() -> list[str]:
    return ["facebook", "instagram", "youtube", "linkedin"]


def build_adapters(settings: Settings) -> dict[str, BaseAdapter]:
    external_sources = list_external_api_sources(enabled_only=True)
    sources_by_platform: dict[str, list[dict[str, object]]] = {platform: [] for platform in available_platforms()}
    for source in external_sources:
        platform = str(source.get("platform") or "").strip().lower()
        if platform in sources_by_platform:
            sources_by_platform[platform].append(source)

    youtube_adapters: list[BaseAdapter] = [
        YouTubeAdapter(
            api_key=settings.youtube_api_key,
            owned_channels_path=settings.owned_youtube_channels_path,
            target_language=settings.target_language,
            target_region=settings.target_region,
            strict_language_filter=settings.strict_language_filter,
            max_results=settings.youtube_max_results,
            max_pages=settings.youtube_max_pages,
            fetch_comments=settings.youtube_fetch_comments,
            comment_threads_per_video=settings.youtube_comment_threads_per_video,
        )
    ]
    if sources_by_platform["youtube"]:
        youtube_adapters.append(
            ExternalApiPlatformAdapter(
                platform="youtube",
                sources=sources_by_platform["youtube"],
            )
        )

    facebook_adapters: list[BaseAdapter] = []
    if settings.apify_token:
        facebook_adapters.append(
            ApifyFacebookOfficialAdapter(
                token=settings.apify_token,
                posts_actor_id=settings.apify_facebook_posts_actor_id,
                reels_actor_id=settings.apify_facebook_reels_actor_id,
                posts_limit=settings.apify_facebook_posts_limit,
                reels_limit=settings.apify_facebook_reels_limit,
            )
        )
        facebook_adapters.append(
            ApifyFacebookSearchAdapter(
                token=settings.apify_token,
                actor_id=settings.apify_facebook_search_actor_id,
                results_limit=settings.apify_facebook_search_limit,
            )
        )
    if sources_by_platform["facebook"] and not settings.apify_token:
        facebook_adapters.append(
            ExternalApiPlatformAdapter(
                platform="facebook",
                sources=sources_by_platform["facebook"],
            )
        )

    return {
        "facebook": CompositeAdapter("facebook", facebook_adapters) if facebook_adapters else StubPlatformAdapter(
            "facebook",
            "gercek connector henuz bagli degil; resmi API veya vendor adaptor gerekecek.",
        ),
        "instagram": ExternalApiPlatformAdapter(
            platform="instagram",
            sources=sources_by_platform["instagram"],
        ) if sources_by_platform["instagram"] else StubPlatformAdapter(
            "instagram",
            "gercek connector henuz bagli degil; hashtag veya tracked source odakli adaptor gerekecek.",
        ),
        "youtube": CompositeAdapter("youtube", youtube_adapters),
        "linkedin": ExternalApiPlatformAdapter(
            platform="linkedin",
            sources=sources_by_platform["linkedin"],
        ) if sources_by_platform["linkedin"] else StubPlatformAdapter(
            "linkedin",
            "gercek connector henuz bagli degil; owned company page veya vendor adaptor gerekecek.",
        ),
    }
