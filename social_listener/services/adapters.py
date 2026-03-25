from __future__ import annotations

import html
import json
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from langdetect import DetectorFactory, LangDetectException, detect

from ..config import Settings

DetectorFactory.seed = 0


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
    unescaped = html.unescape(value)
    return normalize_text(unescaped).translate(TURKISH_ASCII_MAP)


def build_match_text(item: dict[str, object]) -> str:
    return " ".join(
        part
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
    if item.get("source_kind") == "owned-channel":
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
    cleaned = value.strip().lower()
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


class BaseAdapter:
    platform = "base"
    source_kind = "official-api"

    def collect(
        self,
        terms: list[str],
        requested_from: str | None,
        requested_to: str | None,
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
    ) -> tuple[list[dict[str, object]], list[str]]:
        matched_channels = self._matched_owned_channels(terms)
        if not matched_channels:
            return [], []

        items: list[dict[str, object]] = []
        warnings: list[str] = []
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


class StubPlatformAdapter(BaseAdapter):
    def __init__(self, platform: str, message: str) -> None:
        self.platform = platform
        self.message = message

    def collect(
        self,
        terms: list[str],
        requested_from: str | None,
        requested_to: str | None,
    ) -> AdapterResult:
        return AdapterResult(
            items=[],
            warnings=[f"{self.platform}: {self.message}"],
            allow_demo_fallback=True,
        )


def available_platforms() -> list[str]:
    return ["facebook", "instagram", "youtube", "linkedin"]


def build_adapters(settings: Settings) -> dict[str, BaseAdapter]:
    return {
        "facebook": StubPlatformAdapter(
            "facebook",
            "gercek connector henuz bagli degil; resmi API veya vendor adaptor gerekecek.",
        ),
        "instagram": StubPlatformAdapter(
            "instagram",
            "gercek connector henuz bagli degil; hashtag veya tracked source odakli adaptor gerekecek.",
        ),
        "youtube": YouTubeAdapter(
            api_key=settings.youtube_api_key,
            owned_channels_path=settings.owned_youtube_channels_path,
            target_language=settings.target_language,
            target_region=settings.target_region,
            strict_language_filter=settings.strict_language_filter,
            max_results=settings.youtube_max_results,
            max_pages=settings.youtube_max_pages,
            fetch_comments=settings.youtube_fetch_comments,
            comment_threads_per_video=settings.youtube_comment_threads_per_video,
        ),
        "linkedin": StubPlatformAdapter(
            "linkedin",
            "gercek connector henuz bagli degil; owned company page veya vendor adaptor gerekecek.",
        ),
    }
