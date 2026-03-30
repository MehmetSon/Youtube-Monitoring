from __future__ import annotations

from collections import Counter

from .. import repository
from ..config import Settings
from .adapters import (
    DemoAdapter,
    available_platforms,
    build_adapters,
    item_matches_terms,
    matches_target_language,
)


def parse_query_terms(raw_query: str) -> list[str]:
    separators = [",", "\n", ";"]
    normalized = raw_query
    for separator in separators:
        normalized = normalized.replace(separator, "|")
    terms = []
    for piece in normalized.split("|"):
        cleaned = " ".join(piece.lower().split())
        if cleaned and cleaned not in terms:
            terms.append(cleaned)
    return terms


class CollectionService:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.adapters = build_adapters(settings)

    def collect(
        self,
        raw_query: str,
        platforms: list[str] | None = None,
        requested_from: str | None = None,
        requested_to: str | None = None,
        brand_id: int | None = None,
    ) -> dict[str, object]:
        target_platforms = platforms or available_platforms()
        terms = parse_query_terms(raw_query)
        brand_profile = repository.get_brand_profile(brand_id) if brand_id is not None else None

        repository.log_query(raw_query, terms, target_platforms, requested_from, requested_to)
        run_id = repository.start_collection_run(raw_query, target_platforms, requested_from, requested_to)

        all_items: list[dict[str, object]] = []
        warnings: list[str] = []
        summary_counter: Counter[str] = Counter()

        for platform in target_platforms:
            adapter = self.adapters.get(platform)
            if adapter is None:
                warnings.append(f"{platform}: adaptor bulunamadi.")
                continue

            try:
                result = adapter.collect(
                    terms,
                    requested_from,
                    requested_to,
                    brand_profile=brand_profile,
                )
            except Exception as exc:  # pragma: no cover - runtime safety
                result = type(
                    "Result",
                    (),
                    {
                        "items": [],
                        "warnings": [f"{platform}: connector hatasi: {exc}"],
                        "allow_demo_fallback": True,
                    },
                )()

            items = list(result.items)
            warnings.extend(result.warnings)

            if not items and self.settings.enable_demo_data and getattr(result, "allow_demo_fallback", False):
                demo = DemoAdapter(platform).collect(terms, requested_from, requested_to)
                items = demo.items
                warnings.extend(demo.warnings)

            filtered_items = []
            for item in items:
                if item_matches_terms(item, terms):
                    filtered_items.append(item)
            items = filtered_items

            summary_counter[platform] += len(items)
            all_items.extend(items)

        inserted = repository.upsert_content_items(all_items)
        summary = {
            "requested_platforms": target_platforms,
            "items_written": inserted,
            "items_by_platform": dict(summary_counter),
            "demo_enabled": self.settings.enable_demo_data,
        }
        repository.finish_collection_run(
            run_id=run_id,
            status="completed",
            result_count=inserted,
            warnings=warnings,
            summary=summary,
        )
        return {
            "run_id": run_id,
            "terms": terms,
            "warnings": warnings,
            "summary": summary,
        }

    def search(
        self,
        raw_query: str,
        platforms: list[str] | None = None,
        requested_from: str | None = None,
        requested_to: str | None = None,
        limit: int = 250,
    ) -> dict[str, object]:
        target_platforms = platforms or available_platforms()
        terms = parse_query_terms(raw_query)
        rows = repository.search_content(
            terms=terms,
            platforms=target_platforms,
            requested_from=requested_from,
            requested_to=requested_to,
            include_demo=self.settings.enable_demo_data,
            limit=limit,
        )
        if self.settings.strict_language_filter:
            filtered_rows = []
            for row in rows:
                text = " ".join(
                    part
                    for part in [
                        row.get("title") or "",
                        row.get("body_text") or "",
                        row.get("source_name") or "",
                    ]
                    if part
                )
                if matches_target_language(
                    language=row.get("language"),
                    text=text,
                    target_language=self.settings.target_language,
                ):
                    filtered_rows.append(row)
            rows = filtered_rows

        rows = [row for row in rows if item_matches_terms(row, terms)]
        return {
            "terms": terms,
            "count": len(rows),
            "items": rows,
            "platforms": target_platforms,
        }
