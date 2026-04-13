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


def _dedupe_items(items: list[dict[str, object]]) -> list[dict[str, object]]:
    deduped: list[dict[str, object]] = []
    seen_keys: set[tuple[str, str]] = set()
    for item in items:
        platform = str(item.get("platform") or "")
        content_url = str(item.get("content_url") or item.get("permalink") or "").strip().rstrip("/")
        external_id = str(item.get("external_id") or "").strip()
        key = (platform, content_url or external_id)
        if not key[1] or key in seen_keys:
            continue
        seen_keys.add(key)
        deduped.append(item)
    return deduped


class CollectionService:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.adapters = build_adapters(settings)

    def _adapter_results(
        self,
        adapter,
        terms: list[str],
        requested_from: str | None,
        requested_to: str | None,
        *,
        brand_profile: dict[str, object] | None = None,
    ):
        if hasattr(adapter, "collect_iter"):
            yield from adapter.collect_iter(
                terms,
                requested_from,
                requested_to,
                brand_profile=brand_profile,
            )
            return

        yield adapter.collect(
            terms,
            requested_from,
            requested_to,
            brand_profile=brand_profile,
        )

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

        all_items = _dedupe_items(all_items)
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

    def collect_progressive(
        self,
        raw_query: str,
        platforms: list[str] | None = None,
        requested_from: str | None = None,
        requested_to: str | None = None,
        brand_id: int | None = None,
        on_batch=None,
    ) -> dict[str, object]:
        target_platforms = platforms or available_platforms()
        terms = parse_query_terms(raw_query)
        brand_profile = repository.get_brand_profile(brand_id) if brand_id is not None else None

        repository.log_query(raw_query, terms, target_platforms, requested_from, requested_to)
        run_id = repository.start_collection_run(raw_query, target_platforms, requested_from, requested_to)

        warnings: list[str] = []
        summary_counter: Counter[str] = Counter()
        total_written = 0
        batch_index = 0

        for platform in target_platforms:
            adapter = self.adapters.get(platform)
            if adapter is None:
                message = f"{platform}: adaptor bulunamadi."
                warnings.append(message)
                if on_batch:
                    on_batch(
                        {
                            "platform": platform,
                            "batch_index": batch_index,
                            "items_written": 0,
                            "total_written": total_written,
                            "warnings": [message],
                        }
                    )
                continue

            try:
                results_iter = self._adapter_results(
                    adapter,
                    terms,
                    requested_from,
                    requested_to,
                    brand_profile=brand_profile,
                )
                for result in results_iter:
                    items = list(result.items)
                    warnings.extend(result.warnings)

                    if (
                        not items
                        and self.settings.enable_demo_data
                        and getattr(result, "allow_demo_fallback", False)
                    ):
                        demo = DemoAdapter(platform).collect(terms, requested_from, requested_to)
                        items = demo.items
                        warnings.extend(demo.warnings)

                    filtered_items = [item for item in items if item_matches_terms(item, terms)]
                    filtered_items = _dedupe_items(filtered_items)
                    written = repository.upsert_content_items(filtered_items) if filtered_items else 0

                    batch_index += 1
                    total_written += written
                    summary_counter[platform] += len(filtered_items)

                    if on_batch:
                        on_batch(
                            {
                                "platform": platform,
                                "batch_index": batch_index,
                                "items_written": written,
                                "total_written": total_written,
                                "warnings": list(result.warnings),
                            }
                        )
            except Exception as exc:  # pragma: no cover - runtime safety
                message = f"{platform}: connector hatasi: {exc}"
                warnings.append(message)
                if on_batch:
                    on_batch(
                        {
                            "platform": platform,
                            "batch_index": batch_index,
                            "items_written": 0,
                            "total_written": total_written,
                            "warnings": [message],
                        }
                    )

        summary = {
            "requested_platforms": target_platforms,
            "items_written": total_written,
            "items_by_platform": dict(summary_counter),
            "demo_enabled": self.settings.enable_demo_data,
        }
        repository.finish_collection_run(
            run_id=run_id,
            status="completed",
            result_count=total_written,
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
                try:
                    source_kind = str(row.get("source_kind") or "")
                    if source_kind.startswith("owned-"):
                        filtered_rows.append(row)
                        continue
                    text = " ".join(
                        str(part)
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
                except Exception:
                    continue
            rows = filtered_rows

        safe_rows = []
        for row in rows:
            try:
                if item_matches_terms(row, terms):
                    safe_rows.append(row)
            except Exception:
                continue
        rows = safe_rows
        return {
            "terms": terms,
            "count": len(rows),
            "items": rows,
            "platforms": target_platforms,
        }
