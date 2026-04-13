from __future__ import annotations

import json
import logging
import sys
from threading import Lock
from threading import Thread
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from flask import Flask, jsonify, render_template, request
from werkzeug.middleware.proxy_fix import ProxyFix

from .config import load_settings
from .db import INTEGRITY_ERRORS, close_db, init_schema
from .repository import (
    create_brand_profile,
    create_external_api_source,
    delete_brand_profile,
    delete_external_api_source,
    ensure_default_brand_profiles,
    list_external_api_sources,
    list_brand_profiles,
    set_external_api_source_enabled,
    set_item_read_state,
    touch_brand_profile,
    update_brand_profile,
)
from .services import CollectionService
from .services.adapters import available_platforms


def _normalize_datetime(raw_value: str | None, timezone_name: str) -> str | None:
    if not raw_value:
        return None

    value = raw_value.strip()
    if not value:
        return None

    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return value

    if parsed.tzinfo is None:
        try:
            parsed = parsed.replace(tzinfo=ZoneInfo(timezone_name))
        except ZoneInfoNotFoundError:
            parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).replace(microsecond=0).isoformat()


def _coerce_platform_list(raw_value: object, *, fallback: list[str] | None = None) -> list[str]:
    if isinstance(raw_value, str):
        items = [part.strip() for part in raw_value.split(",")]
    elif isinstance(raw_value, list):
        items = [str(part).strip() for part in raw_value]
    else:
        items = fallback or []
    return [item for item in items if item]


def _coerce_bool(raw_value: object, default: bool = True) -> bool:
    if raw_value is None:
        return default
    if isinstance(raw_value, bool):
        return raw_value
    return str(raw_value).strip().lower() in {"1", "true", "yes", "on"}


def _parse_json_payload(raw_value: object, *, fallback: object) -> object:
    if raw_value is None:
        return fallback
    if isinstance(raw_value, (dict, list)):
        return raw_value
    text = str(raw_value).strip()
    if not text:
        return fallback
    return json.loads(text)


def create_app() -> Flask:
    settings = load_settings()
    app = Flask(
        __name__,
        template_folder=str(Path(__file__).parent / "templates"),
        static_folder=str(Path(__file__).parent / "static"),
    )
    app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_port=1)
    app.config["SETTINGS"] = settings
    app.secret_key = settings.secret_key
    app.logger.setLevel(logging.INFO)
    init_lock = Lock()
    init_state = {"ready": False}
    collect_jobs: dict[str, dict[str, object]] = {}
    collect_jobs_lock = Lock()

    app.teardown_appcontext(close_db)

    def ensure_data_store_ready() -> None:
        if init_state["ready"]:
            return

        with init_lock:
            if init_state["ready"]:
                return
            with app.app_context():
                init_schema()
                ensure_default_brand_profiles()
            init_state["ready"] = True

    @app.before_request
    def initialize_data_store_if_needed():
        if request.path in {"/", "/health"} or request.path.startswith("/static/"):
            return None
        try:
            ensure_data_store_ready()
        except Exception:  # pragma: no cover - runtime safety
            app.logger.exception("Veritabani hazirlanamadi.")
            return jsonify({"error": "Veritabani hazirlanamadi. Lutfen biraz sonra tekrar deneyin."}), 503

    @app.get("/")
    def index() -> str:
        return render_template(
            "index.html",
            demo_enabled=settings.enable_demo_data,
            show_api_source_ui=settings.show_api_source_ui,
        )

    @app.get("/api/brands")
    def brands() -> tuple[object, int]:
        return jsonify({"items": list_brand_profiles()}), 200

    @app.get("/api/api-sources")
    def api_sources() -> tuple[object, int]:
        return jsonify({"items": list_external_api_sources()}), 200

    @app.post("/api/brands")
    def create_brand() -> tuple[object, int]:
        payload = request.get_json(silent=True) or request.form
        name = (payload.get("name") or "").strip()
        query_text = (payload.get("query") or "").strip()
        official_youtube_url = (payload.get("official_youtube_url") or "").strip() or None
        official_facebook_url = (payload.get("official_facebook_url") or "").strip() or None
        if not name:
            return jsonify({"error": "Marka adi zorunlu."}), 400
        if not query_text:
            return jsonify({"error": "Filtre sorgusu zorunlu."}), 400

        platform_list = _coerce_platform_list(payload.get("platforms"), fallback=["youtube"])
        requested_from = _normalize_datetime(payload.get("from"), settings.app_timezone)
        requested_to = _normalize_datetime(payload.get("to"), settings.app_timezone)
        try:
            brand = create_brand_profile(
                name=name,
                query_text=query_text,
                platforms=platform_list,
                official_youtube_url=official_youtube_url,
                official_facebook_url=official_facebook_url,
                requested_from=requested_from,
                requested_to=requested_to,
            )
        except INTEGRITY_ERRORS:
            return jsonify({"error": "Bu marka adi zaten var."}), 409
        return jsonify(brand), 201

    @app.post("/api/api-sources")
    def create_api_source() -> tuple[object, int]:
        payload = request.get_json(silent=True) or request.form
        name = (payload.get("name") or "").strip()
        platform = (payload.get("platform") or "").strip().lower()
        method = (payload.get("method") or "GET").strip().upper()
        url_template = (payload.get("url_template") or "").strip()
        body_template = (payload.get("body_template") or "").strip() or None
        results_path = (payload.get("results_path") or "").strip() or None
        is_enabled = _coerce_bool(payload.get("is_enabled"), True)

        if not name:
            return jsonify({"error": "Kaynak adi zorunlu."}), 400
        if platform not in available_platforms():
            return jsonify({"error": "Gecersiz platform."}), 400
        if method not in {"GET", "POST"}:
            return jsonify({"error": "Sadece GET veya POST destekleniyor."}), 400
        if not url_template:
            return jsonify({"error": "Endpoint URL zorunlu."}), 400

        try:
            headers = _parse_json_payload(payload.get("headers_json"), fallback={})
            field_mapping = _parse_json_payload(payload.get("field_mapping_json"), fallback={})
            pagination = _parse_json_payload(payload.get("pagination_json"), fallback={})
        except json.JSONDecodeError:
            return jsonify({"error": "JSON alanlarinda gecersiz format var."}), 400

        if not isinstance(headers, dict):
            return jsonify({"error": "Header JSON bir obje olmali."}), 400
        if not isinstance(field_mapping, dict):
            return jsonify({"error": "Alan esleme JSON bir obje olmali."}), 400
        if not isinstance(pagination, dict):
            return jsonify({"error": "Sayfalama JSON bir obje olmali."}), 400
        if not field_mapping.get("external_id") and not field_mapping.get("content_url") and not field_mapping.get("permalink"):
            return jsonify({"error": "Alan eslemede en az external_id veya content_url/permalink tanimlayin."}), 400

        try:
            source = create_external_api_source(
                name=name,
                platform=platform,
                method=method,
                url_template=url_template,
                headers=headers,
                body_template=body_template,
                results_path=results_path,
                field_mapping=field_mapping,
                pagination=pagination,
                is_enabled=is_enabled,
            )
        except INTEGRITY_ERRORS:
            return jsonify({"error": "Bu kaynak adi zaten var."}), 409
        return jsonify(source), 201

    @app.patch("/api/brands/<int:brand_id>")
    def update_brand(brand_id: int) -> tuple[object, int]:
        payload = request.get_json(silent=True) or request.form
        name = (payload.get("name") or "").strip()
        query_text = (payload.get("query") or "").strip()
        official_youtube_url = (payload.get("official_youtube_url") or "").strip() or None
        official_facebook_url = (payload.get("official_facebook_url") or "").strip() or None
        if not name:
            return jsonify({"error": "Marka adi zorunlu."}), 400
        if not query_text:
            return jsonify({"error": "Filtre sorgusu zorunlu."}), 400

        platform_list = _coerce_platform_list(payload.get("platforms"), fallback=["youtube"])
        requested_from = _normalize_datetime(payload.get("from"), settings.app_timezone)
        requested_to = _normalize_datetime(payload.get("to"), settings.app_timezone)
        try:
            brand = update_brand_profile(
                brand_id=brand_id,
                name=name,
                query_text=query_text,
                platforms=platform_list,
                official_youtube_url=official_youtube_url,
                official_facebook_url=official_facebook_url,
                requested_from=requested_from,
                requested_to=requested_to,
            )
        except INTEGRITY_ERRORS:
            return jsonify({"error": "Bu marka adi zaten var."}), 409
        if brand is None:
            return jsonify({"error": "Marka bulunamadi."}), 404
        return jsonify(brand), 200

    @app.patch("/api/api-sources/<int:source_id>")
    def update_api_source(source_id: int) -> tuple[object, int]:
        payload = request.get_json(silent=True) or request.form
        if "is_enabled" not in payload:
            return jsonify({"error": "is_enabled alani zorunlu."}), 400

        source = set_external_api_source_enabled(source_id, _coerce_bool(payload.get("is_enabled"), True))
        if source is None:
            return jsonify({"error": "API kaynagi bulunamadi."}), 404
        return jsonify(source), 200

    @app.delete("/api/brands/<int:brand_id>")
    def delete_brand(brand_id: int) -> tuple[object, int]:
        deleted = delete_brand_profile(brand_id)
        if not deleted:
            return jsonify({"error": "Marka bulunamadi."}), 404
        return jsonify({"ok": True, "id": brand_id}), 200

    @app.delete("/api/api-sources/<int:source_id>")
    def delete_api_source(source_id: int) -> tuple[object, int]:
        deleted = delete_external_api_source(source_id)
        if not deleted:
            return jsonify({"error": "API kaynagi bulunamadi."}), 404
        return jsonify({"ok": True, "id": source_id}), 200

    @app.get("/health")
    def health() -> tuple[dict[str, object], int]:
        return {"status": "ok"}, 200

    @app.post("/api/collect")
    def collect() -> tuple[object, int]:
        payload = request.get_json(silent=True) or request.form
        raw_query = (payload.get("query") or "").strip()
        if not raw_query:
            return jsonify({"error": "query alani zorunlu."}), 400

        platforms = payload.get("platforms")
        if isinstance(platforms, str):
            platform_list = [part for part in platforms.split(",") if part]
        elif isinstance(platforms, list):
            platform_list = [str(part) for part in platforms]
        else:
            platform_list = request.args.getlist("platform")

        requested_from = _normalize_datetime(payload.get("from"), settings.app_timezone)
        requested_to = _normalize_datetime(payload.get("to"), settings.app_timezone)
        raw_brand_id = payload.get("brand_id")
        brand_id = int(raw_brand_id) if str(raw_brand_id).strip().isdigit() else None
        background = _coerce_bool(payload.get("background"), False)

        if background:
            job_id = uuid4().hex
            with collect_jobs_lock:
                collect_jobs[job_id] = {
                    "id": job_id,
                    "status": "queued",
                    "query": raw_query,
                    "platforms": platform_list,
                    "requested_from": requested_from,
                    "requested_to": requested_to,
                    "brand_id": brand_id,
                    "batches_completed": 0,
                    "items_written": 0,
                    "warnings": [],
                    "summary": {},
                    "error": None,
                }

            def run_background_collection() -> None:
                with collect_jobs_lock:
                    collect_jobs[job_id]["status"] = "running"

                def on_batch(batch: dict[str, object]) -> None:
                    with collect_jobs_lock:
                        job = collect_jobs.get(job_id)
                        if not job:
                            return
                        job["batches_completed"] = int(job.get("batches_completed", 0)) + 1
                        job["items_written"] = batch.get("total_written", job.get("items_written", 0))
                        existing_warnings = list(job.get("warnings") or [])
                        existing_warnings.extend(batch.get("warnings") or [])
                        job["warnings"] = existing_warnings

                try:
                    with app.app_context():
                        ensure_data_store_ready()
                        service = CollectionService(settings)
                        result = service.collect_progressive(
                            raw_query=raw_query,
                            platforms=platform_list,
                            requested_from=requested_from,
                            requested_to=requested_to,
                            brand_id=brand_id,
                            on_batch=on_batch,
                        )
                        if brand_id is not None:
                            touch_brand_profile(brand_id)
                    with collect_jobs_lock:
                        job = collect_jobs.get(job_id)
                        if job:
                            job["status"] = "completed"
                            job["summary"] = result.get("summary", {})
                            job["warnings"] = result.get("warnings", [])
                except Exception as exc:  # pragma: no cover - runtime safety
                    app.logger.exception("Arka plan toplama basarisiz.")
                    with collect_jobs_lock:
                        job = collect_jobs.get(job_id)
                        if job:
                            job["status"] = "failed"
                            job["error"] = str(exc)

            Thread(target=run_background_collection, daemon=True).start()
            return jsonify({"ok": True, "job_id": job_id, "status": "queued"}), 202

        service = CollectionService(settings)
        result = service.collect(
            raw_query=raw_query,
            platforms=platform_list,
            requested_from=requested_from,
            requested_to=requested_to,
            brand_id=brand_id,
        )
        if brand_id is not None:
            touch_brand_profile(brand_id)
        return jsonify(result), 200

    @app.get("/api/collect/<job_id>")
    def collect_status(job_id: str) -> tuple[object, int]:
        with collect_jobs_lock:
            job = collect_jobs.get(job_id)
            if not job:
                return jsonify({"error": "Toplama isi bulunamadi."}), 404
            return jsonify(job), 200

    @app.get("/api/search")
    def search() -> tuple[object, int]:
        raw_query = request.args.get("query", "").strip()
        if not raw_query:
            return jsonify({"error": "query parametresi zorunlu."}), 400

        platform_list = request.args.getlist("platform")
        requested_from = _normalize_datetime(request.args.get("from"), settings.app_timezone)
        requested_to = _normalize_datetime(request.args.get("to"), settings.app_timezone)
        raw_brand_id = request.args.get("brand_id")
        brand_id = int(raw_brand_id) if raw_brand_id and raw_brand_id.isdigit() else None

        try:
            service = CollectionService(settings)
            result = service.search(
                raw_query=raw_query,
                platforms=platform_list,
                requested_from=requested_from,
                requested_to=requested_to,
            )
            if brand_id is not None:
                touch_brand_profile(brand_id, result_count=int(result.get("count", 0)))
            return jsonify(result), 200
        except Exception:  # pragma: no cover - runtime safety
            app.logger.exception("Arama basarisiz.")
            exc = sys.exc_info()[1]
            detail = f"{type(exc).__name__}: {exc}" if exc else "Bilinmeyen hata"
            return jsonify(
                {
                    "error": "Arama gecici olarak kullanilamiyor. Lutfen biraz sonra tekrar deneyin.",
                    "detail": detail,
                }
            ), 500

    @app.post("/api/items/<int:item_id>/read")
    def update_read_state(item_id: int) -> tuple[object, int]:
        payload = request.get_json(silent=True) or request.form
        raw_value = payload.get("is_read")
        if isinstance(raw_value, bool):
            is_read = raw_value
        elif isinstance(raw_value, str):
            is_read = raw_value.strip().lower() in {"1", "true", "yes", "on"}
        else:
            return jsonify({"error": "is_read alani zorunlu."}), 400

        result = set_item_read_state(item_id=item_id, is_read=is_read)
        if result is None:
            return jsonify({"error": "icerik bulunamadi."}), 404
        return jsonify(result), 200

    return app
