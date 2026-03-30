from __future__ import annotations

import logging
from threading import Lock
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from flask import Flask, jsonify, render_template, request
from werkzeug.middleware.proxy_fix import ProxyFix

from .config import load_settings
from .db import INTEGRITY_ERRORS, close_db, init_schema
from .repository import (
    create_brand_profile,
    delete_brand_profile,
    ensure_default_brand_profiles,
    list_brand_profiles,
    set_item_read_state,
    touch_brand_profile,
    update_brand_profile,
)
from .services import CollectionService


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
        )

    @app.get("/api/brands")
    def brands() -> tuple[object, int]:
        return jsonify({"items": list_brand_profiles()}), 200

    @app.post("/api/brands")
    def create_brand() -> tuple[object, int]:
        payload = request.get_json(silent=True) or request.form
        name = (payload.get("name") or "").strip()
        query_text = (payload.get("query") or "").strip()
        official_youtube_url = (payload.get("official_youtube_url") or "").strip() or None
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
                requested_from=requested_from,
                requested_to=requested_to,
            )
        except INTEGRITY_ERRORS:
            return jsonify({"error": "Bu marka adi zaten var."}), 409
        return jsonify(brand), 201

    @app.patch("/api/brands/<int:brand_id>")
    def update_brand(brand_id: int) -> tuple[object, int]:
        payload = request.get_json(silent=True) or request.form
        name = (payload.get("name") or "").strip()
        query_text = (payload.get("query") or "").strip()
        official_youtube_url = (payload.get("official_youtube_url") or "").strip() or None
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
                requested_from=requested_from,
                requested_to=requested_to,
            )
        except INTEGRITY_ERRORS:
            return jsonify({"error": "Bu marka adi zaten var."}), 409
        if brand is None:
            return jsonify({"error": "Marka bulunamadi."}), 404
        return jsonify(brand), 200

    @app.delete("/api/brands/<int:brand_id>")
    def delete_brand(brand_id: int) -> tuple[object, int]:
        deleted = delete_brand_profile(brand_id)
        if not deleted:
            return jsonify({"error": "Marka bulunamadi."}), 404
        return jsonify({"ok": True, "id": brand_id}), 200

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
