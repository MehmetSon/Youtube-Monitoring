import os

from social_listener.app import create_app


app = create_app()


if __name__ == "__main__":
    debug = os.getenv("APP_DEBUG", "").strip().lower() in {"1", "true", "yes", "on"}
    host = os.getenv("APP_HOST", "127.0.0.1").strip() or "127.0.0.1"
    port = int(os.getenv("PORT") or os.getenv("APP_PORT") or "5000")
    app.run(host=host, port=port, debug=debug)
