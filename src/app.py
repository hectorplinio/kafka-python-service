from flask import Flask

from src.controllers.health.api_health import health_bp


def create_app():
    app = Flask(__name__)

    app.register_blueprint(health_bp, url_prefix="/api/check")

    return app


if __name__ == "__main__":
    app = create_app()
    app.run(host="0.0.0.0", port=3000)
