from flask import Flask


def create_app():
    app = Flask(__name__)
    app.config.from_object("config")
    from controllers.api_health import health_bp

    app.register_blueprint(health_bp)

    return app


if __name__ == "__main__":
    app = create_app()
    app.run(host="0.0.0.0", port=5000)
