import pytest
from flask import Flask

from src.controllers.health.api_health import health_bp


@pytest.fixture
def app():
    app = Flask(__name__)
    app.register_blueprint(health_bp, url_prefix="/api/check")
    return app


@pytest.fixture
def client(app):
    return app.test_client()


def test_health_check(client):
    response = client.get("/api/check/")
    print(response.get_json())
    assert response.status_code == 200
    assert response.get_json() == {"status": "ok"}
