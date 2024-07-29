import pytest
from flask import Flask

from src.controllers.api_health import health_bp


@pytest.fixture
def app():
    app = Flask(__name__)
    app.register_blueprint(health_bp)
    return app


@pytest.fixture
def client(app):
    return app.test_client()


def test_health_check(client):
    response = client.get("/check")
    assert response.status_code == 200
    assert response.get_json() == {"status": "ok"}
