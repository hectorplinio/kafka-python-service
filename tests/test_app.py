import pytest

from src.app import create_app


@pytest.fixture
def app():
    app = create_app()
    return app


@pytest.fixture
def client(app):
    return app.test_client()


def test_app_creation(client):
    response = client.get("/check")
    assert response.status_code == 200
    assert response.get_json() == {"status": "ok"}
