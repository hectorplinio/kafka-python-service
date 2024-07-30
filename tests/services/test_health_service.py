import pytest

from src.services.health.health_service import HealthService


@pytest.fixture
def health_service():
    return HealthService()


def test_check_health(health_service):
    result = health_service.check_health()
    assert result == {"status": "ok"}
