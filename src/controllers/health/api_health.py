from flask import Blueprint, Response, make_response

from src.services.health.health_service import HealthService

health_bp = Blueprint("health", __name__)

health_service = HealthService()


@health_bp.route("/", methods=["GET"])
def check() -> Response:

    return make_response(health_service.check_health(), 200)
