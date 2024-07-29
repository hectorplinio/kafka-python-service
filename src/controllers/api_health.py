from flask import Blueprint, Response, make_response

from services.health_service import HealthService

health_bp = Blueprint("health", __name__)

health_service = HealthService()


@health_bp.route("/check", methods=["GET"])
def check() -> Response:

    return make_response(health_service.check_health(), 200)
