from fastapi import APIRouter

from backend.app.services.analysis_service import get_map_layers

router = APIRouter(prefix="/map", tags=["map"])


@router.get("/layers")
def map_layers(
    analysis_id: int | None = None,
    territory: str | None = None,
    year: int | None = None,
    risk_level: str | None = None,
    feature_type: str | None = None,
):
    return get_map_layers(
        analysis_id=analysis_id,
        territory=territory,
        year=year,
        risk_level=risk_level,
        feature_type=feature_type,
    )
