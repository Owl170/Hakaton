from fastapi import APIRouter

from backend.app.config import settings
from backend.app.schemas import HealthResponse
from backend.app.services.seed_service import is_seed_ready

router = APIRouter()


@router.get("/health", response_model=HealthResponse)
def health() -> HealthResponse:
    return HealthResponse(
        status="ok",
        db="ok" if settings.DB_PATH.exists() else "missing",
        model_exists=settings.MODEL_PATH.exists(),
        seed_ready=is_seed_ready(),
    )
