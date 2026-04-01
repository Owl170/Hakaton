from fastapi import APIRouter, HTTPException

from backend.app.services.analysis_service import get_summary

router = APIRouter(prefix="/stats", tags=["stats"])


@router.get("/summary")
def stats_summary(analysis_id: int | None = None):
    try:
        return get_summary(analysis_id)
    except Exception as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
