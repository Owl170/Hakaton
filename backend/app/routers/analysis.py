from fastapi import APIRouter, HTTPException

from backend.app.schemas import AnalysisRunRequest, AnalysisRunResponse
from backend.app.services.analysis_service import (
    get_all_analyses,
    get_analysis_detail,
    run_analysis_job,
)

router = APIRouter(prefix="/analysis", tags=["analysis"])


@router.post("/run", response_model=AnalysisRunResponse)
def run_analysis(payload: AnalysisRunRequest) -> AnalysisRunResponse:
    try:
        result = run_analysis_job(
            territories=payload.territories,
            years=payload.years,
            force_retrain=payload.force_retrain,
        )
        return AnalysisRunResponse(**result)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/results")
def analysis_results():
    return {"items": get_all_analyses()}


@router.get("/{analysis_id}")
def analysis_detail(analysis_id: int):
    try:
        return get_analysis_detail(analysis_id)
    except Exception as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
