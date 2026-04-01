from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse

from backend.app.services.analysis_service import export_geojson_path

router = APIRouter(prefix="/export", tags=["export"])


@router.get("/geojson")
def export_geojson(analysis_id: int | None = None):
    try:
        path = export_geojson_path(analysis_id)
        return FileResponse(path=str(path), filename=path.name, media_type="application/geo+json")
    except Exception as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
