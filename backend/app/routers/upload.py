from fastapi import APIRouter, File, HTTPException, UploadFile

from backend.app.schemas import UploadResponse
from backend.app.services.storage_service import (
    handle_csv_upload,
    handle_raster_upload,
    handle_shapefile_upload,
)

router = APIRouter(prefix="/upload", tags=["upload"])


@router.post("/shapefile", response_model=UploadResponse)
def upload_shapefile(file: UploadFile = File(...)) -> UploadResponse:
    try:
        result = handle_shapefile_upload(file)
        return UploadResponse(**result)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/csv", response_model=UploadResponse)
def upload_csv(file: UploadFile = File(...)) -> UploadResponse:
    try:
        result = handle_csv_upload(file)
        return UploadResponse(**result)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/raster", response_model=UploadResponse)
def upload_raster(file: UploadFile = File(...)) -> UploadResponse:
    try:
        result = handle_raster_upload(file)
        return UploadResponse(**result)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
