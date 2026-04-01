from typing import Any

from pydantic import BaseModel, Field


class UploadResponse(BaseModel):
    upload_id: int
    kind: str
    filename: str
    stored_path: str


class AnalysisRunRequest(BaseModel):
    territories: list[str] | None = Field(default=None)
    years: list[int] | None = Field(default=None)
    force_retrain: bool = Field(default=False)


class AnalysisRunResponse(BaseModel):
    analysis_id: int
    status: str
    summary: dict[str, Any]


class HealthResponse(BaseModel):
    status: str
    db: str
    model_exists: bool
    seed_ready: bool
