from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from backend.app.config import settings
from backend.app.database import get_setting, init_db
from backend.app.routers.analysis import router as analysis_router
from backend.app.routers.export import router as export_router
from backend.app.routers.health import router as health_router
from backend.app.routers.map import router as map_router
from backend.app.routers.stats import router as stats_router
from backend.app.services.analysis_service import mark_stale_running_analyses
from backend.app.services.seed_service import ensure_seed_data
from backend.app.services.storage_service import ensure_default_settings

app = FastAPI(
    title="FrostScan",
    version="1.0.0",
    description="Permafrost degradation monitoring MVP",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/assets", StaticFiles(directory=settings.FRONTEND_DIR / "assets"), name="assets")


@app.on_event("startup")
def startup() -> None:
    init_db()
    mark_stale_running_analyses()
    ensure_seed_data()
    ensure_default_settings()
    required_keys = ["active_boundaries_path", "active_parcels_csv", "active_raster_dir"]
    ready = all(get_setting(key) and Path(get_setting(key)).exists() for key in required_keys)
    if not ready:
        ensure_seed_data()
        ensure_default_settings()


@app.get("/", include_in_schema=False)
def home():
    return FileResponse(settings.FRONTEND_DIR / "index.html")


@app.get("/map", include_in_schema=False)
def map_page():
    return FileResponse(settings.FRONTEND_DIR / "map.html")


app.include_router(health_router)
app.include_router(analysis_router)
app.include_router(map_router)
app.include_router(stats_router)
app.include_router(export_router)
