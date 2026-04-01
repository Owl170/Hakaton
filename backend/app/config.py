from pathlib import Path
import os


class Settings:
    ROOT_DIR = Path(__file__).resolve().parents[2]
    BACKEND_DIR = ROOT_DIR / "backend"
    FRONTEND_DIR = ROOT_DIR / "frontend"
    DATA_DIR = ROOT_DIR / "data"
    DATA_SEED_DIR = DATA_DIR / "seed"
    DATA_UPLOADS_DIR = DATA_DIR / "uploads"
    UPLOADS_SHAPE_DIR = DATA_UPLOADS_DIR / "shapefiles"
    UPLOADS_CSV_DIR = DATA_UPLOADS_DIR / "csv"
    UPLOADS_RASTER_DIR = DATA_UPLOADS_DIR / "rasters"
    MODELS_DIR = ROOT_DIR / "models"
    OUTPUTS_DIR = ROOT_DIR / "outputs"
    OUTPUTS_GEOJSON_DIR = OUTPUTS_DIR / "geojson"
    OUTPUTS_RASTER_DIR = OUTPUTS_DIR / "rasters"
    OUTPUTS_REPORT_DIR = OUTPUTS_DIR / "reports"
    DB_PATH = ROOT_DIR / "frostscan.db"
    MODEL_PATH = MODELS_DIR / "risk_model.joblib"
    EXTERNAL_DATA_DIR = Path(os.getenv("FROSTSCAN_DATA_DIR", "D:/data"))


settings = Settings()

for path in [
    settings.DATA_SEED_DIR,
    settings.UPLOADS_SHAPE_DIR,
    settings.UPLOADS_CSV_DIR,
    settings.UPLOADS_RASTER_DIR,
    settings.MODELS_DIR,
    settings.OUTPUTS_GEOJSON_DIR,
    settings.OUTPUTS_RASTER_DIR,
    settings.OUTPUTS_REPORT_DIR,
]:
    path.mkdir(parents=True, exist_ok=True)
