import argparse
from pathlib import Path
from typing import Any

import geopandas as gpd
import joblib
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, f1_score, roc_auc_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from backend.app.config import settings
from backend.app.services.seed_service import ensure_seed_data
from ml.features import compute_feature_stack
from ml.geo import mask_from_geometry, read_multiband_raster

FEATURE_COLUMNS = ["ndvi", "ndwi", "moisture", "texture"]


def _norm_unit(arr: np.ndarray) -> np.ndarray:
    return np.clip((arr + 1.0) / 2.0, 0.0, 1.0)


def _collect_training_data() -> tuple[np.ndarray, np.ndarray]:
    boundaries_path = settings.DATA_SEED_DIR / "boundaries" / "territories.geojson"
    rasters_dir = settings.DATA_SEED_DIR / "rasters"

    boundaries = gpd.read_file(boundaries_path).to_crs(4326)
    raster_files = sorted(rasters_dir.glob("*.tif"))
    if not raster_files:
        raise ValueError("No seed rasters found for training")

    x_all: list[np.ndarray] = []
    y_all: list[np.ndarray] = []
    rng = np.random.default_rng(42)

    for raster_path in raster_files:
        profile, bands = read_multiband_raster(raster_path)
        feats = compute_feature_stack(bands)
        feature_stack = np.stack([feats[col] for col in FEATURE_COLUMNS], axis=-1)

        for _, territory in boundaries.iterrows():
            tmask = mask_from_geometry(profile, territory.geometry)
            if not tmask.any():
                continue
            x = feature_stack[tmask]
            ndvi = x[:, 0]
            ndwi = x[:, 1]
            moisture = x[:, 2]
            texture = x[:, 3]

            ndwi_n = _norm_unit(ndwi)
            moisture_n = _norm_unit(moisture)
            texture_n = np.clip(texture / (np.quantile(texture, 0.98) + 1e-6), 0.0, 1.0)
            ndvi_inv = np.clip(1.0 - _norm_unit(ndvi), 0.0, 1.0)

            risk_rule = 0.35 * ndwi_n + 0.30 * moisture_n + 0.20 * texture_n + 0.15 * ndvi_inv
            y = (risk_rule > 0.55).astype(np.uint8)

            if x.shape[0] > 15000:
                idx = rng.choice(x.shape[0], 15000, replace=False)
                x = x[idx]
                y = y[idx]

            x_all.append(x)
            y_all.append(y)

    x_data = np.vstack(x_all)
    y_data = np.concatenate(y_all)
    return x_data, y_data


def train_model(force: bool = False) -> dict[str, Any]:
    ensure_seed_data()
    model_path: Path = settings.MODEL_PATH
    if model_path.exists() and not force:
        bundle = joblib.load(model_path)
        return {"model_path": str(model_path), "metrics": bundle.get("metrics", {}), "cached": True}

    x_data, y_data = _collect_training_data()
    x_train, x_test, y_train, y_test = train_test_split(
        x_data,
        y_data,
        test_size=0.2,
        random_state=42,
        stratify=y_data,
    )

    model = Pipeline(
        steps=[
            ("scaler", StandardScaler()),
            (
                "rf",
                RandomForestClassifier(
                    n_estimators=220,
                    max_depth=18,
                    min_samples_leaf=4,
                    class_weight="balanced_subsample",
                    random_state=42,
                    n_jobs=-1,
                ),
            ),
        ]
    )
    model.fit(x_train, y_train)

    prob = model.predict_proba(x_test)[:, 1]
    pred = (prob >= 0.5).astype(np.uint8)
    metrics = {
        "auc": float(roc_auc_score(y_test, prob)),
        "f1": float(f1_score(y_test, pred)),
        "accuracy": float(accuracy_score(y_test, pred)),
        "train_samples": int(len(x_train)),
        "test_samples": int(len(x_test)),
    }

    bundle = {
        "model": model,
        "features": FEATURE_COLUMNS,
        "metrics": metrics,
    }
    model_path.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(bundle, model_path)
    return {"model_path": str(model_path), "metrics": metrics, "cached": False}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--force", action="store_true", help="retrain even if model exists")
    args = parser.parse_args()
    result = train_model(force=args.force)
    print(result)


if __name__ == "__main__":
    main()
