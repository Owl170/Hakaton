import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import geopandas as gpd
from shapely.geometry import mapping

from backend.app.config import settings
from backend.app.database import (
    get_analysis,
    insert_analysis,
    insert_detections,
    list_analyses,
    list_detections,
    update_analysis,
)
from backend.app.services.storage_service import (
    collect_raster_candidates_by_year,
    collect_rasters_by_year,
    load_boundaries_gdf,
    load_parcels_gdf,
)
from ml.predict import run_prediction_pipeline
from ml.train import train_model


def _choose_default_analysis_id(
    available_territories: list[str] | None = None,
    available_years: list[int] | None = None,
    preferred_territory: str | None = None,
    preferred_year: int | None = None,
) -> int | None:
    completed = [a for a in list_analyses() if str(a.get("status")) == "completed"]
    if not completed:
        return None

    target_territories = set(available_territories or [])
    target_years = {int(y) for y in (available_years or [])}
    preferred_territory_norm = str(preferred_territory) if preferred_territory else None
    preferred_year_norm = int(preferred_year) if preferred_year is not None else None

    def score(item: dict[str, Any]) -> tuple[int, int, int, int, int]:
        item_territories = {str(t) for t in item.get("territories", [])}
        item_years = {int(y) for y in item.get("years", [])}
        has_preferred_territory = int(preferred_territory_norm in item_territories) if preferred_territory_norm else 1
        has_preferred_year = int(preferred_year_norm in item_years) if preferred_year_norm is not None else 1
        territory_cover = len(item_territories & target_territories) if target_territories else len(item_territories)
        year_cover = len(item_years & target_years) if target_years else len(item_years)
        full_territories = int(bool(target_territories) and territory_cover == len(target_territories))
        full_years = int(bool(target_years) and year_cover == len(target_years))
        return (
            has_preferred_territory,
            has_preferred_year,
            full_territories,
            full_years,
            territory_cover + year_cover,
            int(item.get("id", 0)),
        )

    best = max(completed, key=score)
    return int(best["id"])


def _rank_completed_analysis_ids(
    preferred_territory: str | None = None,
    preferred_year: int | None = None,
) -> list[int]:
    completed = [a for a in list_analyses() if str(a.get("status")) == "completed"]
    if not completed:
        return []
    preferred_territory_norm = str(preferred_territory) if preferred_territory else None
    preferred_year_norm = int(preferred_year) if preferred_year is not None else None

    def score(item: dict[str, Any]) -> tuple[int, int, int, int]:
        territories = {str(t) for t in item.get("territories", [])}
        years = {int(y) for y in item.get("years", [])}
        has_territory = int(preferred_territory_norm in territories) if preferred_territory_norm else 1
        has_year = int(preferred_year_norm in years) if preferred_year_norm is not None else 1
        return has_territory, has_year, len(territories) + len(years), int(item.get("id", 0))

    ranked = sorted(completed, key=score, reverse=True)
    return [int(item["id"]) for item in ranked]


def run_analysis_job(
    territories: list[str] | None,
    years: list[int] | None,
    force_retrain: bool = False,
) -> dict[str, Any]:
    boundaries = load_boundaries_gdf()
    parcels = load_parcels_gdf()
    raster_map = collect_rasters_by_year()
    raster_candidates = collect_raster_candidates_by_year()

    selected_territories = territories or sorted(boundaries["territory"].astype(str).unique().tolist())
    available_years = sorted(raster_map.keys())
    selected_years = years or available_years
    selected_years = [year for year in selected_years if year in available_years]
    if not selected_years:
        raise ValueError("No rasters available for requested years")

    run_name = f"analysis_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
    analysis_id = insert_analysis(run_name, selected_territories, selected_years, status="running")

    try:
        if force_retrain:
            train_model(force=True)
        elif not settings.MODEL_PATH.exists():
            train_model(force=False)

        detections_gdf, summary = run_prediction_pipeline(
            boundaries_gdf=boundaries,
            parcels_gdf=parcels,
            rasters_by_year=raster_map,
            raster_candidates_by_year=raster_candidates,
            territories=selected_territories,
            years=selected_years,
            model_path=settings.MODEL_PATH,
            output_raster_dir=settings.OUTPUTS_RASTER_DIR,
        )

        output_geojson = settings.OUTPUTS_GEOJSON_DIR / f"analysis_{analysis_id}.geojson"
        if detections_gdf.empty:
            empty_fc = {"type": "FeatureCollection", "features": []}
            output_geojson.write_text(json.dumps(empty_fc, ensure_ascii=True, indent=2), encoding="utf-8")
        else:
            detections_gdf.to_file(output_geojson, driver="GeoJSON")

        detections_records: list[dict[str, Any]] = []
        for _, row in detections_gdf.iterrows():
            metrics = row.get("metrics", {})
            if not isinstance(metrics, dict):
                metrics = {}
            detections_records.append(
                {
                    "year": int(row["year"]),
                    "territory": str(row["territory"]),
                    "parcel_id": str(row["parcel_id"]),
                    "feature_type": str(row["feature_type"]),
                    "risk_level": str(row["risk_level"]),
                    "risk_score": float(row["risk_score"]),
                    "area_ha": float(row["area_ha"]),
                    "geometry_json": json.dumps(mapping(row.geometry), ensure_ascii=True),
                    "metrics_json": json.dumps(metrics, ensure_ascii=True),
                }
            )
        insert_detections(analysis_id, detections_records)

        summary["analysis_id"] = analysis_id
        summary["years"] = selected_years
        summary["territories"] = selected_territories
        update_analysis(
            analysis_id,
            status="completed",
            result_geojson_path=str(output_geojson),
            summary=summary,
        )
        return {
            "analysis_id": analysis_id,
            "status": "completed",
            "summary": summary,
        }
    except Exception as exc:
        update_analysis(analysis_id, status="failed", summary={"error": str(exc)})
        raise


def mark_stale_running_analyses(max_age_minutes: int = 45) -> int:
    now = datetime.now(timezone.utc)
    updated = 0
    for item in list_analyses():
        if str(item.get("status")) != "running":
            continue
        created_raw = item.get("created_at")
        if not created_raw:
            continue
        try:
            created_at = datetime.fromisoformat(str(created_raw))
        except ValueError:
            continue
        if created_at.tzinfo is None:
            created_at = created_at.replace(tzinfo=timezone.utc)
        age_minutes = (now - created_at.astimezone(timezone.utc)).total_seconds() / 60.0
        if age_minutes >= max_age_minutes:
            update_analysis(
                int(item["id"]),
                status="failed",
                summary={"error": "Run marked as stale after interruption"},
            )
            updated += 1
    return updated


def get_all_analyses() -> list[dict[str, Any]]:
    return list_analyses()


def get_analysis_detail(analysis_id: int) -> dict[str, Any]:
    analysis = get_analysis(analysis_id)
    if analysis is None:
        raise ValueError("Analysis not found")
    detections = list_detections(analysis_id)
    detection_features = []
    for item in detections:
        detection_features.append(
            {
                "id": item["id"],
                "year": item["year"],
                "territory": item["territory"],
                "parcel_id": item["parcel_id"],
                "feature_type": item["feature_type"],
                "risk_level": item["risk_level"],
                "risk_score": item["risk_score"],
                "area_ha": item["area_ha"],
                "geometry": json.loads(item["geometry_json"]),
                "metrics": json.loads(item["metrics_json"]),
            }
        )
    analysis["detections"] = detection_features
    return analysis


def get_map_layers(
    analysis_id: int | None = None,
    territory: str | None = None,
    year: int | None = None,
    risk_level: str | None = None,
    feature_type: str | None = None,
) -> dict[str, Any]:
    auto_select = analysis_id is None
    boundaries = load_boundaries_gdf()
    available_years = sorted(collect_rasters_by_year().keys())
    available_territories = sorted(boundaries["territory"].astype(str).unique().tolist())
    target_id = analysis_id or _choose_default_analysis_id(
        available_territories,
        available_years,
        preferred_territory=territory,
        preferred_year=year,
    )
    boundary_features = [
        {
            "type": "Feature",
            "geometry": mapping(row.geometry),
            "properties": {"territory": row["territory"], "layer": "boundary"},
        }
        for _, row in boundaries.iterrows()
    ]
    if target_id is None:
        return {
            "type": "FeatureCollection",
            "features": [],
            "analysis_id": None,
            "boundaries": {"type": "FeatureCollection", "features": boundary_features},
            "available_years": available_years,
            "available_territories": available_territories,
        }

    detections = list_detections(
        target_id,
        territory=territory,
        year=year,
        risk_level=risk_level,
        feature_type=feature_type,
    )

    if auto_select and not detections:
        for candidate_id in _rank_completed_analysis_ids(preferred_territory=territory, preferred_year=year):
            if candidate_id == target_id:
                continue
            candidate_rows = list_detections(
                candidate_id,
                territory=territory,
                year=year,
                risk_level=risk_level,
                feature_type=feature_type,
            )
            if candidate_rows:
                target_id = candidate_id
                detections = candidate_rows
                break
    features = []
    for item in detections:
        features.append(
            {
                "type": "Feature",
                "geometry": json.loads(item["geometry_json"]),
                "properties": {
                    "id": item["id"],
                    "analysis_id": item["analysis_id"],
                    "year": item["year"],
                    "territory": item["territory"],
                    "parcel_id": item["parcel_id"],
                    "feature_type": item["feature_type"],
                    "risk_level": item["risk_level"],
                    "risk_score": item["risk_score"],
                    "area_ha": item["area_ha"],
                    **json.loads(item["metrics_json"]),
                },
            }
        )

    return {
        "type": "FeatureCollection",
        "analysis_id": target_id,
        "features": features,
        "boundaries": {"type": "FeatureCollection", "features": boundary_features},
        "available_years": available_years,
        "available_territories": available_territories,
    }


def get_summary(analysis_id: int | None = None) -> dict[str, Any]:
    boundaries = load_boundaries_gdf()
    available_years = sorted(collect_rasters_by_year().keys())
    available_territories = sorted(boundaries["territory"].astype(str).unique().tolist())
    target_id = analysis_id or _choose_default_analysis_id(available_territories, available_years)
    if target_id is None:
        return {
            "analysis_id": None,
            "total_problem_area_ha": 0.0,
            "objects_count": 0,
            "mean_risk_score": 0.0,
            "risk_distribution": {"low": 0, "moderate": 0, "high": 0, "critical": 0},
            "yearly_dynamics": [],
        }
    analysis = get_analysis(target_id)
    if analysis is None:
        raise ValueError("Analysis not found")
    summary = analysis.get("summary", {})
    summary["analysis_id"] = target_id
    return summary


def export_geojson_path(analysis_id: int | None = None) -> Path:
    boundaries = load_boundaries_gdf()
    available_years = sorted(collect_rasters_by_year().keys())
    available_territories = sorted(boundaries["territory"].astype(str).unique().tolist())
    target_id = analysis_id or _choose_default_analysis_id(available_territories, available_years)
    if target_id is None:
        raise ValueError("No completed analysis found")
    analysis = get_analysis(target_id)
    if analysis is None or not analysis.get("result_geojson_path"):
        raise ValueError("GeoJSON export is not available")

    path = Path(analysis["result_geojson_path"])
    if not path.exists():
        detections = list_detections(target_id)
        features = []
        for item in detections:
            features.append(
                {
                    "type": "Feature",
                    "geometry": json.loads(item["geometry_json"]),
                    "properties": {
                        "year": item["year"],
                        "territory": item["territory"],
                        "parcel_id": item["parcel_id"],
                        "feature_type": item["feature_type"],
                        "risk_level": item["risk_level"],
                        "risk_score": item["risk_score"],
                        "area_ha": item["area_ha"],
                        **json.loads(item["metrics_json"]),
                    },
                }
            )
        fc = {"type": "FeatureCollection", "features": features}
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(fc, ensure_ascii=True, indent=2), encoding="utf-8")
    return path
