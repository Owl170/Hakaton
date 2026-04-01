import argparse
import json
from pathlib import Path
from typing import Any

import geopandas as gpd
import joblib
import numpy as np
import rasterio
from shapely.geometry import mapping

from backend.app.config import settings
from backend.app.services.seed_service import ensure_seed_data
from backend.app.services.storage_service import (
    collect_raster_candidates_by_year,
    collect_rasters_by_year,
    load_boundaries_gdf,
    load_parcels_gdf,
)
from ml.features import compute_feature_stack
from ml.geo import mask_from_geometry, read_multiband_raster, write_risk_raster
from ml.train import train_model


def _risk_level(score: float) -> str:
    if score >= 0.55:
        return "critical"
    if score >= 0.40:
        return "high"
    if score >= 0.23:
        return "moderate"
    return "low"


def _dominant_feature(water: float, wet: float, heave: float) -> str:
    values = {
        "water_expansion": water,
        "overwetting": wet,
        "heave_mounds": heave,
        "surface_texture_change": max(0.0, 1.0 - (water + wet + heave)),
    }
    return max(values.items(), key=lambda item: item[1])[0]


def _ensure_model(model_path: Path):
    if not model_path.exists():
        train_model(force=False)
    bundle = joblib.load(model_path)
    return bundle["model"]


def _candidate_overlap_score(raster_path: str, geometry) -> float:
    try:
        with rasterio.open(raster_path) as src:
            if src.crs is None or src.transform.is_identity:
                return -1.0
            tmask = mask_from_geometry(src.profile, geometry)
            if not tmask.any():
                return 0.0
            return float(tmask.mean())
    except Exception:
        return -1.0


def _select_raster_for_territory(candidates: list[str], geometry) -> str | None:
    best_path = None
    best_score = -1.0
    for path in candidates:
        score = _candidate_overlap_score(path, geometry)
        if score > best_score:
            best_score = score
            best_path = path
    if best_score <= 0.0:
        return None
    return best_path


def _build_territory_year_rasters(
    boundaries: gpd.GeoDataFrame,
    raster_candidates_by_year: dict[int, list[str]] | None,
    rasters_by_year: dict[int, str],
) -> dict[str, dict[int, str]]:
    all_years = sorted(set((raster_candidates_by_year or {}).keys()) | set(rasters_by_year.keys()))
    mapping: dict[str, dict[int, str]] = {}
    for _, row in boundaries.iterrows():
        territory = str(row["territory"])
        geom = row.geometry
        per_year: dict[int, str] = {}
        for year in all_years:
            candidates = list((raster_candidates_by_year or {}).get(year, []))
            fallback = rasters_by_year.get(year)
            if fallback and fallback not in candidates:
                candidates.append(fallback)
            if not candidates:
                continue
            selected = _select_raster_for_territory(candidates, geom)
            if selected:
                per_year[year] = selected
        mapping[territory] = per_year
    return mapping


def _nearest_year_raster(target_year: int, per_year: dict[int, str]) -> tuple[str | None, int | None]:
    if not per_year:
        return None, None
    nearest_year = min(per_year.keys(), key=lambda y: (abs(y - target_year), y))
    return per_year.get(nearest_year), nearest_year


def run_prediction_pipeline(
    *,
    boundaries_gdf: gpd.GeoDataFrame,
    parcels_gdf: gpd.GeoDataFrame,
    rasters_by_year: dict[int, str],
    raster_candidates_by_year: dict[int, list[str]] | None,
    territories: list[str],
    years: list[int],
    model_path: Path,
    output_raster_dir: Path,
) -> tuple[gpd.GeoDataFrame, dict[str, Any]]:
    model = _ensure_model(model_path)
    boundaries = boundaries_gdf[boundaries_gdf["territory"].isin(territories)].copy()
    parcels = parcels_gdf[parcels_gdf["territory"].isin(territories)].copy()
    territory_year_rasters = _build_territory_year_rasters(boundaries, raster_candidates_by_year, rasters_by_year)

    boundaries_area = boundaries.to_crs(3857)
    territory_area_ha = {
        row["territory"]: float(row.geometry.area / 10000.0)
        for _, row in boundaries_area.iterrows()
    }

    water_baseline: dict[str, float] = {}
    results: list[dict[str, Any]] = []
    year_stats: dict[int, dict[str, float | int]] = {}

    output_raster_dir.mkdir(parents=True, exist_ok=True)
    rng = np.random.default_rng(42)

    for year in sorted(years):
        year_candidates = (raster_candidates_by_year or {}).get(year, [])
        if not year_candidates:
            fallback = rasters_by_year.get(year)
            if fallback:
                year_candidates = [fallback]

        raster_cache: dict[str, dict[str, Any]] = {}
        used_cache_keys: list[str] = []

        year_stats.setdefault(year, {"problem_area_ha": 0.0, "objects": 0, "risk_sum": 0.0})

        for _, territory_row in boundaries.iterrows():
            territory_name = str(territory_row["territory"])
            territory_raster = _select_raster_for_territory(year_candidates, territory_row.geometry)
            source_year = year
            used_nearest_year = False
            if territory_raster is None:
                territory_raster, source_year = _nearest_year_raster(
                    year,
                    territory_year_rasters.get(territory_name, {}),
                )
                used_nearest_year = territory_raster is not None and source_year is not None and source_year != year
            if territory_raster is None:
                continue

            if territory_raster not in raster_cache:
                profile, bands = read_multiband_raster(territory_raster)
                feats = compute_feature_stack(bands)
                ndwi_norm = ((feats["ndwi"] + 1.0) / 2.0).astype(np.float32, copy=False)
                np.clip(ndwi_norm, 0.0, 1.0, out=ndwi_norm)

                texture_norm = (feats["texture"] / (np.quantile(feats["texture"], 0.98) + 1e-6)).astype(
                    np.float32, copy=False
                )
                np.clip(texture_norm, 0.0, 1.0, out=texture_norm)

                rule_risk_map = np.empty_like(ndwi_norm, dtype=np.float32)
                rule_risk_map[:] = ndwi_norm
                rule_risk_map *= 0.45
                rule_risk_map += 0.35 * feats["overwet_mask"].astype(np.float32, copy=False)
                rule_risk_map += 0.20 * texture_norm
                np.clip(rule_risk_map, 0.0, 1.0, out=rule_risk_map)

                ndvi_flat = feats["ndvi"].reshape(-1)
                ndwi_flat = feats["ndwi"].reshape(-1)
                moisture_flat = feats["moisture"].reshape(-1)
                texture_flat = feats["texture"].reshape(-1)
                total_pixels = ndvi_flat.shape[0]
                sample_size = min(total_pixels, 60000)
                if sample_size < total_pixels:
                    idx = rng.choice(total_pixels, sample_size, replace=False)
                else:
                    idx = np.arange(total_pixels)
                feature_sample = np.column_stack(
                    [
                        ndvi_flat[idx],
                        ndwi_flat[idx],
                        moisture_flat[idx],
                        texture_flat[idx],
                    ]
                ).astype(np.float32, copy=False)
                ml_calibration = float(model.predict_proba(feature_sample)[:, 1].mean())
                raster_cache[territory_raster] = {
                    "profile": profile,
                    "feats": feats,
                    "risk_map": rule_risk_map,
                    "ml_calibration": ml_calibration,
                }
                used_cache_keys.append(territory_raster)

            item = raster_cache[territory_raster]
            profile = item["profile"]
            feats = item["feats"]
            risk_map = item["risk_map"]
            ml_calibration = float(item["ml_calibration"])

            tmask = mask_from_geometry(profile, territory_row.geometry)
            if not tmask.any():
                continue

            water_fraction_territory = float(feats["water_mask"][tmask].mean())
            water_area = water_fraction_territory * territory_area_ha.get(territory_name, 0.0)
            if territory_name not in water_baseline:
                water_baseline[territory_name] = water_area
            water_delta = max(0.0, water_area - water_baseline[territory_name])

            territory_parcels = parcels[parcels["territory"] == territory_name]
            texture_threshold = float(np.quantile(feats["texture"][tmask], 0.82))

            for _, parcel_row in territory_parcels.iterrows():
                pmask = mask_from_geometry(profile, parcel_row.geometry) & tmask
                if not pmask.any():
                    continue

                pvals = risk_map[pmask]
                pvals = pvals[~np.isnan(pvals)]
                if pvals.size == 0:
                    continue

                ml_score = ml_calibration

                water_fraction = float(feats["water_mask"][pmask].mean())
                wet_fraction = float(feats["overwet_mask"][pmask].mean())
                heave_fraction = float(
                    ((feats["texture"][pmask] >= texture_threshold) & (feats["ndvi"][pmask] < 0.25)).mean()
                )

                rule_score = float(pvals.mean())
                rule_fraction = float(
                    np.clip(
                        1.8 * water_fraction + 1.3 * wet_fraction + 1.1 * heave_fraction,
                        0.0,
                        1.0,
                    )
                )
                delta_norm = float(
                    np.clip(
                        water_delta / max(territory_area_ha.get(territory_name, 1.0) * 0.05, 1.0),
                        0.0,
                        1.0,
                    )
                )
                risk_score = float(
                    np.clip(
                        0.20 * ml_score + 0.50 * rule_score + 0.22 * rule_fraction + 0.08 * delta_norm,
                        0.0,
                        1.0,
                    )
                )
                risk_fraction = float(np.clip(0.70 * rule_fraction + 0.30 * rule_score, 0.0, 1.0))
                parcel_area = float(parcel_row.get("area_ha", 0.0))
                degraded_area = parcel_area * risk_fraction
                risk_level = _risk_level(risk_score)
                feature_type = _dominant_feature(water_fraction, wet_fraction, heave_fraction)

                metrics = {
                    "water_fraction": round(water_fraction, 4),
                    "wet_fraction": round(wet_fraction, 4),
                    "heave_fraction": round(heave_fraction, 4),
                    "water_area_territory_ha": round(water_area, 3),
                    "water_area_delta_ha": round(water_delta, 3),
                    "source_year": int(source_year) if source_year is not None else int(year),
                    "imputed_from_nearest_year": bool(used_nearest_year),
                }

                results.append(
                    {
                        "analysis_year": year,
                        "year": year,
                        "territory": territory_name,
                        "parcel_id": str(parcel_row["parcel_id"]),
                        "feature_type": feature_type,
                        "risk_level": risk_level,
                        "risk_score": round(risk_score, 4),
                        "area_ha": round(degraded_area, 3),
                        "metrics": metrics,
                        "geometry": parcel_row.geometry,
                    }
                )

                year_stats[year]["problem_area_ha"] = float(year_stats[year]["problem_area_ha"]) + degraded_area
                year_stats[year]["objects"] = int(year_stats[year]["objects"]) + 1
                year_stats[year]["risk_sum"] = float(year_stats[year]["risk_sum"]) + risk_score

        for idx, cache_key in enumerate(used_cache_keys, start=1):
            profile = raster_cache[cache_key]["profile"]
            risk_map = raster_cache[cache_key]["risk_map"]
            write_risk_raster(output_raster_dir / f"risk_{year}_{idx}.tif", risk_map, profile)

    if results:
        detections_gdf = gpd.GeoDataFrame(results, geometry="geometry", crs="EPSG:4326")
    else:
        detections_gdf = gpd.GeoDataFrame(
            {
                "analysis_year": [],
                "year": [],
                "territory": [],
                "parcel_id": [],
                "feature_type": [],
                "risk_level": [],
                "risk_score": [],
                "area_ha": [],
                "metrics": [],
            },
            geometry=gpd.GeoSeries([], crs="EPSG:4326"),
            crs="EPSG:4326",
        )

    risk_distribution = {"low": 0, "moderate": 0, "high": 0, "critical": 0}
    for row in results:
        risk_distribution[row["risk_level"]] += 1

    yearly_dynamics: list[dict[str, Any]] = []
    total_objects = 0
    total_area = 0.0
    weighted_risk_sum = 0.0
    for year in sorted(year_stats):
        y = year_stats[year]
        count = int(y["objects"])
        mean_risk = float(y["risk_sum"]) / count if count > 0 else 0.0
        yearly_dynamics.append(
            {
                "year": year,
                "problem_area_ha": round(float(y["problem_area_ha"]), 3),
                "objects_count": count,
                "mean_risk_score": round(mean_risk, 4),
            }
        )
        total_objects += count
        total_area += float(y["problem_area_ha"])
        weighted_risk_sum += float(y["risk_sum"])

    summary = {
        "total_problem_area_ha": round(total_area, 3),
        "objects_count": total_objects,
        "mean_risk_score": round((weighted_risk_sum / total_objects) if total_objects else 0.0, 4),
        "risk_distribution": risk_distribution,
        "yearly_dynamics": yearly_dynamics,
    }
    return detections_gdf, summary


def run_predict_cli(years: list[int] | None = None, territories: list[str] | None = None) -> dict[str, Any]:
    ensure_seed_data()
    boundaries = load_boundaries_gdf()
    parcels = load_parcels_gdf()
    rasters_by_year = collect_rasters_by_year()
    raster_candidates_by_year = collect_raster_candidates_by_year()

    selected_years = years or sorted(rasters_by_year.keys())
    selected_territories = territories or sorted(boundaries["territory"].unique().tolist())

    detections_gdf, summary = run_prediction_pipeline(
        boundaries_gdf=boundaries,
        parcels_gdf=parcels,
        rasters_by_year=rasters_by_year,
        raster_candidates_by_year=raster_candidates_by_year,
        territories=selected_territories,
        years=selected_years,
        model_path=settings.MODEL_PATH,
        output_raster_dir=settings.OUTPUTS_RASTER_DIR,
    )

    output_path = settings.OUTPUTS_GEOJSON_DIR / "predict_output.geojson"
    detections_gdf.to_file(output_path, driver="GeoJSON")
    report_path = settings.OUTPUTS_REPORT_DIR / "predict_summary.json"
    report_path.write_text(json.dumps(summary, ensure_ascii=True, indent=2), encoding="utf-8")
    summary["output_geojson"] = str(output_path)
    summary["output_report"] = str(report_path)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--years", nargs="*", type=int, default=None)
    parser.add_argument("--territories", nargs="*", type=str, default=None)
    args = parser.parse_args()
    summary = run_predict_cli(years=args.years, territories=args.territories)
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
