import re
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
from fastapi import UploadFile
from shapely import wkt
from shapely.geometry import Polygon

from backend.app.config import settings
from backend.app.database import get_setting, set_setting

TERRITORY_ALIASES = {
    "Amga": ("amga", "anga", "\u0430\u043c\u0433\u0430", "\u0430\u043d\u0433\u0430"),
    "Yunkor": ("yunkor", "\u044e\u043d\u043a\u043e\u0440"),
}


def _fallback_territory_polygon(territory: str) -> Polygon | None:
    if territory == "Amga":
        return Polygon(
            [
                (131.781939, 60.985284),
                (132.217637, 60.985284),
                (132.217637, 60.841198),
                (131.781939, 60.841198),
            ]
        )
    if territory == "Yunkor":
        return Polygon(
            [
                (120.068611, 60.520207),
                (120.452440, 60.520207),
                (120.452440, 60.287948),
                (120.068611, 60.287948),
            ]
        )
    return None


def _path_is_valid(path_value: str | None) -> bool:
    if not path_value:
        return False
    try:
        return Path(path_value).exists()
    except Exception:
        return False


def _extract_year(path: Path) -> int | None:
    name = path.name
    m = re.search(r"KANOPUS_(\d{4})\d{4}", name, flags=re.IGNORECASE)
    if m:
        year = int(m.group(1))
        if 2010 <= year <= 2035:
            return year

    m = re.search(r"KANOPUS_(20\d{2})\b", name, flags=re.IGNORECASE)
    if m:
        year = int(m.group(1))
        if 2010 <= year <= 2035:
            return year

    name_years = [int(y) for y in re.findall(r"(20\d{2})", name) if 2010 <= int(y) <= 2035]
    if name_years:
        return name_years[0]

    full_years = [int(y) for y in re.findall(r"(20\d{2})", str(path)) if 2010 <= int(y) <= 2035]
    if full_years:
        return min(full_years)
    return None


def _candidate_score(path: Path) -> int:
    name = path.name.upper()
    score = 0
    if ".L2.MS" in name:
        score += 60
    if ".L2.PMS" in name:
        score += 55
    if ".L1.MS" in name:
        score += 35
    if "KANOPUS" in name:
        score += 10
    if "AMGA" in name or "YUNKOR" in name:
        score += 6
    if ".PAN" in name and ".PMS" not in name:
        score -= 100
    if "MS" not in name and "PMS" not in name:
        if re.search(r"KANOPUS_20\d{2}\.TIF", name):
            score += 8
        else:
            score -= 100
    return score


def normalize_territory_name(value: str | None) -> str:
    raw = (value or "").strip()
    low = raw.lower()
    for canonical, aliases in TERRITORY_ALIASES.items():
        if low == canonical.lower():
            return canonical
        if any(alias in low for alias in aliases):
            return canonical
    return raw or "unknown"


def _match_territory_from_path(path: Path) -> str | None:
    text = str(path).lower()
    for canonical, aliases in TERRITORY_ALIASES.items():
        if any(alias in text for alias in aliases):
            return canonical
    return None


def _is_usable_raster(path: Path) -> bool:
    try:
        with rasterio.open(path) as src:
            if src.count < 4:
                return False
            if src.crs is None:
                return False
            if src.transform.is_identity:
                return False
        return True
    except Exception:
        return False


def _iter_rasters(directory: Path) -> list[Path]:
    if not directory.exists():
        return []
    return [p for p in directory.rglob("*.tif*") if p.is_file()]


def _find_external_territory_shapefiles() -> dict[str, list[Path]]:
    result: dict[str, list[Path]] = {"Amga": [], "Yunkor": []}
    if not settings.EXTERNAL_DATA_DIR.exists():
        return result

    for shp in settings.EXTERNAL_DATA_DIR.rglob("*.shp"):
        territory = _match_territory_from_path(shp)
        if territory in result:
            result[territory].append(shp)
    return result


def _generate_external_boundaries() -> Path | None:
    shp_map = _find_external_territory_shapefiles()
    if not shp_map["Amga"] and not shp_map["Yunkor"]:
        return None

    features: list[dict] = []
    for territory in ["Amga", "Yunkor"]:
        geoms = []
        for shp in shp_map[territory]:
            try:
                gdf = gpd.read_file(shp)
                if gdf.empty:
                    continue
                if gdf.crs is None:
                    gdf = gdf.set_crs(4326)
                else:
                    gdf = gdf.to_crs(4326)
                geoms.extend([geom for geom in gdf.geometry if geom is not None and not geom.is_empty])
            except Exception:
                continue

        if geoms:
            union_geom = gpd.GeoSeries(geoms, crs=4326).union_all()
        else:
            union_geom = None

        if union_geom is None or union_geom.is_empty:
            union_geom = _fallback_territory_polygon(territory)

        if union_geom is None or union_geom.is_empty:
            continue
        features.append({"territory": territory, "geometry": union_geom.buffer(0)})

    if not features:
        return None

    boundaries_gdf = gpd.GeoDataFrame(features, crs="EPSG:4326")
    out_path = settings.DATA_UPLOADS_DIR / "external_boundaries.geojson"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    boundaries_gdf.to_file(out_path, driver="GeoJSON")
    return out_path


def _generate_external_parcels_csv(boundaries_path: Path | None) -> Path | None:
    if boundaries_path is None or not boundaries_path.exists():
        return None
    boundaries = gpd.read_file(boundaries_path)
    if boundaries.empty:
        return None
    if boundaries.crs is None:
        boundaries = boundaries.set_crs(4326)
    else:
        boundaries = boundaries.to_crs(4326)

    rows: list[dict] = []
    counter = 1
    for _, row in boundaries.iterrows():
        territory = normalize_territory_name(str(row.get("territory", "unknown")))
        geom = row.geometry
        if geom is None or geom.is_empty:
            continue

        minx, miny, maxx, maxy = geom.bounds
        dx = (maxx - minx) / 8.0
        dy = (maxy - miny) / 6.0
        jitter_x = dx * 0.18
        jitter_y = dy * 0.18
        rng = np.random.default_rng(abs(hash(territory)) % (2**32))

        for i in range(8):
            for j in range(6):
                x0 = minx + i * dx
                x1 = minx + (i + 1) * dx
                y0 = miny + j * dy
                y1 = miny + (j + 1) * dy
                cell = Polygon(
                    [
                        (x0 + rng.uniform(-jitter_x, jitter_x), y0 + rng.uniform(-jitter_y, jitter_y)),
                        (x1 + rng.uniform(-jitter_x, jitter_x), y0 + rng.uniform(-jitter_y, jitter_y)),
                        (x1 + rng.uniform(-jitter_x, jitter_x), y1 + rng.uniform(-jitter_y, jitter_y)),
                        (x0 + rng.uniform(-jitter_x, jitter_x), y1 + rng.uniform(-jitter_y, jitter_y)),
                    ]
                ).buffer(0)
                if cell.is_empty:
                    continue
                clipped = geom.intersection(cell)
                if clipped.is_empty:
                    continue
                clipped = clipped.buffer(0)
                if clipped.is_empty:
                    continue
                rows.append(
                    {
                        "parcel_id": f"EXT-{counter:05d}",
                        "territory": territory,
                        "cadastral_number": f"14:00:{counter:07d}",
                        "owner": "dataset",
                        "crop": "",
                        "geometry_wkt": clipped.wkt,
                    }
                )
                counter += 1

    if not rows:
        return None

    out_df = pd.DataFrame(rows)
    gdf = gpd.GeoDataFrame(
        out_df,
        geometry=gpd.GeoSeries.from_wkt(out_df["geometry_wkt"], crs=4326),
        crs=4326,
    )
    out_df["area_ha"] = (gdf.to_crs(3857).area / 10000.0).round(4)
    out_df = out_df[out_df["area_ha"] > 0.2].copy()

    out_path = settings.UPLOADS_CSV_DIR / "external_parcels.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(out_path, index=False)
    return out_path


def ensure_default_settings() -> None:
    default_boundaries = settings.DATA_SEED_DIR / "boundaries" / "territories.geojson"
    default_parcels = settings.DATA_SEED_DIR / "parcels.csv"
    default_rasters_dir = settings.DATA_SEED_DIR / "rasters"

    active_boundaries = get_setting("active_boundaries_path")
    active_parcels = get_setting("active_parcels_csv")
    active_raster_dir = get_setting("active_raster_dir")

    if settings.EXTERNAL_DATA_DIR.exists():
        ext_boundaries = _generate_external_boundaries()
        if ext_boundaries and ext_boundaries.exists():
            set_setting("active_boundaries_path", str(ext_boundaries))
            active_boundaries = str(ext_boundaries)

        boundary_path = Path(active_boundaries) if _path_is_valid(active_boundaries) else None
        ext_parcels = _generate_external_parcels_csv(boundary_path)
        if ext_parcels and ext_parcels.exists():
            set_setting("active_parcels_csv", str(ext_parcels))
            active_parcels = str(ext_parcels)

        set_setting("active_raster_dir", str(settings.EXTERNAL_DATA_DIR))
        active_raster_dir = str(settings.EXTERNAL_DATA_DIR)

    if not _path_is_valid(active_boundaries) and default_boundaries.exists():
        set_setting("active_boundaries_path", str(default_boundaries))
    if not _path_is_valid(active_parcels) and default_parcels.exists():
        set_setting("active_parcels_csv", str(default_parcels))
    if not _path_is_valid(active_raster_dir) and default_rasters_dir.exists():
        set_setting("active_raster_dir", str(default_rasters_dir))


def handle_shapefile_upload(upload: UploadFile) -> dict:
    raise ValueError("Custom uploads are disabled. FrostScan uses only dataset from D:/data")


def handle_csv_upload(upload: UploadFile) -> dict:
    raise ValueError("Custom uploads are disabled. FrostScan uses only dataset from D:/data")


def handle_raster_upload(upload: UploadFile) -> dict:
    raise ValueError("Custom uploads are disabled. FrostScan uses only dataset from D:/data")


def load_boundaries_gdf() -> gpd.GeoDataFrame:
    path = get_setting("active_boundaries_path") or str(settings.DATA_SEED_DIR / "boundaries" / "territories.geojson")
    gdf = gpd.read_file(path)
    if gdf.crs is None:
        gdf = gdf.set_crs(4326)
    else:
        gdf = gdf.to_crs(4326)

    if "territory" not in gdf.columns:
        gdf["territory"] = [f"territory_{i+1}" for i in range(len(gdf))]
    gdf["territory"] = gdf["territory"].astype(str).map(normalize_territory_name)
    gdf = gdf[gdf.geometry.notnull()].copy()
    gdf = gdf[~gdf.geometry.is_empty].copy()
    return gdf[["territory", "geometry"]].copy()


def _assign_territory_by_geometry(gdf: gpd.GeoDataFrame, boundaries: gpd.GeoDataFrame) -> list[str]:
    boundary_items = [(str(row["territory"]), row.geometry) for _, row in boundaries.iterrows()]
    assigned: list[str] = []
    for geom in gdf.geometry:
        territory = "unknown"
        if geom is not None and not geom.is_empty:
            rep = geom.representative_point()
            for name, boundary in boundary_items:
                if boundary is not None and boundary.contains(rep):
                    territory = name
                    break
        assigned.append(territory)
    return assigned


def load_parcels_gdf() -> gpd.GeoDataFrame:
    path = Path(get_setting("active_parcels_csv") or str(settings.DATA_SEED_DIR / "parcels.csv"))
    df = pd.read_csv(path)
    columns_map = {str(c).strip().upper(): c for c in df.columns}
    geometry_col = columns_map.get("GEOMETRY_WKT") or columns_map.get("OBJ_WKT")
    if geometry_col is None:
        raise ValueError("CSV must have geometry_wkt column")

    geoms = []
    for value in df[geometry_col].astype(str):
        try:
            geoms.append(wkt.loads(value))
        except Exception:
            geoms.append(None)

    gdf = gpd.GeoDataFrame(df, geometry=geoms, crs="EPSG:4326")
    gdf = gdf[gdf.geometry.notnull()].copy()
    gdf = gdf[~gdf.geometry.is_empty].copy()

    if "parcel_id" not in gdf.columns:
        gdf["parcel_id"] = [f"P-{i+1:05d}" for i in range(len(gdf))]
    if "cadastral_number" not in gdf.columns:
        gdf["cadastral_number"] = gdf["parcel_id"].astype(str)
    if "territory" not in gdf.columns:
        boundaries = load_boundaries_gdf()
        gdf["territory"] = _assign_territory_by_geometry(gdf, boundaries)
    gdf["territory"] = gdf["territory"].astype(str).map(normalize_territory_name)
    if "area_ha" not in gdf.columns:
        if "AREA" in gdf.columns:
            area_raw = pd.to_numeric(gdf["AREA"], errors="coerce")
            gdf["area_ha"] = np.where(area_raw > 5000, area_raw / 10000.0, area_raw)
            gdf["area_ha"] = gdf["area_ha"].fillna(gdf.to_crs(3857).area / 10000.0)
        else:
            gdf["area_ha"] = gdf.to_crs(3857).area / 10000.0
    return gdf


def _build_raster_candidates() -> dict[int, list[tuple[int, int, str]]]:
    candidates: dict[int, list[tuple[int, int, str]]] = {}
    active_dir_raw = get_setting("active_raster_dir")
    sources: list[tuple[int, Path]] = []
    if active_dir_raw and Path(active_dir_raw).exists():
        sources.append((2, Path(active_dir_raw)))
    elif settings.EXTERNAL_DATA_DIR.exists():
        sources.append((2, settings.EXTERNAL_DATA_DIR))
    else:
        sources.append((1, settings.DATA_SEED_DIR / "rasters"))

    for priority, directory in sources:
        for raster_path in _iter_rasters(directory):
            year = _extract_year(raster_path)
            if year is None:
                continue
            if not _is_usable_raster(raster_path):
                continue
            score = _candidate_score(raster_path)
            if score < 0:
                continue
            item = (priority, score, str(raster_path.resolve()))
            candidates.setdefault(year, []).append(item)

    if not candidates:
        for raster_path in _iter_rasters(settings.DATA_SEED_DIR / "rasters"):
            year = _extract_year(raster_path)
            if year is None:
                continue
            if not _is_usable_raster(raster_path):
                continue
            candidates.setdefault(year, []).append((1, 10, str(raster_path.resolve())))

    for year in list(candidates.keys()):
        unique: dict[str, tuple[int, int, str]] = {}
        for item in candidates[year]:
            unique[item[2]] = item
        candidates[year] = sorted(unique.values(), key=lambda x: (x[0], x[1], x[2]), reverse=True)
    return candidates


def collect_raster_candidates_by_year(max_candidates: int = 24) -> dict[int, list[str]]:
    candidates = _build_raster_candidates()
    result: dict[int, list[str]] = {}
    for year, rows in candidates.items():
        result[year] = [path for _, _, path in rows[:max_candidates]]
    return result


def collect_rasters_by_year() -> dict[int, str]:
    candidates = _build_raster_candidates()
    selected: dict[int, str] = {}
    for year, rows in sorted(candidates.items(), key=lambda item: item[0]):
        selected[year] = rows[0][2]
    return selected
