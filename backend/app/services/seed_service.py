from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
from rasterio.transform import from_origin
from shapely.geometry import Polygon

from backend.app.config import settings


def _seed_territory_polygons() -> dict[str, Polygon]:
    # Approximate envelopes around the real settlements.
    amga = Polygon(
        [
            (131.781939, 60.985284),
            (132.217637, 60.985284),
            (132.217637, 60.841198),
            (131.781939, 60.841198),
        ]
    )
    yunkor = Polygon(
        [
            (120.068611, 60.520207),
            (120.452440, 60.520207),
            (120.452440, 60.287948),
            (120.068611, 60.287948),
        ]
    )
    return {"Amga": amga, "Yunkor": yunkor}


def _is_legacy_seed_layout() -> bool:
    path = _seed_boundaries_path()
    if not path.exists():
        return False
    try:
        gdf = gpd.read_file(path)
        if gdf.empty:
            return True
        if gdf.crs is None:
            gdf = gdf.set_crs(4326)
        else:
            gdf = gdf.to_crs(4326)
        bounds = tuple(float(v) for v in gdf.total_bounds.tolist())
        minx, miny, maxx, maxy = bounds
        is_old_extent = (128.5 <= minx <= 129.5) and (130.2 <= maxx <= 130.9) and (61.5 <= miny <= 61.9) and (62.2 <= maxy <= 62.6)
        return is_old_extent
    except Exception:
        return True


def _seed_boundaries_path() -> Path:
    return settings.DATA_SEED_DIR / "boundaries" / "territories.geojson"


def _seed_parcels_path() -> Path:
    return settings.DATA_SEED_DIR / "parcels.csv"


def _seed_rasters_dir() -> Path:
    return settings.DATA_SEED_DIR / "rasters"


def is_seed_ready() -> bool:
    boundaries_ok = _seed_boundaries_path().exists()
    parcels_ok = _seed_parcels_path().exists()
    rasters_ok = _seed_rasters_dir().exists() and len(list(_seed_rasters_dir().glob("*.tif"))) >= 8
    return boundaries_ok and parcels_ok and rasters_ok


def ensure_seed_data() -> None:
    if is_seed_ready() and not _is_legacy_seed_layout():
        return

    boundaries_dir = settings.DATA_SEED_DIR / "boundaries"
    rasters_dir = _seed_rasters_dir()
    boundaries_dir.mkdir(parents=True, exist_ok=True)
    rasters_dir.mkdir(parents=True, exist_ok=True)

    territory_polygons = _seed_territory_polygons()
    amga = territory_polygons["Amga"]
    yunkor = territory_polygons["Yunkor"]

    boundaries = gpd.GeoDataFrame(
        [{"territory": "Amga", "geometry": amga}, {"territory": "Yunkor", "geometry": yunkor}],
        crs="EPSG:4326",
    )
    boundaries.to_file(_seed_boundaries_path(), driver="GeoJSON")
    boundaries.to_file(boundaries_dir / "territories.shp")

    parcel_rows: list[dict] = []
    for territory, poly in [("Amga", amga), ("Yunkor", yunkor)]:
        minx, miny, maxx, maxy = poly.bounds
        dx = (maxx - minx) / 3.0
        dy = (maxy - miny) / 2.0
        idx = 1
        for i in range(3):
            for j in range(2):
                cell = Polygon(
                    [
                        (minx + i * dx, miny + j * dy),
                        (minx + (i + 1) * dx, miny + j * dy),
                        (minx + (i + 1) * dx, miny + (j + 1) * dy),
                        (minx + i * dx, miny + (j + 1) * dy),
                    ]
                )
                clip = poly.intersection(cell)
                if clip.is_empty:
                    continue
                parcel_rows.append(
                    {
                        "parcel_id": f"{territory[:2].upper()}-{idx:03d}",
                        "territory": territory,
                        "cadastral_number": f"14:{1000 + i * 10 + j}:{2000 + idx}",
                        "owner": f"Farm-{territory[:2].upper()}-{idx}",
                        "crop": ["hay", "potato", "grain", "feed", "barley", "oat"][idx - 1],
                        "geometry_wkt": clip.wkt,
                    }
                )
                idx += 1

    parcels_gdf = gpd.GeoDataFrame(
        parcel_rows,
        geometry=gpd.GeoSeries.from_wkt([row["geometry_wkt"] for row in parcel_rows], crs="EPSG:4326"),
        crs="EPSG:4326",
    )
    parcels_gdf["area_ha"] = parcels_gdf.to_crs(3857).area / 10000.0
    parcels_df = parcels_gdf.drop(columns=["geometry"]).copy()
    parcels_df["area_ha"] = parcels_gdf["area_ha"].round(2)
    parcels_df.to_csv(_seed_parcels_path(), index=False)

    minx, miny, maxx, maxy = boundaries.total_bounds
    pad_x = max((maxx - minx) * 0.08, 0.25)
    pad_y = max((maxy - miny) * 0.12, 0.15)
    raster_bounds = (
        float(minx - pad_x),
        float(miny - pad_y),
        float(maxx + pad_x),
        float(maxy + pad_y),
    )
    for year in range(2018, 2026):
        _generate_synthetic_raster(rasters_dir / f"kanopus_{year}.tif", year, raster_bounds)


def _generate_synthetic_raster(path: Path, year: int, bounds: tuple[float, float, float, float]) -> None:
    width = 256
    height = 256
    minx, miny, maxx, maxy = bounds
    transform = from_origin(minx, maxy, (maxx - minx) / width, (maxy - miny) / height)
    rng = np.random.default_rng(seed=year)

    yy, xx = np.mgrid[0:height, 0:width]
    x = xx / float(width)
    y = yy / float(height)
    t = (year - 2018) / 7.0

    lake_a = np.exp(-(((x - (0.24 + 0.08 * t)) ** 2) + ((y - 0.42) ** 2)) / 0.0065)
    lake_b = np.exp(-(((x - (0.57 + 0.06 * t)) ** 2) + ((y - 0.64) ** 2)) / 0.0080)
    water_signal = np.clip(lake_a + 0.75 * lake_b, 0.0, 1.0)

    wet_zone = np.exp(-(((x - 0.48) ** 2) + ((y - 0.35 + 0.03 * t) ** 2)) / 0.03)
    wet_signal = np.clip(0.2 + 0.6 * wet_zone + 0.25 * water_signal, 0.0, 1.0)

    texture_seed = rng.normal(0.0, 0.06, size=(height, width))
    texture_wave = 0.03 * np.sin(18 * x) * np.cos(14 * y)
    noise = texture_seed + texture_wave

    nir = np.clip(0.60 - 0.48 * water_signal - 0.23 * wet_signal + noise, 0.02, 0.95)
    red = np.clip(0.29 + 0.24 * water_signal + 0.09 * wet_signal + rng.normal(0.0, 0.02, size=(height, width)), 0.02, 0.95)
    green = np.clip(0.27 + 0.34 * water_signal + 0.17 * wet_signal + rng.normal(0.0, 0.02, size=(height, width)), 0.02, 0.95)
    blue = np.clip(0.21 + 0.30 * water_signal + 0.12 * wet_signal + rng.normal(0.0, 0.02, size=(height, width)), 0.02, 0.95)

    stack = np.stack([blue, green, red, nir]).astype(np.float32)
    stack_uint16 = np.clip(stack * 10000, 0, 10000).astype(np.uint16)

    profile = {
        "driver": "GTiff",
        "height": height,
        "width": width,
        "count": 4,
        "dtype": "uint16",
        "transform": transform,
        "crs": "EPSG:4326",
        "compress": "deflate",
    }
    with rasterio.open(path, "w", **profile) as dst:
        dst.write(stack_uint16)
