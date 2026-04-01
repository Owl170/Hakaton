from pathlib import Path

import numpy as np
from pyproj import CRS, Transformer
import rasterio
from rasterio.enums import Resampling
from rasterio.features import geometry_mask
from affine import Affine
from shapely.ops import transform


def read_multiband_raster(path: str | Path, max_side: int = 2048) -> tuple[dict, dict[str, np.ndarray]]:
    with rasterio.open(path) as src:
        profile = src.profile.copy()
        if src.count < 4:
            raise ValueError(f"Raster has {src.count} bands, expected >=4: {path}")

        out_h = int(src.height)
        out_w = int(src.width)
        if max(out_h, out_w) > max_side:
            scale = float(max_side) / float(max(out_h, out_w))
            out_h = max(int(src.height * scale), 64)
            out_w = max(int(src.width * scale), 64)
            arr = src.read(
                indexes=[1, 2, 3, 4],
                out_shape=(4, out_h, out_w),
                resampling=Resampling.bilinear,
            ).astype(np.float32)
            transform = src.transform * Affine.scale(src.width / out_w, src.height / out_h)
            profile.update(height=out_h, width=out_w, transform=transform, count=4)
        else:
            arr = src.read(indexes=[1, 2, 3, 4]).astype(np.float32)
            profile.update(count=4)

    arr /= 10000.0
    bands = {
        "blue": arr[0],
        "green": arr[1],
        "red": arr[2],
        "nir": arr[3],
    }
    return profile, bands


def mask_from_geometry(profile: dict, geometry, geometry_crs: str = "EPSG:4326") -> np.ndarray:
    transform_profile = profile.get("transform")
    if transform_profile is None or transform_profile.is_identity:
        return np.zeros((profile["height"], profile["width"]), dtype=bool)

    raster_crs = profile.get("crs")
    geom = geometry
    if raster_crs is not None:
        src = CRS.from_user_input(geometry_crs)
        dst = CRS.from_user_input(raster_crs)
        if src != dst:
            transformer = Transformer.from_crs(src, dst, always_xy=True)
            geom = transform(transformer.transform, geometry)
    return geometry_mask(
        [geom],
        transform=profile["transform"],
        out_shape=(profile["height"], profile["width"]),
        invert=True,
    )


def write_risk_raster(path: str | Path, risk_map: np.ndarray, profile: dict) -> None:
    out_profile = profile.copy()
    out_profile.update(
        {
            "count": 1,
            "dtype": "float32",
            "compress": "deflate",
            "nodata": -9999.0,
        }
    )
    to_write = np.where(np.isnan(risk_map), -9999.0, risk_map).astype(np.float32)
    with rasterio.open(path, "w", **out_profile) as dst:
        dst.write(to_write, 1)
