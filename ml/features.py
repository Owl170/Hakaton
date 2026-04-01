import numpy as np
from scipy.ndimage import uniform_filter


def _safe_div(a: np.ndarray, b: np.ndarray, eps: float = 1e-6) -> np.ndarray:
    return a / (b + eps)


def local_std(arr: np.ndarray, size: int = 5) -> np.ndarray:
    mean = uniform_filter(arr, size=size, mode="nearest")
    mean_sq = uniform_filter(arr * arr, size=size, mode="nearest")
    var = np.maximum(mean_sq - mean * mean, 0.0)
    return np.sqrt(var)


def compute_feature_stack(bands: dict[str, np.ndarray]) -> dict[str, np.ndarray]:
    blue = bands["blue"]
    green = bands["green"]
    red = bands["red"]
    nir = bands["nir"]

    ndvi = _safe_div(nir - red, nir + red)
    ndwi = _safe_div(green - nir, green + nir)
    moisture = _safe_div((blue + green) - red, blue + green + red)
    texture = local_std(ndvi, size=5)

    water_mask = ndwi > 0.12
    overwet_mask = (moisture > 0.08) & (ndvi < 0.30)
    heave_mask = (texture > np.quantile(texture, 0.82)) & (ndvi < 0.25)

    return {
        "ndvi": ndvi.astype(np.float32),
        "ndwi": ndwi.astype(np.float32),
        "moisture": moisture.astype(np.float32),
        "texture": texture.astype(np.float32),
        "water_mask": water_mask,
        "overwet_mask": overwet_mask,
        "heave_mask": heave_mask,
    }
