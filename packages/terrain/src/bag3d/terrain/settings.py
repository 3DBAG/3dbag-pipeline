"""Shared configuration for separately materializable terrain stages."""

import math
from urllib.parse import urlsplit

from dagster import ConfigurableResource
from pydantic import Field, field_validator


class TerrainSettings(ConfigurableResource):
    bounds: list[float] | None = None
    interpolation_radius_m: float = Field(default=500, gt=0)
    workers: int = Field(default=4, ge=1)
    base_url: str = "http://localhost:8001"
    gdal_bin_dir: str | None = None

    @field_validator("bounds")
    @classmethod
    def validate_bounds(cls, value):
        if value is not None:
            if len(value) != 4:
                raise ValueError("Bounds must contain W,S,E,N")
            w, s, e, n = value
            if not all(math.isfinite(v) for v in value) or not (
                0 <= w < e < 10 and 49 <= s < n < 56
            ):
                raise ValueError("Expected a Netherlands-region W,S,E,N bounding box")
        return value

    @field_validator("interpolation_radius_m")
    @classmethod
    def finite_radius(cls, value):
        if not math.isfinite(value):
            raise ValueError("Interpolation radius must be finite")
        return value

    @field_validator("base_url")
    @classmethod
    def validate_url(cls, value):
        parsed = urlsplit(value)
        if (
            parsed.scheme not in ("http", "https")
            or not parsed.netloc
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError(
                "Expected an absolute HTTP(S) directory URL without query or fragment"
            )
        return value.rstrip("/")

    def processing_signature(self) -> dict:
        return {
            "bounds": self.bounds,
            "interpolation_radius_m": self.interpolation_radius_m,
        }
