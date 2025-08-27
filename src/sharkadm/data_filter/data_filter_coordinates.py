import polars as pl

from sharkadm.data import PolarsDataHolder
from sharkadm.data_filter.base import PolarsDataFilter


class PolarsDataFilterBoundingBox(PolarsDataFilter):
    lat_col = "sample_latitude_dd"
    lon_col = "sample_longitude_dd"

    def __init__(
        self,
        lat_min: float | str | None = None,
        lat_max: float | str | None = None,
        lon_min: float | str | None = None,
        lon_max: float | str | None = None,
    ):
        super().__init__(
            lat_min=lat_min,
            lat_max=lat_max,
            lon_min=lon_min,
            lon_max=lon_max,
        )
        self._lat_min = lat_min
        self._lat_max = lat_max
        self._lon_min = lon_min
        self._lon_max = lon_max

    def _get_filter_mask(
        self,
        data_holder: PolarsDataHolder,
    ) -> pl.Series:
        boolean = pl.Series(
            [True for _ in range(len(data_holder.data))], dtype=pl.Boolean
        )
        if self._lat_min is not None:
            boolean = boolean & (data_holder.data[self.lat_col] >= self._lat_min)
        if self._lat_max is not None:
            boolean = boolean & (data_holder.data[self.lat_col] <= self._lat_max)
        if self._lon_min is not None:
            boolean = boolean & (data_holder.data[self.lon_col] >= self._lon_min)
        if self._lon_max is not None:
            boolean = boolean & (data_holder.data[self.lon_col] <= self._lon_max)
        return boolean
