import polars as pl

from sharkadm.data import PolarsDataHolder
from sharkadm.data_filter.base import PolarsDataFilter


class PolarsDataFilterQflag(PolarsDataFilter):
    def __init__(self, *qflags: str):
        if not all([isinstance(item, str) for item in qflags]):
            raise Exception(f"Invalid datatypes {qflags}")
        super().__init__(qflags=qflags)
        self._qflags = qflags

    def _get_filter_mask(self, data_holder: PolarsDataHolder) -> pl.Series:
        return data_holder.data.select(
            pl.col("quality_flag").is_in(self._qflags)
        ).to_series()
