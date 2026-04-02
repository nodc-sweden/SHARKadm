import polars as pl

from sharkadm.data import PolarsDataHolder
from sharkadm.data_filter.base import PolarsDataFilter


class PolarsDataFilterParameter(PolarsDataFilter):
    def __init__(self, parameter: str):
        super().__init__(parameter=parameter)
        self._parameter = parameter

    def _get_filter_mask(self, data_holder: PolarsDataHolder) -> pl.Series:
        return data_holder.data.select(pl.col("parameter") == self._parameter).to_series()
