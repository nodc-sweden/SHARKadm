import polars as pl

from sharkadm.data import PolarsDataHolder
from sharkadm.data_filter.base import PolarsDataFilter


class PolarsDataFilterLocation(PolarsDataFilter):
    def __init__(self, locations: list[str]):
        super().__init__(locations=locations)
        self._locations = locations

    def _get_filter_mask(self, data_holder: PolarsDataHolder) -> pl.Series:
        mask = data_holder.data[self._locations[0]]
        for col in self._locations[1:]:
            mask = mask | data_holder.data[col]
        return mask
