import re

import polars as pl

from sharkadm.data import PolarsDataHolder
from sharkadm.data_filter.base import PolarsDataFilter


def _get_all_values(pattern: str, collection_values: set[str]):
    all_values = []
    for value in set(collection_values):
        if re.match(pattern, value):
            all_values.append(value)
    return all_values


class PolarsDataFilterMatchInColumn(PolarsDataFilter):
    def __init__(self, column: str, pattern: str, ignore_case: bool = False):
        """pattern can be a regex"""
        super().__init__(column=column, pattern=pattern)
        self._column = column
        self._pattern = pattern

    def _get_filter_mask(self, data_holder: PolarsDataHolder) -> pl.Series:
        all_values = _get_all_values(self._pattern, set(data_holder.data[self._column]))
        return data_holder.data.select(pl.col(self._column).is_in(all_values)).to_series()


class PolarsDataFilterMatchInColumn(PolarsDataFilter):
    def __init__(self, column: str, *patterns: str, ignore_case: bool = False):
        """pattern can be a regex"""
        super().__init__(column=column, pattern=patterns)
        self._column = column
        self._patterns = patterns

    def _get_filter_mask(self, data_holder: PolarsDataHolder) -> pl.Series:
        all_values = set()
        for pat in self._patterns:
            all_values.update(_get_all_values(pat, set(data_holder.data[self._column])))
        return data_holder.data.select(pl.col(self._column).is_in(all_values)).to_series()
