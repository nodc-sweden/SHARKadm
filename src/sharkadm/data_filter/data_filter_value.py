import polars as pl

from sharkadm import adm_logger
from sharkadm.data import PolarsDataHolder
from sharkadm.data_filter.base import PolarsDataFilter


class PolarsDataFilterValueLessThan(PolarsDataFilter):
    def __init__(self, column: str, value: float):
        super().__init__(column=column, value=value)
        self._column = column
        self._value = value

    def _get_filter_mask(self, data_holder: PolarsDataHolder) -> pl.Series | None:
        if self._column not in data_holder.data:
            adm_logger.log_workflow(
                f"Could not filter data. Missing column {self._column}", level=adm_logger.ERROR
            )
            raise
        return data_holder.data[self._column].cast(float) < self._value
