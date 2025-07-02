import polars as pl

from sharkadm import adm_logger
from sharkadm.data import PolarsDataHolder
from sharkadm.data_filter.base import PolarsDataFilter


class PolarsDataFilterOnLand(PolarsDataFilter):
    def _get_filter_mask(self, data_holder: PolarsDataHolder) -> pl.Series | None:
        col = "location_wb"
        if col not in data_holder.data:
            adm_logger.log_workflow(
                f"Could not filter data. Missing column {col}", level=adm_logger.ERROR
            )
            raise
        return data_holder.data["location_wb"] == ""
