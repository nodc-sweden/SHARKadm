import polars as pl

from sharkadm import adm_logger
from sharkadm.data import PolarsDataHolder
from sharkadm.data_filter.base import PolarsDataFilter


class PolarsDataFilterInside1nm(PolarsDataFilter):
    @property
    def description(self) -> str:
        return "Inside 1 nm"

    def _get_filter_mask(self, data_holder: PolarsDataHolder) -> pl.Series | None:
        col = "location_sea_area_code"
        if col not in data_holder.data:
            adm_logger.log_workflow(
                f"Could not filter data. Missing column {col}", level=adm_logger.ERROR
            )
            raise
        col = "location_sea_area_name"
        if col not in data_holder.data:
            adm_logger.log_workflow(
                f"Could not filter data. Missing column {col}", level=adm_logger.ERROR
            )
            raise

        # boolean_wb = data_holder.data['location_wb'] != 'N'
        boolean_code = data_holder.data["location_sea_area_code"] != ""
        boolean_name = data_holder.data["location_sea_area_name"] != ""
        return boolean_code & boolean_name
