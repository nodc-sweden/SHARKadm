import polars as pl

from sharkadm import adm_logger
from sharkadm.data import PolarsDataHolder
from sharkadm.data_filter.base import PolarsDataFilter
from sharkadm.utils import approved_data


class PolarsDataFilterApprovedData(PolarsDataFilter):
    def _get_filter_mask(self, data_holder: PolarsDataHolder) -> pl.Series:
        if "approved_key" not in data_holder.data:
            adm_logger.log_workflow(
                "Could not create data filter mask. Missing column approved_key",
                level=adm_logger.ERROR,
            )
            return pl.Series()
        ap_data = approved_data.ApprovedData()
        mapper = ap_data.mapper
        return data_holder.data.select(
            pl.col("approved_key").replace_strict(mapper, default=False)
        )["approved_key"]
