from abc import ABC, abstractmethod

import pandas as pd
import polars as pl

from sharkadm.data import PandasDataHolder, PolarsDataHolder
from sharkadm.sharkadm_logger import adm_logger
from sharkadm.utils import approved_data


class DataFilter(ABC):
    """Create child class to filter data in data_holder"""

    @property
    def name(self) -> str:
        return self.__class__.__name__

    @abstractmethod
    def get_filter_mask(self, data_holder: PandasDataHolder) -> pd.Series | None: ...


class PolarsDataFilter(ABC):
    """Create child class to filter data in data_holder"""

    @property
    def name(self) -> str:
        return self.__class__.__name__

    @abstractmethod
    def get_filter_mask(
        self, data_holder: PolarsDataHolder
    ) -> pl.expr.expr.Expr | None: ...


class DataFilterRestrictDepth(DataFilter):
    col_to_check = "location_wb"

    def get_filter_mask(self, data_holder: PandasDataHolder) -> pd.Series | None:
        col = "location_wb"
        if col not in data_holder.data:
            adm_logger.log_workflow(
                f"Could not filter data. Missing column {col}", level=adm_logger.ERROR
            )
            raise
        col = "location_county"
        if col not in data_holder.data:
            adm_logger.log_workflow(
                f"Could not filter data. Missing column {col}", level=adm_logger.ERROR
            )
            raise

        # boolean_wb = data_holder.data['location_wb'] != 'N'
        boolean_wb = (data_holder.data["location_wb"] == "Y") | (
            data_holder.data["location_wb"] == "P"
        )
        boolean_county = data_holder.data["location_county"] != ""
        return boolean_wb | boolean_county
        # return data_holder.data['location_wb'] != 'N'


class PolarsDataFilterRestrictAreaR(PolarsDataFilter):
    def get_filter_mask(self, data_holder: PolarsDataHolder) -> pl.Series:
        if "location_r" in data_holder.data:
            return data_holder.data["location_r"]
        return (
            data_holder.data["location_ra"]
            | data_holder.data["location_rb"]
            | data_holder.data["location_rc"]
            | data_holder.data["location_rg"]
            | data_holder.data["location_rh"]
            | data_holder.data["location_ro"]
        )


class PolarsDataFilterApprovedData(PolarsDataFilter):
    def get_filter_mask(self, data_holder: PolarsDataHolder) -> pl.Series:
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


class PolarsDataFilterInside12nm(PolarsDataFilter):
    def get_filter_mask(self, data_holder: PolarsDataHolder) -> pd.Series | None:
        col = "location_wb"
        if col not in data_holder.data:
            adm_logger.log_workflow(
                f"Could not filter data. Missing column {col}", level=adm_logger.ERROR
            )
            raise
        col = "location_county"
        if col not in data_holder.data:
            adm_logger.log_workflow(
                f"Could not filter data. Missing column {col}", level=adm_logger.ERROR
            )
            raise

        # boolean_wb = data_holder.data['location_wb'] != 'N'
        boolean_wb = (data_holder.data["location_wb"] == "Y") | (
            data_holder.data["location_wb"] == "P"
        )
        boolean_county = data_holder.data["location_county"] != ""
        return boolean_wb | boolean_county


class PolarsDataFilterApprovedAndOutside12nm(PolarsDataFilter):
    def get_filter_mask(self, data_holder: PolarsDataHolder) -> pd.Series | None:
        approved_bool = PolarsDataFilterApprovedData().get_filter_mask(data_holder)
        inside12nm_bool = PolarsDataFilterInside12nm().get_filter_mask(data_holder)
        outside12nm_bool = ~inside12nm_bool
        return approved_bool | outside12nm_bool
