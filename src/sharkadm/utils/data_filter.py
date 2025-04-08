from abc import ABC, abstractmethod

import pandas as pd

from sharkadm.data import PandasDataHolder
from sharkadm.sharkadm_logger import adm_logger


class DataFilter(ABC):
    """Create child class to filter data in data_holder"""

    @property
    def name(self) -> str:
        return self.__class__.__name__

    @abstractmethod
    def get_filter_mask(self, data_holder: PandasDataHolder) -> pd.Series | None: ...


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
