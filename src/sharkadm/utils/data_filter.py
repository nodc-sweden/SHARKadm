from abc import ABC, abstractmethod
import pandas as pd
import numpy as np
from sharkadm import adm_logger

from sharkadm.data import DataHolder


class DataFilter(ABC):
    """Create child class to filter data in data_holder"""

    @property
    def name(self) -> str:
        return self.__class__.__name__

    @abstractmethod
    def get_filter_mask(self, data_holder: DataHolder) -> pd.Series | None:
        ...


class DataFilterRestrictDepth(DataFilter):
    col_to_check = 'location_wb'

    def get_filter_mask(self, data_holder: DataHolder) -> pd.Series | None:
        if self.col_to_check not in data_holder.data:
            adm_logger.log_workflow(f'Could not filter data. Missing column {self.col_to_check}', level=adm_logger.ERROR)
            raise
        return data_holder.data[self.col_to_check] != 'N'
