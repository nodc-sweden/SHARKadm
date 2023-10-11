import logging
from abc import ABC, abstractmethod

import pandas as pd

from SHARKadm.data.data_source.base import DataFile

logger = logging.getLogger(__name__)


class DataHolder(ABC):
    """Class to hold data from a specific data type. Add data using the add_data_source method"""

    def __repr__(self) -> str:
        return f'{self.__class__.__name__} (data type = "{self.data_type}"): {self.dataset_name}'

    @property
    @abstractmethod
    def data(self) -> pd.DataFrame:
        ...

    @property
    @abstractmethod
    def data_type(self) -> str:
        ...

    @property
    @abstractmethod
    def dataset_name(self) -> str:
        ...

    @property
    def columns(self) -> list[str]:
        return sorted(self.data.columns)




