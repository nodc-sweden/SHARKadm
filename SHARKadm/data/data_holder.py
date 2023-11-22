import logging
from abc import ABC, abstractmethod

import pandas as pd

from SHARKadm.data.data_source.base import DataFile

logger = logging.getLogger(__name__)


class DataHolder(ABC):
    """Class to hold data from a specific data type. Add data using the add_data_source method"""
    _data = pd.DataFrame()

    def __repr__(self) -> str:
        return f'{self.__class__.__name__} (data type = "{self.data_type}"): {self.dataset_name}'

    @property
    def workflow_message(self) -> str:
        return f'Using DataHolder: {self.__class__.__name__}'

    @property
    def data_holder_name(self) -> str:
        """Short name of the data_holder"""
        return self.__class__.__name__

    @staticmethod
    @abstractmethod
    def get_data_holder_description() -> str:
        """Verbal description describing what the data_holder is doing"""
        ...

    @property
    def data(self) -> pd.DataFrame:
        return self._data

    @data.setter
    def data(self, df: pd.DataFrame) -> None:
        if type(df) != pd.DataFrame:
            raise 'Data must be of type pd.DataFrame'
        self._data = df

    # @property
    # @abstractmethod
    # def data(self) -> pd.DataFrame:
    #     ...

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




