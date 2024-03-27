import logging
from abc import ABC, abstractmethod

import pandas as pd

from sharkadm import adm_logger
from sharkadm.data.data_source.base import DataFile

logger = logging.getLogger(__name__)


class DataHolder(ABC):
    """Class to hold data from a specific data type. Add data using the add_data_source method"""

    def __init__(self, *args, **kwargs):
        self._data = pd.DataFrame()
        self._data_sources: dict[str, DataFile] = dict()
        self._number_metadata_rows = 0

    def __repr__(self) -> str:
        return f'{self.__class__.__name__} (data type = "{self.data_type}"): {self.dataset_name}'

    def __add__(self, other):
        if self.data_type != other.data_type:
            adm_logger.log_workflow(f'Not allowed to merge data_sources of different data_types: {self.data_type} and {other.data_type}')
            return
        if self.dataset_name == other.dataset_name:
            adm_logger.log_workflow(f'Not allowed to merge to instances of the same dataset: {self.dataset_name}')
            return
        concat_data = pd.concat([self.data, other.data], axis=0)
        concat_data.fillna('', inplace=True)
        concat_data.reset_index(inplace=True)
        cdh = ConcatDataHolder()
        cdh.data = concat_data
        cdh.data_type = self.data_type
        return cdh

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
    def number_metadata_rows(self) -> int:
        return self._number_metadata_rows

    @property
    def columns(self) -> list[str]:
        return sorted(self.data.columns)

    @property
    def mapped_columns(self) -> dict[str, str]:
        mapped = dict()
        for name, source in self._data_sources.items():
            mapped.update(source.mapped_columns)
        return mapped


class ConcatDataHolder(DataHolder):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._data_type = ''

    @staticmethod
    def get_data_holder_description() -> str:
        return 'This is a concatenated data holder'

    @property
    def data_type(self) -> str:
        return self._data_type

    @data_type.setter
    def data_type(self, data_type: str) -> None:
        self._data_type = data_type

    @property
    def dataset_name(self) -> str:
        return '#'.join(self.data['dataset_name'].unique())

    @property
    def number_metadata_rows(self) -> None:
        return





