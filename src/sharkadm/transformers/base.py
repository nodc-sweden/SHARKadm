from abc import ABC, abstractmethod
from typing import Protocol, TYPE_CHECKING
import time

import pandas as pd
import numpy as np

from sharkadm import adm_logger, config
from sharkadm.data import get_valid_data_holders
from sharkadm.data import is_valid_data_holder
from sharkadm.utils.data_filter import DataFilter

if TYPE_CHECKING:
    from sharkadm.data.data_holder import DataHolder


class DataHolderProtocol(Protocol):

    @property
    def data(self) -> pd.DataFrame:
        ...

    @data.setter
    def data(self, df: pd.DataFrame) -> None:
        ...

    @property
    @abstractmethod
    def data_type(self) -> str:
        ...

    @property
    @abstractmethod
    def data_type_internal(self) -> str:
        ...

    @property
    @abstractmethod
    def data_structure(self) -> str:
        ...

    @property
    @abstractmethod
    def dataset_name(self) -> str:
        ...


class Transformer(ABC):
    """Abstract base class used as a blueprint for changing data in a DataHolder"""
    valid_data_types = []
    invalid_data_types = []

    valid_data_holders = []
    invalid_data_holders = []

    valid_data_structures = []
    invalid_data_structures = []

    def __init__(self, data_filter: DataFilter = None, **kwargs):
        self._data_filter = data_filter
        self._kwargs = kwargs

    def __repr__(self) -> str:
        return f'Transformer: {self.__class__.__name__}'

    @property
    def name(self) -> str:
        return self.__class__.__name__

    @staticmethod
    @abstractmethod
    def get_transformer_description() -> str:
        """Verbal description describing what the transformer is doing"""
        ...

    @property
    def description(self) -> str:
        return self.get_transformer_description()

    def transform(self, data_holder: 'DataHolder') -> None:
        if data_holder.data_type_internal not in config.get_valid_data_types(valid=self.valid_data_types,
                                                                             invalid=self.invalid_data_types):
            adm_logger.log_workflow(f'Invalid data_type {data_holder.data_type_internal} for transformer'
                                    f' {self.__class__.__name__}', level=adm_logger.DEBUG)
            return
        if not is_valid_data_holder(data_holder, valid=self.valid_data_holders, invalid=self.invalid_data_holders):
            adm_logger.log_workflow(f'Invalid data_holder {data_holder.__class__.__name__} for transformer'
                                    f' {self.__class__.__name__}')
            return
        if data_holder.data_structure.lower() not in config.get_valid_data_structures(valid=self.invalid_data_structures,
                                                                                invalid=self.invalid_data_structures):
            adm_logger.log_workflow(f'Invalid data_format {data_holder.data_structure} for transformer'
                                    f' {self.__class__.__name__}', level=adm_logger.DEBUG)
            return

        adm_logger.log_workflow(f'Applying transformer: {self.__class__.__name__}', item=self.get_transformer_description(), level=adm_logger.DEBUG)
        t0 = time.time()
        self._transform(data_holder=data_holder)
        adm_logger.log_workflow(f'Transformer {self.__class__.__name__} executed in {time.time()-t0} seconds', level=adm_logger.DEBUG)

    @abstractmethod
    def _transform(self, data_holder: 'DataHolder') -> None:
        ...

    def _get_filter_mask(self, data_holder: 'DataHolder') -> pd.Series | np.ndarray:
        if not self._data_filter:
            return np.ones(len(data_holder.data), dtype=bool)
        adm_logger.log_workflow(f'Using data filter {self._data_filter.name} on transformer {self.name}', level=adm_logger.WARNING)
        return self._data_filter.get_filter_mask(data_holder)


