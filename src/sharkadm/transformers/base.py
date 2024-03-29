from abc import ABC, abstractmethod
from typing import Protocol
import time

import pandas as pd

from sharkadm import adm_logger, config
from sharkadm.data import get_valid_data_holders


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
    def dataset_name(self) -> str:
        ...


class Transformer(ABC):
    """Abstract base class used as a blueprint for changing data in a DataHolder"""
    valid_data_types = []
    invalid_data_types = []

    valid_data_holders = []
    invalid_data_holders = []

    def __init__(self, **kwargs):
        self._kwargs = kwargs

    def __repr__(self) -> str:
        return f'Transformer: {self.__class__.__name__}'

    @property
    def transformer_name(self) -> str:
        """Short name of the transformer"""
        return self.__class__.__name__

    @staticmethod
    @abstractmethod
    def get_transformer_description() -> str:
        """Verbal description describing what the transformer is doing"""
        ...

    def transform(self, data_holder: DataHolderProtocol) -> None:
        if data_holder.data_type.lower() not in config.get_valid_data_types(valid=self.valid_data_types,
                                                                            invalid=self.invalid_data_types):
            adm_logger.log_workflow(f'Invalid data_type {data_holder.data_type} for transformer'
                                    f' {self.__class__.__name__}', level=adm_logger.DEBUG)
            return
        if data_holder.__class__.__name__ not in get_valid_data_holders(valid=self.valid_data_holders,
                                                                       invalid=self.invalid_data_holders):
            adm_logger.log_workflow(f'Invalid data_holder {data_holder.__class__.__name__} for transformer'
                                    f' {self.__class__.__name__}')
            return

        adm_logger.log_workflow(f'Applying transformer: {self.__class__.__name__}', add=self.get_transformer_description())
        t0 = time.time()
        self._transform(data_holder=data_holder)
        adm_logger.log_workflow(f'Transformer {self.__class__.__name__} executed in {time.time()-t0} seconds')

    @abstractmethod
    def _transform(self, data_holder: DataHolderProtocol) -> None:
        ...
