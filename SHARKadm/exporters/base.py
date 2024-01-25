from abc import ABC, abstractmethod
from typing import Protocol

import pandas as pd

from SHARKadm import adm_logger, config
from SHARKadm.data import get_valid_data_holders


class DataHolderProtocol(Protocol):

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


class Exporter(ABC):
    """Abstract base class used as a blueprint for exporting stuff in a DataHolder"""
    valid_data_types = []
    invalid_data_types = []

    valid_data_holders = []
    invalid_data_holders = []

    def __init__(self, **kwargs):
        self._kwargs = kwargs

    def __repr__(self) -> str:
        return f'Exporter: {self.__class__.__name__}'

    @staticmethod
    @abstractmethod
    def get_exporter_description() -> str:
        """Verbal description describing what the exporter is doing"""
        ...

    @property
    def workflow_message(self) -> str:
        return f'Applying exporter: {self.__class__.__name__}'
    
    def export(self, data_holder: DataHolderProtocol) -> None:
        if data_holder.data_type.lower() not in config.get_valid_data_types(valid=self.valid_data_types,
                                                                            invalid=self.invalid_data_types):
            adm_logger.log_workflow(f'Invalid data_type {data_holder.data_type} for exporter {self.__class__.__name__}', level=adm_logger.DEBUG)
            return
        if data_holder.__class__.__name__ not in get_valid_data_holders(valid=self.valid_data_holders,
                                                                        invalid=self.invalid_data_holders):
            adm_logger.log_workflow(f'Invalid data_holder {data_holder.__class__.__name__} for exporter'
                                    f' {self.__class__.__name__}')
            print(f'{self.valid_data_holders=}')
            print(f'{self.invalid_data_holders=}')
            print()
            print(f'{get_valid_data_holders(valid=self.valid_data_holders, invalid=self.invalid_data_holders)}')
            print()
            print(f'{data_holder.__class__.__name__=}')
            print('-' * 100)
            print('-' * 100)
            print('-' * 100)
            return
        adm_logger.log_workflow(self.workflow_message)
        self._export(data_holder=data_holder)

    @abstractmethod
    def _export(self, data_holder: DataHolderProtocol) -> None:
        ...
