import time
from abc import ABC, abstractmethod
from typing import Protocol

import pandas as pd

from sharkadm import adm_logger
from sharkadm import config
from sharkadm.data import get_valid_data_holders


class DataHolderProtocol(Protocol):

    @property
    @abstractmethod
    def data(self) -> pd.DataFrame:
        ...

    @property
    @abstractmethod
    def data_type(self) -> str:
        ...


class Validator(ABC):
    """Abstract base class used as a blueprint to validate/tidy/check data in a DataHolder"""
    valid_data_types = []
    invalid_data_types = []

    valid_data_holders = []
    invalid_data_holders = []

    def __init__(self, **kwargs):
        self._kwargs = kwargs

    def __repr__(self) -> str:
        return f'Validator: {self.__class__.__name__}'

    @staticmethod
    @abstractmethod
    def get_validator_description() -> str:
        """Verbal description describing what the validator is doing"""
        ...
    
    def validate(self, data_holder: DataHolderProtocol) -> None:
        if data_holder.data_type.lower() not in config.get_valid_data_types(valid=self.valid_data_types,
                                                                            invalid=self.invalid_data_types):
            adm_logger.log_workflow(
                f'Invalid data_type {data_holder.data_type} for validator {self.__class__.__name__}', level=adm_logger.DEBUG)
            return
        if data_holder.__class__.__name__ not in get_valid_data_holders(valid=self.valid_data_holders,
                                                                       invalid=self.invalid_data_holders):
            adm_logger.log_workflow(f'Invalid data_holder {data_holder.__class__.__name__} for validator'
                                    f' {self.__class__.__name__}')
            return

        adm_logger.log_workflow(f'Applying validator: {self.__class__.__name__}', add=self.get_validator_description())
        t0 = time.time()
        self._validate(data_holder=data_holder)
        adm_logger.log_workflow(f'Validator {self.__class__.__name__} executed in {time.time() - t0} seconds')

    @abstractmethod
    def _validate(self, data_holder: DataHolderProtocol) -> None:
        ...

