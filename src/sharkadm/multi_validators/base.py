import time
from abc import abstractmethod
from typing import Protocol, Type

import pandas as pd

from sharkadm import adm_logger, config
from sharkadm.data import get_valid_data_holders
from sharkadm.validators import Validator


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

    @property
    @abstractmethod
    def data_structure(self) -> str:
        ...


class MultiValidator(Validator):
    """Abstract base class used as a blueprint for doing multiple changes in data in a DataHolder"""
    valid_data_types: list[str] = []
    invalid_data_types: list[str] = []

    valid_data_holders: list[str] = []
    invalid_data_holders: list[str] = []

    valid_data_structures: list[str] = []
    invalid_data_structures: list[str] = []

    _validators: list[Type[Validator]] = []

    def __init__(self, **kwargs):
        super().__init__()
        self._kwargs = kwargs

    def __repr__(self) -> str:
        return f'Multi validator: {self.__class__.__name__}'

    @property
    def name(self) -> str:
        return self.__class__.__name__

    @staticmethod
    @abstractmethod
    def get_validator_description() -> str:
        """Verbal description describing what the multi validator is doing"""
        ...

    @property
    def description(self) -> str:
        return self.get_validator_description()

    def validate(self, data_holder: DataHolderProtocol) -> None:
        if data_holder.data_type.lower() not in config.get_valid_data_types(valid=self.valid_data_types,
                                                                            invalid=self.invalid_data_types):
            adm_logger.log_workflow(f'Invalid data_type {data_holder.data_type} for multi validator'
                                    f' {self.__class__.__name__}', level=adm_logger.DEBUG)
            return
        if data_holder.__class__.__name__ not in get_valid_data_holders(valid=self.valid_data_holders,
                                                                       invalid=self.invalid_data_holders):
            adm_logger.log_workflow(f'Invalid data_holder {data_holder.__class__.__name__} for multi validator'
                                    f' {self.__class__.__name__}')
            return
        if data_holder.data_structure.lower() not in config.get_valid_data_structures(
                valid=self.invalid_data_structures,
                invalid=self.invalid_data_structures):
            adm_logger.log_workflow(f'Invalid data_format {data_holder.data_structure} for multi validator'
                                    f' {self.__class__.__name__}', level=adm_logger.DEBUG)
            return

        adm_logger.log_workflow(f'Applying multi validator: {self.__class__.__name__}', add=self.get_validator_description(), level=adm_logger.DEBUG)
        t0 = time.time()
        for vali in self._validators:
            vali().validate(data_holder=data_holder)
        adm_logger.log_workflow(f'Multi validator {self.__class__.__name__} executed in {time.time()-t0} seconds', level=adm_logger.DEBUG)

    def _validate(self, data_holder: DataHolderProtocol) -> None:
        # Dummy method must be present to implement MultiValidator
        pass



