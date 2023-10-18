from abc import ABC, abstractmethod
from typing import Protocol

import pandas as pd

from SHARKadm import adm_logger


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

    def __init__(self, **kwargs):
        self._kwargs = kwargs

    def __repr__(self) -> str:
        return f'Validator: {self.__class__.__name__}'

    @staticmethod
    @abstractmethod
    def get_validator_description() -> str:
        """Verbal description describing what the validator is doing"""
        ...

    @property
    def workflow_message(self) -> str:
        return f'Applying validator: {self.__class__.__name__}'
    
    def validate(self, data_holder: DataHolderProtocol) -> None:
        adm_logger.log_workflow(self.workflow_message)
        self._validate(data_holder=data_holder)

    @abstractmethod
    def _validate(self, data_holder: DataHolderProtocol) -> None:
        ...

