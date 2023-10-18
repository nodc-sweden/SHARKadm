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


class Transformer(ABC):
    """Abstract base class used as a blueprint for changing data in a DataHolder"""

    def __init__(self, **kwargs):
        self._kwargs = kwargs

    def __repr__(self) -> str:
        return f'Transformer: {self.__class__.__name__}'

    @property
    def workflow_message(self) -> str:
        return f'Applying transformer: {self.__class__.__name__}'

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
        adm_logger.log_workflow(self.workflow_message)
        self._transform(data_holder=data_holder)

    @abstractmethod
    def _transform(self, data_holder: DataHolderProtocol) -> None:
        ...
