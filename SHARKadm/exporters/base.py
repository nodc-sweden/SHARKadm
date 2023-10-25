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

    @property
    @abstractmethod
    def dataset_name(self) -> str:
        ...


class Exporter(ABC):
    """Abstract base class used as a blueprint for exporting stuff in a DataHolder"""

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
        adm_logger.log_workflow(self.workflow_message)
        self._export(data_holder=data_holder)

    @abstractmethod
    def _export(self, data_holder: DataHolderProtocol) -> None:
        ...
