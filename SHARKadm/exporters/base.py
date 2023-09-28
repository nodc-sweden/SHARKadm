from abc import ABC, abstractmethod
from typing import Protocol
import pandas as pd


class DataHolderProtocol(Protocol):

    @property
    @abstractmethod
    def data(self) -> pd.DataFrame:
        ...

    @property
    @abstractmethod
    def data_type(self) -> str:
        ...


class Exporter(ABC):
    """Abstract base class used as a blueprint for exporting stuff in a DataHolder"""

    def __repr__(self) -> str:
        return f'Exporter: {self.__class__.__name__}'

    @abstractmethod
    def export(self, data_holder: DataHolderProtocol) -> None:
        ...
