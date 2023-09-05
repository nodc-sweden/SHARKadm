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


class Transformer(ABC):
    """Abstract base class used as a blueprint for changing data in a DataHolder"""
    @abstractmethod
    def transform(self, data_holder: DataHolderProtocol) -> None:
        ...
