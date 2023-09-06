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


class Validator(ABC):
    """Abstract base class used as a blueprint to validate/tidy/check data in a DataHolder"""

    def __repr__(self) -> str:
        return f'Validator: {self.__class__.__name__}'

    @abstractmethod
    def validate(self, data_holder: DataHolderProtocol) -> None:
        ...
