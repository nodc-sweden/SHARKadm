from abc import ABC, abstractmethod

import pandas as pd
import pathlib

from typing import Protocol

# from SHARKadm.config.import_config import ImportMatrixMapper


class ImportMapper(Protocol):

    def get_internal_name(self, external_par: str) -> str:
        ...


class DataFile(ABC):
    _data: pd.DataFrame = pd.DataFrame()
    _original_header: list = []
    _header_mapper: ImportMapper = None

    def __init__(self,
                 path: str | pathlib.Path = None,
                 data_type: str = None,
                 encoding: str = 'cp1252',
                 ) -> None:
        self._path: pathlib.Path = pathlib.Path(path)
        self._data_type = data_type
        self._encoding: str = encoding
        self._original_header: list = []
        self._data: pd.DataFrame = pd.DataFrame()

        self._load_file()
        self._add_source_to_data()
        self._save_original_header()

    def __repr__(self) -> str:
        return f'{self.__class__.__name__} with data type "{self._data_type}": {self._path}'

    @abstractmethod
    def _load_file(self) -> None:
        ...

    def _add_source_to_data(self) -> None:
        self._data['source'] = self.source

    def _save_original_header(self) -> None:
        self._original_header = list(self._data.columns)

    @property
    def data_type(self) -> str:
        return self._data_type.lower()

    @property
    def source(self) -> str:
        return str(self._path)

    @property
    def header(self) -> list[str]:
        return list(self._data.columns)

    def map_header(self, mapper: ImportMapper) -> None:
        mapped_header = []
        for item in self._original_header:
            mapped_header.append(mapper.get_internal_name(item))
        self._data.columns = mapped_header
        self._header_mapper = mapper

    def get_data(self) -> pd.DataFrame:
        return self._data

