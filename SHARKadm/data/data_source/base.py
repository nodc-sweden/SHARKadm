from abc import ABC, abstractmethod

import pandas as pd
import pathlib

from typing import Protocol

# from SHARKadm.config.import_config import ImportMatrixMapper


class ImportMapper(Protocol):

    def get_internal_name(self, external_par: str) -> str:
        ...


class DataFile(ABC):

    def __init__(self,
                 path: str | pathlib.Path = None,
                 data_type: str = None,
                 encoding: str = 'cp1252',
                 ) -> None:
        self._path: pathlib.Path = pathlib.Path(path)
        self._data_type = data_type
        self._encoding: str = encoding
        self._data: pd.DataFrame = pd.DataFrame()
        self._original_header: list = []
        self._header_mapper: ImportMapper | None = None
        self._mapped_columns = dict()

        self._load_file()
        self._strip_column_names()
        self._add_source_to_data()
        self._save_original_header()

    def __repr__(self) -> str:
        return f'{self.__class__.__name__} with data type "{self.data_type}": {self._path}'

    @abstractmethod
    def _load_file(self) -> None:
        ...

    def _strip_column_names(self) -> None:
        self._data.columns = [col.strip() for col in self._data.columns]

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
            internal_name = mapper.get_internal_name(item)
            self._mapped_columns[item] = internal_name
            mapped_header.append(internal_name)
        self._data.columns = mapped_header
        self._header_mapper = mapper
        print()
        print()
        print('-'*100)
        for old, new in zip(self._original_header, self._data.columns):
            print(f'{old} -> {new}')
        print()

    def add_concatenated_column(self, new_column: str, columns_to_use: list[str]) -> None:
        """Adds a concatenated column specified in new_column using the columns listed in columns_to_use"""
        for col in columns_to_use:
            if new_column not in self._data.columns:
                self._data[new_column] = self._data[col]
            else:
                self._data[new_column] = self._data[new_column] + ' <-> ' + self._data[col]

    @property
    def data(self) -> pd.DataFrame:
        return self.get_data()

    @property
    def mapped_columns(self) -> dict[str, str]:
        return self._mapped_columns

    def get_data(self) -> pd.DataFrame:
        return self._data

