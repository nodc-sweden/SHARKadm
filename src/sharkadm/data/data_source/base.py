import pathlib
from abc import ABC, abstractmethod
from typing import Protocol

import pandas as pd
import polars as pl

from sharkadm.config.data_type import DataType, data_type_handler


class ImportMapper(Protocol):
    def get_internal_name(self, external_par: str) -> str: ...

    def get_external_name(self, external_par: str) -> str: ...


class PolarsDataSource:
    def __init__(
        self,
        data_type: str | None = None,
    ) -> None:
        self._data_type_obj: DataType | None = None
        if data_type:
            self._data_type_obj = data_type_handler.get_data_type_obj(data_type)
        self._source = None
        self._data: pl.DataFrame = pl.DataFrame()
        self._original_header: list = []
        self._header_mapper: ImportMapper | None = None
        self._mapped_columns: dict = dict()
        self._not_mapped_columns: list = []
        self._unit_mapper: dict[str, str] = dict()

    def __repr__(self) -> str:
        return (
            f'{self.__class__.__name__} with data type "{self.data_type}": {self.source}'
        )

    def _do_post_init_stuf(self):
        self._strip_column_names()
        self._add_source_to_data()
        self._save_original_header()

    def _strip_column_names(self) -> None:
        self._data = self._data.rename(str.strip)

    def _remove_temp_tag(self):
        self._data = self._data.rename(lambda column: column.replace("TEMP.", ""))

    def _add_source_to_data(self) -> None:
        self._data = self._data.with_columns(source=pl.lit(self.source))

    def _save_original_header(self) -> None:
        self._original_header = self._data.columns

    @property
    def data_type_obj(self) -> DataType:
        return self._data_type_obj

    @data_type_obj.setter
    def data_type_obj(self, data_type_obj: DataType) -> None:
        if not isinstance(data_type_obj, DataType):
            raise TypeError(type(data_type_obj))
        self._data_type_obj = data_type_obj

    @property
    def data_type(self) -> str:
        return self._data_type_obj.data_type

    @property
    def source(self) -> str:
        return str(self._source)

    @property
    def header(self) -> list[str]:
        return list(self._data.columns)

    @property
    def header_mapper(self):
        return self._header_mapper

    @property
    def unit_mapper(self) -> dict[str, str]:
        return self._unit_mapper

    def map_header(self, mapper: ImportMapper) -> None:
        mapped_header = []
        for item in self._original_header:
            internal_name = mapper.get_internal_name(item)
            if item == internal_name:
                self._not_mapped_columns.append(item)
            self._mapped_columns[item] = internal_name
            while internal_name in mapped_header:
                internal_name = f"{internal_name}__duplicate"
            mapped_header.append(internal_name)
        self._data.columns = mapped_header
        self._header_mapper = mapper
        self._remove_temp_tag()

    def get_data(self) -> pl.DataFrame:
        return self._data

    @property
    def data(self) -> pl.DataFrame:
        return self._data

    @property
    def mapped_columns(self) -> dict[str, str]:
        return self._mapped_columns

    @property
    def not_mapped_columns(self) -> list[str]:
        return self._not_mapped_columns


class PolarsDataFile(PolarsDataSource, ABC):
    def __init__(
        self,
        path: str | pathlib.Path | None = None,
        data_type: str | None = None,
        encoding: str = "cp1252",
        **kwargs,
    ) -> None:
        super().__init__(data_type=data_type)
        self._path: pathlib.Path = pathlib.Path(path)
        self._source: str = str(self._path)
        self._encoding: str = encoding

        self._load_file()
        self._do_post_init_stuf()

    @property
    def path(self) -> pathlib.Path:
        return self._path

    @abstractmethod
    def _load_file(self) -> None: ...


class PolarsDataDataFrame(PolarsDataSource, ABC):
    def __init__(
        self, df: pl.DataFrame, data_type: str | None = None, source: str = ""
    ) -> None:
        super().__init__(data_type=data_type)
        self._source: str = source or "Given polars dataframe"
        self._data: pd.DataFrame = df
        self._original_header: list = []
        self._header_mapper: ImportMapper | None = None
        self._mapped_columns: dict = dict()
        self._not_mapped_columns: list = []

        self._do_post_init_stuf()
