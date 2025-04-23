import logging
from typing import Protocol

import pandas as pd
import polars as pl

from sharkadm.data.data_holder import PandasDataHolder, PolarsDataHolder
from sharkadm.data.data_source.base import DataDataFrame, DataFile

logger = logging.getLogger(__name__)


class HeaderMapper(Protocol):
    def get_internal_name(self, external_par: str) -> str: ...



class PandasDataFrameDataHolder(PandasDataHolder):
    _data_type: str = ""
    _data_structure: str = ""

    def __init__(
        self,
        df: pd.DataFrame,
        data_structure: str,
        data_type: str,
        header_mapper: HeaderMapper = None,
    ):
        super().__init__()

        self._header_mapper = header_mapper

        self._data: df
        self._data_structure = data_structure
        self._data_type = data_type

        self._load_data(df)

    @staticmethod
    def get_data_holder_description() -> str:
        return """Holds data from a given pandas dataframe"""

    def _load_data(self, df: pd.DataFrame) -> None:
        d_source = DataDataFrame(df, data_type=self.data_type)
        if self._header_mapper:
            d_source.map_header(self._header_mapper)
        self._set_data_source(d_source)
        # self._data = self._get_data_from_data_source(d_source)
        self._dataset_name = "Pandas dataframe"

    @staticmethod
    def _get_data_from_data_source(data_source: DataFile) -> pd.DataFrame:
        data = data_source.get_data()
        data = data.fillna("")
        data.reset_index(inplace=True, drop=True)
        return data

    # @property
    # def data(self) -> pd.DataFrame:
    #     return self._data
    #
    # @data.setter
    # def data(self, df: pd.DataFrame) -> None:
    #     if type(df) != pd.DataFrame:
    #         raise 'Data must be of type pd.DataFrame'
    #     self._data = df

    @property
    def data_type(self) -> str:
        return self._data_type

    @property
    def dataset_name(self) -> str:
        return self._dataset_name

    @property
    def columns(self) -> list[str]:
        return sorted(self.data.columns)


class PolarsDataFrameDataHolder(PolarsDataHolder):
    _data_type: str = "unknown"
    _data_type_internal: str = "unknown"
    _data_structure: str = "row"

    def __init__(
        self,
        df: pl.DataFrame,
        header_mapper: HeaderMapper = None,
    ):
        super().__init__()

        self._header_mapper = header_mapper

        self._data: pl.DataFrame = df
        self._dataset_name = "Polars dataframe"
        # self._data_structure = data_structure
        # self._data_type = data_type

        # self._load_data(df)

    @staticmethod
    def get_data_holder_description() -> str:
        return """Holds data from a given pandas dataframe"""

    def _load_data(self, df: pd.DataFrame) -> None:
        d_source = DataDataFrame(df, data_type=self.data_type)
        if self._header_mapper:
            d_source.map_header(self._header_mapper)
        self._set_data_source(d_source)
        self._dataset_name = "Polars dataframe"

    @staticmethod
    def _get_data_from_data_source(data_source: DataFile) -> pd.DataFrame:
        data = data_source.get_data()
        data = data.fillna("")
        data.reset_index(inplace=True, drop=True)
        return data

    @property
    def data_type(self) -> str:
        return self._data_type

    @property
    def data_type_internal(self) -> str:
        return self._data_type_internal

    @property
    def dataset_name(self) -> str:
        return self._dataset_name

    @property
    def columns(self) -> list[str]:
        return sorted(self.data.columns)
