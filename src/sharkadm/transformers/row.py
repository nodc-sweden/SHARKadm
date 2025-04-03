from abc import abstractmethod
from typing import Protocol

import pandas as pd
import polars as pl

from sharkadm import adm_logger
from .base import Transformer, PolarsTransformer


class DataHolderProtocol(Protocol):
    @property
    def data(self) -> pd.DataFrame: ...

    @data.setter
    def data(self, df: pd.DataFrame) -> None: ...

    @property
    @abstractmethod
    def data_type(self) -> str: ...

    @property
    def number_metadata_rows(self) -> int: ...


class PolarsRowsDataHolderProtocol(Protocol):
    @property
    def data(self) -> pl.DataFrame: ...

    @data.setter
    def data(self, df: pl.DataFrame) -> None: ...

    @property
    @abstractmethod
    def data_type(self) -> str: ...

    @property
    def number_metadata_rows(self) -> int: ...


class AddRowNumber(Transformer):
    col_to_set = "row_number"

    @staticmethod
    def get_transformer_description() -> str:
        return (
            f"Adds row number. This column can typically be used to reference data "
            f"in log. Transformer should be set by the controller when setting "
            f"the data holder"
        )

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        if self.col_to_set in data_holder.data:
            adm_logger.log_transformation(
                f"Column {self.col_to_set} already present. Will not overwrite"
            )
            return
        data_holder.data[self.col_to_set] = (
            data_holder.data.index + 1 + data_holder.number_metadata_rows
        ).astype(str)


class PolarsAddRowNumber(PolarsTransformer):
    col_to_set = "row_number"

    @staticmethod
    def get_transformer_description() -> str:
        return (
            "Adds row number. This column can typically be used to reference data in "
            "log. Transformer should be set by the controller when setting the data "
            "holder"
        )

    def _transform(self, data_holder: PolarsRowsDataHolderProtocol) -> None:
        if self.col_to_set in data_holder.data:
            adm_logger.log_transformation(
                f"Column {self.col_to_set} already present. Will not overwrite"
            )
            return
        data_holder.data = data_holder.data.with_row_index(self.col_to_set, 1).cast(
            pl.String
        )
