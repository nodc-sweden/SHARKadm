import numpy as np
import pandas as pd
import polars as pl

from sharkadm.config import get_header_mapper_from_data_holder
from sharkadm.data import PolarsDataHolder
from sharkadm.exporters.base import DataHolderProtocol, Exporter, PolarsExporter
from sharkadm.sharkadm_logger import adm_logger


class DataFrame(Exporter):
    def __init__(
        self,
        header_as: str | None = None,
        float_columns: bool | list[str] = False,
    ):
        super().__init__()
        self._header_as = header_as
        self._float_columns = float_columns

    @staticmethod
    def get_exporter_description() -> str:
        return """
        Returns a modified dataframe. Option to:
        map header via "header_as"
        convert certain columns to float via: "float_columns". If set to True,
            parameter column and position.columns are converted
        """

    def _export(self, data_holder: DataHolderProtocol) -> pd.DataFrame | None:
        df = data_holder.data.copy(deep=True)
        if self._float_columns:
            if self._float_columns is True:
                self._float_columns = self._get_float_columns(df)
            for col in self._float_columns:
                if col not in df.columns:
                    continue
                try:
                    df[col] = pd.to_numeric(df[col])
                except ValueError:
                    for value, part_df in df.groupby(col):
                        new_value = self._convert_to_float(str(value))
                        if new_value == np.nan:
                            self._log(
                                f"Could not convert {value} to numeric in column {col}. "
                                f"setting value to np.nan ({len(part_df)} places).",
                                item=col,
                                level=adm_logger.WARNING,
                            )
                        df.loc[part_df.index, col] = new_value
        if self._header_as:
            mapper = get_header_mapper_from_data_holder(
                data_holder, import_column=self._header_as
            )
            if not mapper:
                self._log(
                    f"Could not find mapper using header_as = {self._header_as}",
                    level=adm_logger.WARNING,
                )
                return
            new_column_names = [mapper.get_external_name(col) for col in df.columns]
            df.columns = new_column_names
        return df

    def _get_float_columns(self, df: pd.DataFrame):
        float_columns = ["value"]
        for col in df.columns:
            if "latitude" in col:
                float_columns.append(col)
            if "longitude" in col:
                float_columns.append(col)
            if "temperature" in col:
                float_columns.append(col)
            if "pressure" in col:
                float_columns.append(col)
            if "depth" in col:
                float_columns.append(col)
        return float_columns

    def _convert_to_float(self, value: str):
        try:
            return float(value)
        except ValueError:
            return np.nan


class PolarsDataFrame(PolarsExporter):
    def __init__(
        self,
        header_as: str | None = None,
        float_columns: bool | list[str] = False,
    ):
        super().__init__()
        self._header_as = header_as
        self._float_columns = float_columns

    @staticmethod
    def get_exporter_description() -> str:
        return """
        Returns a modified dataframe. Option to:
        map header via "header_as"
        convert certain columns to float via: "float_columns". If set to True,
            parameter column and position.columns are converted
        """

    def _export(self, data_holder: PolarsDataHolder) -> pl.DataFrame | None:
        df = data_holder.data.with_columns()
        if self._float_columns:
            if self._float_columns is True:
                self._float_columns = self._get_float_columns(df)
            for col in self._float_columns:
                if col not in df.columns:
                    continue
                try:
                    df = df.with_columns(pl.col(col).cast(float))
                except pl.exceptions.InvalidOperationError:
                    for (value,), part_df in df.group_by(col):
                        new_value = self._convert_to_float(str(value))
                        if new_value == np.nan:
                            self._log(
                                f"Could not convert {value} to numeric in column {col}. "
                                f"setting value to np.nan ({len(part_df)} places).",
                                item=col,
                                level=adm_logger.WARNING,
                            )
                        print(f"{value=}")
                        print(f"{new_value=}")
                        df = df.with_columns(
                            pl.when(pl.col(col) == value)
                            .then(pl.lit(new_value))
                            .otherwise(pl.col(col))
                            .alias(col)
                        )
        if self._header_as:
            mapper = get_header_mapper_from_data_holder(
                data_holder, import_column=self._header_as
            )
            if not mapper:
                self._log(
                    f"Could not find mapper using header_as = {self._header_as}",
                    level=adm_logger.WARNING,
                )
                return
            new_column_names = [mapper.get_external_name(col) for col in df.columns]
            df.columns = new_column_names
        return df

    def _get_float_columns(self, df: pd.DataFrame):
        float_columns = ["value"]
        for col in df.columns:
            if "latitude" in col:
                float_columns.append(col)
            if "longitude" in col:
                float_columns.append(col)
            if "temperature" in col:
                float_columns.append(col)
            if "pressure" in col:
                float_columns.append(col)
            if "depth" in col:
                float_columns.append(col)
        return float_columns

    def _convert_to_float(self, value: str):
        try:
            return float(value)
        except ValueError:
            return np.nan
