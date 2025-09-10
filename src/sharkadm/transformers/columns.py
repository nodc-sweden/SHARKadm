import re

import polars as pl

from sharkadm.config import get_column_views_config
from sharkadm.utils import approved_data, matching_strings

from ..data import PolarsDataHolder
from ..sharkadm_logger import adm_logger
from .base import (
    DataHolderProtocol,
    PolarsTransformer,
    Transformer,
)


class AddColumnViewsColumns(Transformer):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._column_views = get_column_views_config()

    @staticmethod
    def get_transformer_description() -> str:
        return (
            "Adds empty columns from column_views not already present in dataframe. "
            "NN data dded!"
        )

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        columns_to_add = self._column_views.get_columns_for_view(
            data_holder.data_type_internal
        )
        empty_cols_to_add = []
        for col in columns_to_add:
            if col in data_holder.data.columns:
                continue
            # data_holder.data[col] = ''
            empty_cols_to_add.append(col)
            # data_holder.data.loc[:, col] = ''
        data_holder.data.loc[:, empty_cols_to_add] = ""


class AddDEPHqcColumn(Transformer):
    valid_data_holders = ("LimsDataHolder",)

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._column_views = get_column_views_config()

    @staticmethod
    def get_transformer_description() -> str:
        return "Adds QC column for DEPH if missing"

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        if "Q_DEPH" not in data_holder.data.columns:
            data_holder.data["Q_DEPH"] = ""


class RemoveColumns(Transformer):
    def __init__(self, *args, **kwargs):
        self._args = args
        super().__init__(**kwargs)

    @staticmethod
    def get_transformer_description() -> str:
        return "Removes columns matching given strings in args"

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        exclude_columns = []
        for col in data_holder.data.columns:
            for arg in self._args:
                if re.match(arg, col):
                    exclude_columns.append(col)
                    break
        keep_columns = [
            col for col in data_holder.data.columns if col not in exclude_columns
        ]
        data_holder.data = data_holder.data[keep_columns]


class PolarsRemoveColumns(PolarsTransformer):
    def __init__(self, *args, **kwargs):
        self._args = args
        super().__init__(**kwargs)

    @staticmethod
    def get_transformer_description() -> str:
        return "Removes columns matching given strings in args"

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        columns_to_remove = set()
        for arg in self._args:
            columns_to_remove |= set(
                filter(lambda x: re.match(arg, x), data_holder.data.columns)
            )
        data_holder.data = data_holder.data.drop(columns_to_remove)


class SortColumns(Transformer):
    def __init__(self, key=None, **kwargs):
        self._key = key
        super().__init__(**kwargs)

    @staticmethod
    def get_transformer_description() -> str:
        return 'Sorting columns in data. Option to give "key" for the sort funktion'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        new_col_order = sorted(data_holder.data.columns, key=self._key)
        data_holder.data = data_holder.data[new_col_order]


class PolarsAddApprovedKeyColumn(PolarsTransformer):
    @staticmethod
    def get_transformer_description() -> str:
        return "Adds a column with keys to match against approved data"

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data = approved_data.add_concatenated_column(
            data_holder.data, column_name="approved_key"
        )


class PolarsAddColumnViewsColumns(PolarsTransformer):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._column_views = get_column_views_config()

    @staticmethod
    def get_transformer_description() -> str:
        return (
            "Adds empty columns from column_views not already present in dataframe. "
            "NN data dded!"
        )

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        columns_to_add = self._column_views.get_columns_for_view(
            data_holder.data_type_internal
        )
        empty_cols_to_add = []
        for col in columns_to_add:
            if col in data_holder.data.columns:
                continue
            empty_cols_to_add.append(pl.lit("").alias(col))
        data_holder.data = data_holder.data.with_columns(empty_cols_to_add)


class PolarsSortColumns(PolarsTransformer):
    def __init__(self, key=None, **kwargs):
        self._key = key
        super().__init__(**kwargs)

    @staticmethod
    def get_transformer_description() -> str:
        return 'Sorting columns in data. Option to give "key" for the sort funktion'

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        new_col_order = sorted(data_holder.data.columns, key=self._key)
        data_holder.data = data_holder.data[new_col_order]


class PolarsAddDEPHqcColumn(Transformer):
    valid_data_holders = ("PolarsLimsDataHolder",)

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._column_views = get_column_views_config()

    @staticmethod
    def get_transformer_description() -> str:
        return "Adds QC column for DEPH if missing"

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        if "Q_DEPH" not in data_holder.data.columns:
            data_holder.data["Q_DEPH"] = ""


class AddColumnsWithPrefix(Transformer):
    def __init__(
        self, apply_on_columns: tuple[str] | None = None, col_prefix: str | None = None
    ) -> None:
        super().__init__()
        self.apply_on_columns = apply_on_columns
        self.col_prefix = col_prefix
        self._handled_cols = dict()
        if not self.apply_on_columns or self.col_prefix is None:
            self._log(
                "Not enough input, will do nothing ",
                level=adm_logger.DEBUG,
            )
            return

    @staticmethod
    def get_transformer_description() -> str:
        return "Copies columns to new column with prefix specified by the user"

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        for source_col in self._get_matching_cols(data_holder):
            target_col = f"{self.col_prefix}_{source_col}"
            if target_col in data_holder.data.columns:
                self._log(
                    f"Column already present. Will do nothing: {target_col}",
                    level=adm_logger.DEBUG,
                )
                continue
            data_holder.data = data_holder.data.with_columns(
                [pl.col(source_col).alias(target_col)]
            )
            self._log(
                f"Column {target_col} set from source column {source_col}",
                level=adm_logger.DEBUG,
            )

    def _get_matching_cols(self, data_holder: PolarsDataHolder) -> list[str]:
        return matching_strings.get_matching_strings(
            strings=data_holder.data.columns, match_strings=self.apply_on_columns
        )
