import re

import polars as pl

from sharkadm.config import get_column_views_config
from sharkadm.utils import add_column, approved_data, matching_strings

from .. import adm_logger
from ..data import PolarsDataHolder
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
                if re.search(arg, col):
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
                filter(lambda x: re.search(arg, x), data_holder.data.columns)
            )
        data_holder.data = data_holder.data.drop(columns_to_remove)


class PolarsClearColumns(PolarsTransformer):
    def __init__(self, *args, **kwargs):
        self._args = args
        super().__init__(**kwargs)

    @staticmethod
    def get_transformer_description() -> str:
        return "Clears columns matching given strings in args"

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        columns_to_clear = set()
        for arg in self._args:
            columns_to_clear |= set(
                filter(lambda x: re.search(arg, x), data_holder.data.columns)
            )
        for col in columns_to_clear:
            data_holder.data = data_holder.data.with_columns(pl.lit("").alias(col))


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
            data_holder.data,
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


class PolarsAddDEPHqcColumn(PolarsTransformer):
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


class PolarsAddFloatColumns(PolarsTransformer):
    def __init__(self, columns: list[str], **kwargs):
        super().__init__(**kwargs)
        self._columns = columns

    @staticmethod
    def get_transformer_description() -> str:
        return "Converts matching columns to float. Uses regex to match columns."

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        columns = matching_strings.get_matching_strings(
            data_holder.data.columns, self._columns
        )
        for col in columns:
            data_holder.data = add_column.add_float_column(data_holder.data, col)


class PolarsAddColumnDiff(PolarsTransformer):
    def __init__(self, col1: str, col2: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._col1 = col1
        self._col2 = col2

    @staticmethod
    def get_transformer_description() -> str:
        return (
            "Add column with calculated difference between two given columns. "
            "New column name is <col1>_minus_<col2)>."
        )

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        new_col_name = f"{self._col1}_minus_{self._col2}"
        float_col1 = f"{self._col1}_float"
        float_col2 = f"{self._col2}_float"
        data_holder.data = add_column.add_float_column(
            data_holder.data, self._col1, column_name=float_col1
        )
        data_holder.data = add_column.add_float_column(
            data_holder.data, self._col2, column_name=float_col2
        )
        data_holder.data = data_holder.data.with_columns(
            (pl.col(float_col1) - pl.col(float_col2)).alias(new_col_name)
        )

        data_holder.data = data_holder.data.drop([float_col1, float_col2])


class PolarsAddBooleanLargerThan(PolarsTransformer):
    def __init__(self, col1: str, col2: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._col1 = col1
        self._col2 = col2

    @staticmethod
    def get_transformer_description() -> str:
        return (
            "Add boolean column with True values where col1 is larger than col2. "
            "New column name is <col1>_is_larger_than_<col2)>."
        )

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        new_col_name = f"{self._col1}_is_larger_than_{self._col2}"
        float_col1 = f"{self._col1}_float"
        float_col2 = f"{self._col2}_float"
        data_holder.data = add_column.add_float_column(
            data_holder.data, self._col1, column_name=float_col1
        )
        data_holder.data = add_column.add_float_column(
            data_holder.data, self._col2, column_name=float_col2
        )
        data_holder.data = data_holder.data.with_columns(
            (pl.col(float_col1) > pl.col(float_col2)).alias(new_col_name)
        )

        data_holder.data = data_holder.data.drop([float_col1, float_col2])


class PolarsFixDuplicateColumns(PolarsTransformer):
    @staticmethod
    def get_transformer_description() -> str:
        return (
            "Trying to fix duplicate columns in data. "
            "Logs warning if column with no values or values are the same. "
            "Columns are then removed. "
            "Logs error if conflict is found"
        )

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        mapper = dict()
        for col in data_holder.data.columns:
            if "__duplicate" not in col:
                continue
            orig_col = col.split("__duplicate")[0]
            mapper.setdefault(orig_col, set())
            mapper[orig_col].add(orig_col)
            mapper[orig_col].add(col)
        if not mapper:
            return
        for key, cols in mapper.items():
            if len(cols) > 2:
                self._log(
                    f"Cant handle more than two duplicated columns: {key}",
                    level=adm_logger.ERROR,
                )
                continue
            # Check if one of the columns are empty
            valid_col = None
            for col in cols:
                if set(data_holder.data[col]) != {""}:
                    valid_col = col
                    continue
                if set(data_holder.data[col]) != {""} and valid_col:
                    valid_col = None
            if valid_col:
                self._log(
                    f"No values in duplicated column {key}. "
                    f"Will keep the one with values.",
                    level=adm_logger.WARNING,
                )
                if "__duplicate" in valid_col:
                    data_holder.data = data_holder.data.drop(key)
                    data_holder.data = data_holder.data.rename({valid_col: key})
                else:
                    data_holder.data = data_holder.data.drop(f"{valid_col}__duplicate")
                continue
            columns = list[cols]
            if len(
                data_holder.data.filter(pl.col(columns[0]) == pl.col(columns[1]))
            ) == len(data_holder.data):
                self._log(
                    f"Duplicated column {key} has the same values. "
                    f"Will remove one of them.",
                    level=adm_logger.WARNING,
                )
                data_holder.data = data_holder.data.drop(f"{valid_col}__duplicate")
