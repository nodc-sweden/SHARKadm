from types import MappingProxyType

import polars as pl

from sharkadm.data import PolarsDataHolder
from sharkadm.sharkadm_logger import adm_logger
from sharkadm.transformers.base import (
    DataHolderProtocol,
    PolarsTransformer,
    Transformer,
)
from sharkadm.utils import matching_strings


class FixYesNo(Transformer):
    apply_on_columns = (".*accreditated",)
    _mapping = MappingProxyType({"y": "Y", "yes": "Y", "n": "N", "no": "N"})

    def __init__(self, apply_on_columns: list[str] | None = None, **kwargs):
        super().__init__(**kwargs)
        if apply_on_columns:
            self.apply_on_columns = apply_on_columns

    @staticmethod
    def get_transformer_description() -> str:
        return "Fix boolean values to YES or No (Y or N?)"

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        use_columns = matching_strings.get_matching_strings(
            strings=data_holder.data.columns, match_strings=self.apply_on_columns
        )
        for col in use_columns:
            data_holder.data[col] = data_holder.data[col].apply(self._map_value)

    def _map_value(self, x: str):
        return self._mapping.get(x.lower(), x)


class PolarsFixYesNo(PolarsTransformer):
    apply_on_columns = (".*accreditated",)
    _mapping = MappingProxyType({"y": "Y", "yes": "Y", "n": "N", "no": "N"})

    def __init__(self, apply_on_columns: list[str] | None = None, **kwargs):
        super().__init__(**kwargs)
        if apply_on_columns:
            self.apply_on_columns = apply_on_columns

    @staticmethod
    def get_transformer_description() -> str:
        return "Fix boolean values to YES or No (Y or N?)"

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        use_columns = matching_strings.get_matching_strings(
            strings=data_holder.data.columns, match_strings=self.apply_on_columns
        )

        for col in use_columns:
            data_holder.data = data_holder.data.with_columns(
                pl.col(col)
                .str.to_lowercase()
                .replace_strict(self._mapping, default=pl.col(col))
            )


class PolarsFixTrueAndFalse(PolarsTransformer):
    apply_on_column = "value"

    def __init__(self, apply_on_column: str | None = None, **kwargs):
        super().__init__(**kwargs)
        if apply_on_column:
            self.apply_on_column = apply_on_column

    @staticmethod
    def get_transformer_description() -> str:
        return "Fix boolean values to TRUE or FALSE"

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        self._fix_true(data_holder)
        self._fix_false(data_holder)

    def _fix_true(self, data_holder: PolarsDataHolder) -> None:
        for val in ["true", "t", "sant", "ja", "y", "yes"]:
            filtered_df = data_holder.data.filter(
                pl.col(self.apply_on_column).str.to_lowercase() == val,
                pl.col(self.apply_on_column) != "TRUE",
            )
            if not len(filtered_df):
                continue
            data_holder.data = data_holder.data.with_columns(
                pl.when(pl.col(self.apply_on_column).str.to_lowercase() == val)
                .then(pl.lit("TRUE"))
                .otherwise(pl.col(self.apply_on_column))
                .alias(self.apply_on_column)
            )
            adm_logger.log_transformation(
                f"Translated {val} -> TRUE ({len(filtered_df)} places)",
                level=adm_logger.INFO,
            )

    def _fix_false(self, data_holder: PolarsDataHolder) -> None:
        for val in ["false", "f", "falskt", "nej", "n", "no"]:
            filtered_df = data_holder.data.filter(
                pl.col(self.apply_on_column).str.to_lowercase() == val,
                pl.col(self.apply_on_column) != "FALSE",
            )
            if not len(filtered_df):
                continue
            data_holder.data = data_holder.data.with_columns(
                pl.when(pl.col(self.apply_on_column).str.to_lowercase() == val)
                .then(pl.lit("FALSE"))
                .otherwise(pl.col(self.apply_on_column))
                .alias(self.apply_on_column)
            )
            adm_logger.log_transformation(
                f"Translated {val} -> FALSE ({len(filtered_df)} places)",
                level=adm_logger.INFO,
            )
