from types import MappingProxyType

import polars as pl

from ..data import PolarsDataHolder
from .base import PolarsTransformer


class PolarsConvertFlagsToSDN(PolarsTransformer):
    valid_data_types = ("physicalchemical",)
    flag_col = "quality_flag"
    mapping = MappingProxyType(
        {
            "": "1",
            "BLANK": "1",
            "A": "1",
            "E": "2",
            "S": "3",
            "B": "4",
            "<": "Q",
            ">": "7",
            "R": "8",
            "M": "9",
        }
    )

    @staticmethod
    def get_transformer_description() -> str:
        return (
            f"Converts values in internal column {PolarsConvertFlagsToSDN.flag_col} "
            f"to SeaDataNet schema"
        )

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        data_holder.data = data_holder.data.with_columns(
            pl.col(self.flag_col)
            .replace_strict(self.mapping, default="9")
            .alias(self.flag_col)
        )
