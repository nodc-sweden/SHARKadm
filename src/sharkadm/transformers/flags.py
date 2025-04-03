from .base import (
    Transformer,
    DataHolderProtocol,
    PolarsTransformer,
    PolarsDataHolderProtocol,
)
from sharkadm import adm_logger

import polars as pl


class ConvertFlagsToSDN(Transformer):
    valid_data_types = ["physicalchemical"]
    flag_col = "quality_flag"
    mapping = {
        "": "1",
        "BLANK": "1",
        "A": "1",
        "E": "2",
        "S": "3",
        "B": "4",
        "<": "6",
        ">": "7",
        "R": "8",
        "M": "9",
    }

    @staticmethod
    def get_transformer_description() -> str:
        return (
            f"Converts values in internal column {ConvertFlagsToSDN.flag_col} "
            f"to SeaDataNet schema"
        )

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        for flag, df in data_holder.data.groupby(self.flag_col):
            stripped_flag = str(flag).strip()
            new_flag = self.mapping.get(stripped_flag)
            if new_flag:
                adm_logger.log_transformation(
                    f"Converting flag: {flag} -> {new_flag} ({len(df)} places)",
                    level=adm_logger.INFO,
                )
                data_holder.data.loc[df.index, self.flag_col] = new_flag
        # Missing value
        boolean = data_holder.data["value"] == ""
        data_holder.data.loc[boolean, self.flag_col] = "9"


class PolarsConvertFlagsToSDN(PolarsTransformer):
    valid_data_types = ["physicalchemical"]
    flag_col = "quality_flag"
    mapping = {
        "": "1",
        "BLANK": "1",
        "A": "1",
        "E": "2",
        "S": "3",
        "B": "4",
        "<": "6",
        ">": "7",
        "R": "8",
        "M": "9",
    }

    @staticmethod
    def get_transformer_description() -> str:
        return (
            f"Converts values in internal column {ConvertFlagsToSDN.flag_col} "
            f"to SeaDataNet schema"
        )

    def _transform(self, data_holder: PolarsDataHolderProtocol) -> None:
        data_holder.data = data_holder.data.with_columns(
            pl.col(self.flag_col).replace(self.mapping, default="9").alias(self.flag_col)
        )
