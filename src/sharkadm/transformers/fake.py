import polars as pl

from ..data import PolarsDataHolder
from .base import PolarsTransformer


class FakeAddPressureFromDepth(PolarsTransformer):
    valid_data_holders = ("LimsDataHolder",)

    @staticmethod
    def get_transformer_description() -> str:
        return "Adds pressure = depth to lims data"

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        data_holder.data = data_holder.data.with_columns(
            pl.col("DEPH").alias("PRES"),
            pl.col("Q_DEPH").alias("Q_PRES"),
        )


class FakeAddCTDtagToColumns(PolarsTransformer):
    valid_data_holders = ("LimsDataHolder",)

    @staticmethod
    def get_transformer_description() -> str:
        return (
            "Adds _CTD to all parameter columns. This is so that data can be compatible "
            "with the cdtvis bokeh visualization."
        )

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        new_column = []
        for col in data_holder.data.columns:
            if "CTD" in col:
                new_column.append(col)
                continue
            if col.startswith(("Q_", "Q0_")) or f"Q_{col}" in data_holder.data.columns:
                new_column.append(f"{col}_CTD")
            else:
                new_column.append(col)
        data_holder.data.columns = new_column
