import polars as pl

from ..data import PolarsDataHolder
from .base import PolarsTransformer


class RemoveRowsWithNoParameterValue(PolarsTransformer):
    valid_data_structures = ("row",)

    @staticmethod
    def get_transformer_description() -> str:
        return "Removes rows where parameter value has no value"

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        data_holder.data = data_holder.filter(pl.col("value") != "")
