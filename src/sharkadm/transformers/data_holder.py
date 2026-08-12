import polars as pl

from ..data import PolarsDataHolder
from .base import PolarsTransformer

STATUS_CONFIG = dict()


class AddDataHolderName(PolarsTransformer):
    col_to_set = "data_holder_name"

    @staticmethod
    def get_transformer_description() -> str:
        return "Adds data_holder name"

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        data_holder.data = data_holder.data.with_columns(
            pl.lit(data_holder.name).alias(self.col_to_set)
        )
