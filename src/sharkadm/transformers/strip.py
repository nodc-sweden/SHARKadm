import polars as pl

from ..data import PolarsDataHolder
from .base import PolarsTransformer


class PolarsStripAllValues(PolarsTransformer):
    @staticmethod
    def get_transformer_description() -> str:
        return "Strips all values in data"

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        data_holder.data = data_holder.data.with_columns(
            pl.col(pl.Utf8).str.strip_chars()
        )
