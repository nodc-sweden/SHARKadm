import polars as pl

from ..data import PolarsDataHolder
from .base import PolarsTransformer


class PolarsAddStaticInternetAccessInfo(PolarsTransformer):
    col_to_set = "internet_access"
    text_to_set = "https://shark.smhi.se"

    @staticmethod
    def get_transformer_description() -> str:
        return "Adds link to where you can find the data. This information is static!"

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        data_holder.data = data_holder.data.with_columns(
            pl.lit(self.text_to_set).alias(self.col_to_set)
        )
