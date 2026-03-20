import polars as pl

from ..data import PolarsDataHolder
from .base import PolarsTransformer


class PolarsAddStaticDataHoldingCenterEnglish(PolarsTransformer):
    col_to_set = "data_holding_centre"
    text_to_set = "Swedish Meteorological and Hydrological Institute (SMHI)"

    @staticmethod
    def get_transformer_description() -> str:
        return (
            f"Sets {PolarsAddStaticDataHoldingCenterEnglish.col_to_set} to "
            f"{PolarsAddStaticDataHoldingCenterEnglish.text_to_set}"
        )

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        data_holder.data = data_holder.data.with_columns(
            pl.lit(self.text_to_set).alias(self.col_to_set)
        )


class PolarsAddStaticDataHoldingCenterSwedish(PolarsTransformer):
    col_to_set = "data_holding_centre"
    text_to_set = "Sveriges Meteorologiska och Hydrologiska Institut (SMHI)"

    @staticmethod
    def get_transformer_description() -> str:
        return (
            f"Sets {PolarsAddStaticDataHoldingCenterEnglish.col_to_set} to "
            f"{PolarsAddStaticDataHoldingCenterEnglish.text_to_set}"
        )

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        data_holder.data = data_holder.data.with_columns(
            pl.lit(self.text_to_set).alias(self.col_to_set)
        )
