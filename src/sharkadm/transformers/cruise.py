import polars as pl

from ..data import PolarsDataHolder
from . import PolarsTransformer


class PolarsAddCruiseId(PolarsTransformer):
    valid_data_holders = ("PolarsLimsDataHolder",)

    @staticmethod
    def get_transformer_description() -> str:
        return "Adds cruise id column"

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        data_holder.data = data_holder.data.with_columns(
            pl.concat_st(
                [
                    pl.col("visit_year"),
                    pl.col("platform_code"),
                    pl.col("expedition_id"),
                ],
                separator="_",
            )
        )
