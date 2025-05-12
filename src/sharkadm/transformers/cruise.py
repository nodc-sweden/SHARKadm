from .base import DataHolderProtocol, Transformer
from ..data import PolarsDataHolder
import polars as pl


class AddCruiseId(Transformer):
    valid_data_holders = ("LimsDataHolder",)

    @staticmethod
    def get_transformer_description() -> str:
        return "Adds cruise id column"

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data["cruise_id"] = (
            data_holder.data["visit_year"]
            .str.cat(data_holder.data["platform_code"], "_")
            .str.cat(data_holder.data["expedition_id"], "_")
        )


class PolarsAddCruiseId(Transformer):
    valid_data_holders = ("PolarsLimsDataHolder",)

    @staticmethod
    def get_transformer_description() -> str:
        return "Adds cruise id column"

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        data_holder.data = data_holder.data.with_columns(
            pl.concat_st([
                pl.col("visit_year"),
                pl.col("platform_code"),
                pl.col("expedition_id"),
            ], separator="_")
        )
