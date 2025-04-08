import polars as pl

from .base import (
    DataHolderProtocol,
    PolarsDataHolderProtocol,
    PolarsTransformer,
    Transformer,
)


class AddVisitKey(Transformer):
    valid_data_types = ("physicalchemical",)

    @staticmethod
    def get_transformer_description() -> str:
        return "Adds visit key column"

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        boolean = data_holder.data["datetime"] != ""
        data_holder.data.loc[boolean, "visit_key"] = (
            data_holder.data.loc[boolean, "datetime"]
            .apply(lambda x: x.strftime("%Y%m%d_%H%M"))
            .str.cat(data_holder.data.loc[boolean, "platform_code"], "_")
            .str.cat(data_holder.data.loc[boolean, "reported_station_name"], "_")
        )


class PolarsAddVisitKey(PolarsTransformer):
    valid_data_types = ("physicalchemical",)

    @staticmethod
    def get_transformer_description() -> str:
        return "Adds visit key column"

    def _transform(self, data_holder: PolarsDataHolderProtocol) -> None:
        data_holder.data = data_holder.data.with_columns(
            pl.when(pl.col("datetime").is_not_null()).then(
                pl.concat_str(
                    pl.col("datetime").dt.strftime("%Y%m%d_%H%M"),
                    pl.col("platform_code"),
                    pl.col("reported_station_name"),
                    separator="_",
                ).alias("visit_key")
            )
        )
