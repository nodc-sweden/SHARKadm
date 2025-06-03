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


class PolarsAddPhysicalChemicalKey(PolarsTransformer):
    valid_data_types = ("physicalchemical", "Profile")
    col_to_set = "key"

    @staticmethod
    def get_transformer_description() -> str:
        return "Adds 'original ' key column: <YEAR>_<COUNTRY>_<SHIP>_<SERNO>"

    def _transform(self, data_holder: PolarsDataHolderProtocol) -> None:
        data_holder.data = data_holder.data.with_columns(
            pl.concat_str(
                pl.col("reported_station_name"),
                pl.col("visit_year"),
                pl.col("platform_code").str.slice(0, 2),
                pl.col("platform_code").str.slice(2, 4),
                pl.col("visit_id"),
                separator="_",
            ).alias(self.col_to_set)
        )
