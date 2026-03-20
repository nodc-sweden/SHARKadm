import polars as pl

from sharkadm.data.archive.archive_data_holder import PolarsArchiveDataHolder
from sharkadm.data.lims import PolarsLimsDataHolder
from sharkadm.sharkadm_logger import adm_logger
from sharkadm.transformers.base import PolarsTransformer


class PolarsAddAnalyseInfo(PolarsTransformer):
    valid_data_holders = (
        "PolarsArchiveDataHolder",
        "PolarsLimsDataHolder",
        "PolarsDvTemplateDataHolder",
    )
    valid_data_structures = ("row",)

    @staticmethod
    def get_transformer_description() -> str:
        return "Adds analyse information to data"

    def _transform(
        self, data_holder: PolarsArchiveDataHolder | PolarsLimsDataHolder
    ) -> None:
        if "parameter" not in data_holder.columns:
            self._log(
                "Can not add analyse info. Data is not in row format.",
                level=adm_logger.ERROR,
            )
            return
        if not hasattr(data_holder, "analyse_info") or data_holder.analyse_info is None:
            self._log(
                "Can not add analyse info. No analyse_info found",
                level=adm_logger.WARNING,
            )
            return

        for par in data_holder.analyse_info.data:
            for info in data_holder.analyse_info.data[par]:
                if (info["VALIDFR"] != "") and (info["VALIDTO"] != ""):
                    mask = [
                        pl.col("datetime") >= info["VALIDFR"],
                        pl.col("datetime") <= info["VALIDTO"],
                        pl.col("parameter") == par,
                    ]
                    missing_date = False
                else:
                    mask = [
                        pl.col("parameter") == par,
                    ]
                    missing_date = True

                for col, value in info.items():
                    if col in ["VALIDFR", "VALIDTO"]:
                        continue
                    if col not in data_holder.data.columns:
                        data_holder.data = data_holder.data.with_columns(
                            pl.lit("").alias(col)
                        )
                    data_holder.data = data_holder.data.with_columns(
                        pl.when(mask)
                        .then(pl.lit("") if missing_date else pl.lit(value))
                        .otherwise(pl.col(col))
                        .alias(col)
                    )
