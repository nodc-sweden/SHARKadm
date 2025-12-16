# -*- coding: utf-8 -*-

import polars as pl

from sharkadm.data.archive import ArchiveDataHolder
from sharkadm.data.archive.archive_data_holder import PolarsArchiveDataHolder
from sharkadm.data.lims import LimsDataHolder, PolarsLimsDataHolder
from sharkadm.sharkadm_logger import adm_logger
from sharkadm.transformers.base import PolarsTransformer, Transformer


class AddAnalyseInfo(Transformer):
    valid_data_holders = ("ArchiveDataHolder", "LimsDataHolder", "DvTemplateDataHolder")

    @staticmethod
    def get_transformer_description() -> str:
        return "Adds analyse information to data"

    def _transform(self, data_holder: ArchiveDataHolder | LimsDataHolder) -> None:
        if "parameter" not in data_holder.columns:
            self._log(
                "Can not add analyse info. Data is not in row format.",
                level=adm_logger.ERROR,
            )
            return
        pars = data_holder.analyse_info.parameters
        for (par, dtime), df in data_holder.data.groupby(["parameter", "datetime"]):
            if not dtime:
                continue
            if par not in pars:
                self._log(
                    f'No analyse info for parameter "{par}"', level=adm_logger.WARNING
                )
                continue
            info = data_holder.analyse_info.get_info(par, dtime.date())
            for col in data_holder.analyse_info.columns:
                if col in ["VALIDFR", "VALIDTO"]:
                    continue
                if col not in data_holder.data.columns:
                    data_holder.data[col] = ""
                data_holder.data.loc[df.index, col] = info.get(col, "")


class PolarsAddAnalyseInfoOld(PolarsTransformer):
    valid_data_holders = (
        "PolarsArchiveDataHolder",
        "PolarsLimsDataHolder",
        "PolarsDvTemplateDataHolder",
    )

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

        parameters = data_holder.analyse_info.parameters
        for (parameter, dtime), df in data_holder.data.group_by(
            ["parameter", "datetime"]
        ):
            if not dtime:
                continue
            if parameter not in parameters:
                self._log(
                    f"No analyse info for parameter '{parameter}'",
                    level=adm_logger.WARNING,
                )
                continue
            info = data_holder.analyse_info.get_info(parameter, dtime.date())

            analyse_info_columns = [
                column
                for column in data_holder.analyse_info.columns
                if column not in ["VALIDFR", "VALIDTO"]
            ]
            data_holder.data = data_holder.data.with_columns(
                [
                    pl.lit("").alias(col)
                    for col in analyse_info_columns
                    if col not in data_holder.data
                ]
            )

            for column in analyse_info_columns:
                data_holder.data = data_holder.data.with_columns(
                    pl.when(
                        (pl.col("parameter") == parameter) & (pl.col("datetime") == dtime)
                    )
                    .then(pl.lit(info.get(column, "")))
                    .otherwise(pl.col(column))
                    .alias(column)
                )


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
                mask = [
                    pl.col("datetime") >= info["VALIDFR"],
                    pl.col("datetime") <= info["VALIDTO"],
                    pl.col("parameter") == par,
                ]
                for col, value in info.items():
                    if col in ["VALIDFR", "VALIDTO"]:
                        continue
                    if col not in data_holder.data.columns:
                        data_holder.data = data_holder.data.with_columns(
                            pl.lit("").alias(col)
                        )
                    data_holder.data = data_holder.data.with_columns(
                        pl.when(mask)
                        .then(pl.lit(value))
                        .otherwise(pl.col(col))
                        .alias(col)
                    )
