# -*- coding: utf-8 -*-
import datetime

import polars as pl

from sharkadm import adm_logger
from sharkadm.data.archive import ArchiveDataHolder
from sharkadm.data.archive.archive_data_holder import PolarsArchiveDataHolder
from sharkadm.data.lims import LimsDataHolder, PolarsLimsDataHolder
from sharkadm.transformers.base import Transformer, PolarsTransformer
from typing import Protocol


class AddAnalyseInfo(Transformer):
    valid_data_holders = ['ArchiveDataHolder', 'LimsDataHolder', 'DvTemplateDataHolder']

    @staticmethod
    def get_transformer_description() -> str:
        return f'Adds analyse information to data'

    def _transform(self, data_holder: ArchiveDataHolder | LimsDataHolder) -> None:
        if 'parameter' not in data_holder.columns:
            adm_logger.log_transformation('Can not add analyse info. Data is not in row format.', level=adm_logger.ERROR)
            return
        pars = data_holder.analyse_info.parameters
        for (par, dtime), df in data_holder.data.groupby(['parameter', 'datetime']):
            if not dtime:
                continue
            if par not in pars:
                adm_logger.log_transformation(f'No analyse info for parameter "{par}"', level=adm_logger.WARNING)
                continue
            info = data_holder.analyse_info.get_info(par, dtime.date())
            for col in data_holder.analyse_info.columns:
                if col in ['VALIDFR', 'VALIDTO']:
                    continue
                if col not in data_holder.data.columns:
                    data_holder.data[col] = ''
                data_holder.data.loc[df.index, col] = info.get(col, '')


class PolarsAddAnalyseInfo(PolarsTransformer):
    valid_data_holders = ["ArchiveDataHolder", "LimsDataHolder", "DvTemplateDataHolder"]

    @staticmethod
    def get_transformer_description() -> str:
        return "Adds analyse information to data"

    def _transform(
        self, data_holder: PolarsArchiveDataHolder | PolarsLimsDataHolder
    ) -> None:
        if "parameter" not in data_holder.columns:
            adm_logger.log_transformation(
                "Can not add analyse info. Data is not in row format.",
                level=adm_logger.ERROR,
            )
            return

        parameters = data_holder.analyse_info.parameters
        for (parameter, dtime), df in data_holder.data.group_by(
            ["parameter", "datetime"]
        ):
            if not dtime:
                continue
            if parameter not in parameters:
                adm_logger.log_transformation(
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
            for column in analyse_info_columns:
                data_holder.data = data_holder.data.with_columns(
                    pl.when(
                        pl.col("parameter").str == parameter
                        and pl.col("datetime").dt == dtime
                    )
                    .then(pl.lit(info.get(column, "")).alias(column))
                    .when(column not in data_holder.analyse_info.columns)
                    .then(pl.lit("").alias(column))
                )


