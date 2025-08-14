# -*- coding: utf-8 -*-

import polars as pl

from sharkadm.data import LimsDataHolder
from sharkadm.data.archive import ArchiveDataHolder, PolarsArchiveDataHolder
from sharkadm.data.dv_template import PolarsDvTemplateDataHolder
from sharkadm.data.lims import PolarsLimsDataHolder
from sharkadm.sharkadm_logger import adm_logger
from sharkadm.transformers.base import PolarsTransformer, Transformer


class AddSamplingInfo(Transformer):
    valid_data_holders = ("ArchiveDataHolder", "LimsDataHolder", "DvTemplateDataHolder")

    @staticmethod
    def get_transformer_description() -> str:
        return "Adds sampling information to data"

    def _transform(self, data_holder: ArchiveDataHolder | LimsDataHolder) -> None:
        if "parameter" not in data_holder.columns:
            self._log(
                "Can not add sampling info. Data is not in row format.",
                level=adm_logger.ERROR,
            )
            return
        pars = data_holder.sampling_info.parameters
        i = 0
        for (par, dtime), df in data_holder.data.groupby(["parameter", "datetime"]):
            if not dtime:
                continue
            if par not in pars:
                continue
            info = data_holder.sampling_info.get_info(par, dtime.date())
            i += 1
            for col in data_holder.sampling_info.columns:
                if col in ["VALIDFR", "VALIDTO"]:
                    continue
                if col not in data_holder.data.columns:
                    data_holder.data[col] = ""
                data_holder.data.loc[df.index, col] = info.get(col, "")


class PolarsAddSamplingInfo(PolarsTransformer):
    valid_data_holders = (
        "PolarsArchiveDataHolder",
        "PolarsLimsDataHolder",
        "PolarsDvTemplateDataHolder",
    )

    @staticmethod
    def get_transformer_description() -> str:
        return "Adds sampling information to data"

    def _transform(
        self,
        data_holder: (
            PolarsArchiveDataHolder | PolarsLimsDataHolder | PolarsDvTemplateDataHolder
        ),
    ) -> None:
        if "parameter" not in data_holder.columns:
            self._log(
                "Can not add sampling info. Data is not in row format.",
                level=adm_logger.ERROR,
            )
            return
        pars = data_holder.sampling_info.parameters
        i = 0
        for (par, dtime), df in data_holder.data.group_by(["parameter", "datetime"]):
            if not dtime:
                continue
            if par not in pars:
                continue
            info = data_holder.sampling_info.get_info(par, dtime.date())
            i += 1
            for col in data_holder.sampling_info.columns:
                if col in ["VALIDFR", "VALIDTO"]:
                    continue
                if col not in data_holder.data.columns:
                    self._add_empty_col(data_holder, col)
                data_holder.data = data_holder.data.with_columns(
                    pl.when(pl.col("parameter") == par, pl.col("datetime") == dtime)
                    .then(pl.lit(info.get(col, "")))
                    .otherwise(pl.col(col))
                    .alias(col)
                )
