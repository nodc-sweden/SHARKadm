# -*- coding: utf-8 -*-


from sharkadm.data.archive import ArchiveDataHolder
from sharkadm.sharkadm_logger import adm_logger

from ..data import LimsDataHolder
from .base import Transformer


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
