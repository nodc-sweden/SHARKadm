# -*- coding: utf-8 -*-
import datetime

from sharkadm import adm_logger
from sharkadm.data.archive import ArchiveDataHolder
from sharkadm.data.lims import LimsDataHolder
from sharkadm.transformers.base import Transformer
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
                continue
            info = data_holder.analyse_info.get_info(par, dtime.date())
            for col in data_holder.analyse_info.columns:
                if col in ['VALIDFR', 'VALIDTO']:
                    continue
                data_holder.data.loc[df.index, col] = info.get(col, '')
