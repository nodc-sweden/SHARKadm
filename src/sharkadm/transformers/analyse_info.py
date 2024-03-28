# -*- coding: utf-8 -*-

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

        for (par, dtime), df in data_holder.data.groupby(['parameter', 'datetime']):
            value = data_holder.analyse_info.get(par, par, dtime.date()) or ''
            data_holder.data.loc[df.index, par] = value



        # for par in data_holder.analyse_info.parameters:
        #     boolean = data_holder.data['parameter'] == par
        #     date_data = data_holder.data['datetime'].apply(lambda d: d.date())
        #     for date in set(date_data):
        #         boolean = boolean & (date_data == date)
        #         for col in data_holder.analyse_info.columns:
        #             value = data_holder.analyse_info.get(par, col, date) or ''
        #             adm_logger.log_transformation('Adding analyse info for parameter', add=par)
        #             data_holder.data.loc[boolean, col] = value


