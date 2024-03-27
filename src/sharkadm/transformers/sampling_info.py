# -*- coding: utf-8 -*-

import pathlib
import sys

from .base import Transformer, DataHolderProtocol
from sharkadm.data.archive import ArchiveDataHolder
from sharkadm import adm_logger
from sharkadm.utils import yaml_data
from sharkadm.data.archive.sampling_info import SamplingInfo


class AddSamplingInfo(Transformer):
    valid_data_holders = ['ArchiveDataHolder']

    @staticmethod
    def get_transformer_description() -> str:
        return f'Adds sampling information to data'

    def _transform(self, data_holder: ArchiveDataHolder) -> None:
        if 'parameter' not in data_holder.columns:
            adm_logger.log_transformation('Can not add sampling info. Data is not in row format.', level=adm_logger.ERROR)
            return
        for par in data_holder.sampling_info.parameters:
            boolean = data_holder.data['parameter'] == par
            date_data = data_holder.data['datetime'].apply(lambda d: d.date())
            for date in set(date_data):
                boolean = boolean & (date_data == date)
                for col in data_holder.sampling_info.columns:
                    value = data_holder.sampling_info.get(par, col, date) or ''
                    data_holder.data.loc[boolean, col] = value
                    adm_logger.log_transformation('Adding sampling info for parameter', add=par)


