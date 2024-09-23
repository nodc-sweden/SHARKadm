# -*- coding: utf-8 -*-

import pathlib
import sys

from .base import Transformer, DataHolderProtocol
from sharkadm.data.archive import ArchiveDataHolder
from sharkadm import adm_logger
from sharkadm.utils import yaml_data
from sharkadm.data.archive.sampling_info import SamplingInfo
from ..data import LimsDataHolder


class AddSamplingInfo(Transformer):
    valid_data_holders = ['ArchiveDataHolder', 'LimsDataHolder', 'DvTemplateDataHolder']

    @staticmethod
    def get_transformer_description() -> str:
        return f'Adds sampling information to data'

    def _transform(self, data_holder: ArchiveDataHolder | LimsDataHolder) -> None:
        if 'parameter' not in data_holder.columns:
            adm_logger.log_transformation('Can not add sampling info. Data is not in row format.', level=adm_logger.ERROR)
            return
        pars = data_holder.sampling_info.parameters
        i = 0
        for (par, dtime), df in data_holder.data.groupby(['parameter', 'datetime']):
            if not dtime:
                continue
            if par not in pars:
                continue
            info = data_holder.sampling_info.get_info(par, dtime.date())
            i += 1
            for col in data_holder.sampling_info.columns:
                if col in ['VALIDFR', 'VALIDTO']:
                    continue
                if col not in data_holder.data.columns:
                    data_holder.data[col] = ''
                data_holder.data.loc[df.index, col] = info.get(col, '')
