from abc import abstractmethod

import pandas as pd

from sharkadm import adm_logger
from .base import Transformer, DataHolderProtocol
from sharkadm import config
from sharkadm.data import archive


class CopyVariable(Transformer):
    valid_data_holders = archive.get_archive_data_holder_names()

    @staticmethod
    def get_transformer_description() -> str:
        return f'Copies COPY_VARIABLE columns to row.'

    def _transform(self, data_holder: archive.ArchiveDataHolder) -> None:
        self._remove_columns(data_holder)
        self._wide_to_long(data_holder)
        self._cleanup(data_holder)

    def _remove_columns(self, data_holder: archive.ArchiveDataHolder) -> None:
        for col in ['parameter', 'value', 'unit']:
            if col in data_holder.data.columns:
                data_holder.data.drop(col, axis=1, inplace=True)

    def _wide_to_long(self, data_holder: archive.ArchiveDataHolder) -> None:
        data_holder.data = data_holder.data.melt(id_vars=[col for col in data_holder.data.columns if not col.startswith('COPY_VARIABLE')],
                                                 var_name='parameter')

    def _cleanup(self, data_holder: archive.ArchiveDataHolder) -> None:
        data_holder.data['unit'] = data_holder.data['parameter'].apply(self._fix_unit)
        data_holder.data['parameter'] = data_holder.data['parameter'].apply(self._fix_parameter)
        keep_columns = [col for col in data_holder.data.columns if not col.startswith('COPY_VARIABLE')]
        data_holder.data = data_holder.data[keep_columns]

    def _fix_unit(self, x) -> str:
        return x.split('.')[-1]

    def _fix_parameter(self, x) -> str:
        # return x.replace('COPY_VARIABLE', '')
        return x.split('.')[1]
