from abc import abstractmethod

import pandas as pd
from sharkadm.utils import matching_strings

from sharkadm import adm_logger
from .base import Transformer, DataHolderProtocol
from sharkadm import config
from sharkadm.data import archive
from sharkadm.data import DataHolder


class WideToLong(Transformer):
    valid_data_structures = ['column']
    invalid_data_holders = ['ZoobenthosBedaArchiveDataHolder']

    def __init__(self,
                 ignore_containing: str | list[str] | None = None,
                 column_name_parameter: str = 'parameter',
                 column_name_value: str = 'value',
                 column_name_qf: str = 'quality_flag',
                 column_name_unit: str = 'unit',
                 **kwargs
                 ):

        # ignore_containing can be regex
        super().__init__(**kwargs)

        self._ignore_containing = ignore_containing or []
        if type(self._ignore_containing) == str:
            self._ignore_containing = [self._ignore_containing]

        self._column_name_parameter = column_name_parameter
        self._column_name_value = column_name_value
        self._column_name_qf = column_name_qf
        self._column_name_unit = column_name_unit

        self._metadata_columns = []
        self._data_columns = []
        self._qf_col_mapping = {}

    @staticmethod
    def get_transformer_description() -> str:
        return f'Transposes data from column data to row data'

    def _transform(self, data_holder: DataHolder) -> None:
        self._qf_prefix = data_holder.qf_column_prefixes
        self._metadata_columns = []
        self._data_columns = []
        self._qf_col_mapping = {}
        if self._column_name_parameter in data_holder.columns:
            adm_logger.log_transformation(f'Could not transform to row format. {self._column_name_parameter} already in data')
            return
        self._save_metadata_columns(data_holder.data)
        self._save_data_columns(data_holder.data)
        data_holder.data = self._get_transposed_data(data_holder.data)
        self._cleanup(data_holder)

    def _save_metadata_columns(self, df: pd.DataFrame) -> None:
        for col in df.columns:
            if self._ignore(col):
                if not self._associated_qf_col(col, df):
                    self._metadata_columns.append(col)
                    continue
            if self._is_qf_col(col):
                continue
            if self._associated_qf_col(col, df):
                continue
            self._metadata_columns.append(col)

    def _save_data_columns(self, df: pd.DataFrame) -> None:
        for original_col in df.columns:
            col = original_col
            if 'COPY_VARIABLE' in original_col:
                col = col.split('.')[1]
            qcol = self._associated_qf_col(col, df)
            if not qcol and 'COPY_VARIABLE' not in original_col:
                continue
            if self._ignore(col):
                continue
            self._data_columns.append(original_col)
            self._qf_col_mapping[original_col] = qcol
            self._qf_col_mapping[col] = qcol

    def _ignore(self, col: str) -> bool:
        if matching_strings.get_matching_strings([col], self._ignore_containing):
            return True
        return False

    def _is_qf_col(self, col: str) -> bool:
        for prefix in self._qf_prefix:
            if col.startswith(prefix):
                return True
        return False

    def _associated_qf_col(self, col: str, df: pd.DataFrame) -> str:
        for prefix in self._qf_prefix:
            qcol = f'{prefix}{col}'
            if qcol in df.columns:
                return qcol
        return ''

    def _get_transposed_data(self, df: pd.DataFrame) -> pd.DataFrame:
        data = []
        for i, row in df.iterrows():
            meta = list(row[self._metadata_columns].values)
            for col in self._data_columns:
                q_col = self._qf_col_mapping.get(col)
                if not row[col]:
                    # print(f'{col=}')
                    continue
                par = self._get_parameter_name_from_parameter(col)
                value = row[col]
                qf = row.get(q_col)
                if qf is None:
                    adm_logger.log_transformation(f'No quality_flag parameter ({q_col}) found for {par}', level=adm_logger.WARNING)
                    qf = ''
                unit = self._get_unit_from_parameter(col)
                new_row = meta + [par, value, qf, unit]
                data.append(new_row)

        # self.meta = meta
        # self.par = par
        # self.value = value
        # self.qf = qf
        # self.unit = unit
        # self.new_row = new_row
        # self.row = row
        # self.data = data
        self.columns = self._metadata_columns + [self._column_name_parameter,
                                                                           self._column_name_value,
                                                                           self._column_name_qf,
                                                                           self._column_name_unit]
        new_df = pd.DataFrame(data=data, columns=self._metadata_columns + [self._column_name_parameter,
                                                                           self._column_name_value,
                                                                           self._column_name_qf,
                                                                           self._column_name_unit])
        return new_df

    def _cleanup(self, data_holder: DataHolder):
        keep_columns = [col for col in data_holder.data.columns if not col.startswith('COPY_VARIABLE')]
        data_holder.data = data_holder.data[keep_columns]

    @staticmethod
    def _get_unit_from_parameter(par: str) -> str:
        if '.' in par:
            return par.split('.')[-1]
        if '[' in par:
            return par.split('[')[-1].split(']')[0].strip()
        return ''

    @staticmethod
    def _get_parameter_name_from_parameter(par: str) -> str:
        if '.' in par:
            return par.split('.')[1]
        if '[' in par:
            return par.split('[')[0].strip()
        return par

    # def _old_remove_columns(self, data_holder: archive.ArchiveDataHolder) -> None:
    #     for col in ['parameter', 'value', 'unit']:
    #         if col in data_holder.data.columns:
    #             data_holder.data.drop(col, axis=1, inplace=True)
    #
    # def _wide_to_long(self, data_holder: archive.ArchiveDataHolder) -> None:
    #     data_holder.data = data_holder.data.melt(id_vars=[col for col in data_holder.data.columns if not col.startswith('COPY_VARIABLE')],
    #                                              var_name='parameter')
    #
    # def _cleanup(self, data_holder: archive.ArchiveDataHolder) -> None:
    #     data_holder.data['unit'] = data_holder.data['parameter'].apply(self._fix_unit)
    #     data_holder.data['parameter'] = data_holder.data['parameter'].apply(self._fix_parameter)
    #
    # def _fix_unit(self, x) -> str:
    #     return x.split('.')[-1]
    #
    # def _fix_parameter(self, x) -> str:
    #     return x.split('.')[1]
