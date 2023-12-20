import pandas as pd
from SHARKadm import adm_logger

from .base import Transformer, DataHolderProtocol
from dyntaxa import get_translate_dyntaxa_object


class AddReportedScientificName(Transformer):
    col_to_set = 'reported_scientific_name'
    check_columns = ['scientific_name', 'dyntaxa_id']

    @staticmethod
    def get_transformer_description() -> str:
        return f'Adds reported_scientific_name if not given. Information is taken from either scientific_name or ' \
               f'dyntaxa_id. '

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        self.check_columns = [col for col in self.check_columns if col in data_holder.data.columns]
        if all(data_holder.data[self.col_to_set]):
            adm_logger.log_transformation(f'All {self.col_to_set} reported. Will skip {self.__class__.__name__}.')
            return
        if not self.check_columns:
            adm_logger.log_transformation(f'Could not add {self.col_to_set}. No source columns available. There might '
                                          f'still be data in {self.col_to_set}!')
            return
        data_holder.data[self.col_to_set] = data_holder.data.apply(lambda row: self._add(row), axis=1)

    def _add(self, row: pd.Series) -> str:
        if row[self.col_to_set].strip():
            return row[self.col_to_set].strip()
        for col in self.check_columns:
            if not row[col]:
                continue
            adm_logger.log_transformation(f'Setting {self.col_to_set} from {col}')
            return row[col]
        adm_logger.log_transformation(f'Not able to set {self.col_to_set}!', level='warning')
        return row[self.col_to_set]


class AddScientificNameTranslatedWithDyntaxa(Transformer):
    col_to_set = 'scientific_name'
    source_col = 'reported_scientific_name'
    translate_dyntaxa = get_translate_dyntaxa_object()

    @staticmethod
    def get_transformer_description() -> str:
        return f'Adds scientific_name translated from dyntaxa.'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        if self.col_to_set not in data_holder.data.columns:
            adm_logger.log_transformation(f'Adding column {self.col_to_set} in {self.__class__.__name__}',
                                          level='debug')
            data_holder.data[self.col_to_set] = ''
        elif all(data_holder.data[self.col_to_set]):
            adm_logger.log_transformation(f'All {self.col_to_set} reported. Will skip {self.__class__.__name__}.')
            return
        data_holder.data[self.col_to_set] = data_holder.data.apply(lambda row: self._add(row), axis=1)

    def _add(self, row: pd.Series) -> str:
        current_name = row[self.col_to_set].strip()
        source_name = row[self.source_col].strip()
        new_name = self.translate_dyntaxa.get(source_name)
        if new_name:
            if not current_name:
                adm_logger.log_transformation(f'Translated: {source_name} -> {new_name}')
            elif current_name != new_name:
                adm_logger.log_transformation(f'Translated: {source_name} -> {new_name}. Replacing: {current_name}')
            return new_name
        else:
            if current_name and current_name != source_name:
                adm_logger.log_transformation(f'No translation and {source_name} ({self.source_col}) is not the '
                                              f'same as {current_name} ({self.col_to_set})')
            return source_name


