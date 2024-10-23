import pandas as pd

from sharkadm import adm_logger
from .base import Transformer, DataHolderProtocol


class old_AddReportedScientificName(Transformer):
    invalid_data_types = ['physicalchemical', 'chlorophyll', 'bacterioplankton']
    col_to_set = 'reported_scientific_name'
    check_columns = ['scientific_name', 'dyntaxa_id']

    @staticmethod
    def get_transformer_description() -> str:
        return f'Adds reported_scientific_name if not given. Information is taken from either scientific_name or ' \
               f'dyntaxa_id. '

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        self.check_columns = [col for col in self.check_columns if col in data_holder.data.columns]
        if self.col_to_set not in data_holder.data.columns:
            data_holder.data[self.col_to_set] = ''
        elif all(data_holder.data[self.col_to_set]):
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


class SetScientificNameFromDyntaxaScientificName(Transformer):
    invalid_data_types = ['physicalchemical', 'chlorophyll']
    source_col = 'dyntaxa_scientific_name'
    col_to_set = 'scientific_name'

    @staticmethod
    def get_transformer_description() -> str:
        return f'Sets {SetScientificNameFromDyntaxaScientificName.col_to_set} from {SetScientificNameFromDyntaxaScientificName.source_col} '

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data[self.col_to_set] = data_holder.data[self.source_col]



