import pandas as pd

from sharkadm import adm_logger
from .base import Transformer, DataHolderProtocol


class SetScientificNameFromReportedScientificName(Transformer):
    valid_data_types = ['plankton_imaging']
    source_col = 'reported_scientific_name'
    col_to_set = 'scientific_name'

    @staticmethod
    def get_transformer_description() -> str:
        return f'Sets {SetScientificNameFromReportedScientificName.col_to_set} from {SetScientificNameFromReportedScientificName.source_col} '

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data[self.col_to_set] = data_holder.data[self.source_col]


class SetScientificNameFromDyntaxaScientificName(Transformer):
    invalid_data_types = ['physicalchemical', 'chlorophyll']
    source_col = 'dyntaxa_scientific_name'
    col_to_set = 'scientific_name'

    @staticmethod
    def get_transformer_description() -> str:
        return f'Sets {SetScientificNameFromDyntaxaScientificName.col_to_set} from {SetScientificNameFromDyntaxaScientificName.source_col} '

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data[self.col_to_set] = data_holder.data[self.source_col]



