import pandas as pd
from sharkadm import adm_logger
import nodc_dyntaxa

from .base import Transformer, DataHolderProtocol


class AddRedList(Transformer):
    invalid_data_types = ['physicalchemical', 'chlorophyll']

    col_to_set = 'red_listed'
    source_cols = ['dyntaxa_id', 'scientific_name', 'reported_scientific_name']
    red_list_obj = nodc_dyntaxa.get_red_list_object()
    mapped = dict()

    @staticmethod
    def get_transformer_description() -> str:
        return f'Adds info if red listed. Red listed species are marked with Y'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        if self.col_to_set not in data_holder.data.columns:
            adm_logger.log_transformation(f'Adding column {self.col_to_set} in {self.__class__.__name__}',
                                          level=adm_logger.DEBUG)
            data_holder.data[self.col_to_set] = ''
        data_holder.data[self.col_to_set] = data_holder.data.apply(lambda row: self._add(row), axis=1)

    def _add(self, row: pd.Series) -> str:
        for col in self.source_cols:
            value = row[col]
            if not value.strip():
                continue
            info = self.mapped.get(value)
            if info:
                return 'Y'
            info = self.red_list_obj.get_info(value)
            if info:
                self.mapped[value] = True
                adm_logger.log_transformation(f'{value} is marked as red listed', level=adm_logger.INFO)
                return 'Y'
        return ''
