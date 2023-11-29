import pandas as pd
from SHARKadm import adm_logger
from micro import dyntaxa
import polars as pl

from .base import Transformer, DataHolderProtocol


class AddDyntaxaId(Transformer):
    col_to_set = 'dyntaxa_id'
    source_col = 'scientific_name'
    dyntaxa_id = dyntaxa.get_dyntaxa_taxon_object()
    mapped_dyntaxa = dict()

    @staticmethod
    def get_transformer_description() -> str:
        return f'Adds dyntaxa_id.'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        if self.col_to_set not in data_holder.data.columns:
            adm_logger.log_transformation(f'Adding column {self.col_to_set} in {self.__class__.__name__}', level='debug')
            data_holder.data[self.col_to_set] = ''
        if all(data_holder.data[self.col_to_set]):
            adm_logger.log_transformation(f'All {self.col_to_set} reported. Will skip {self.__class__.__name__}.')
            return
        data_holder.data[self.col_to_set] = data_holder.data.apply(lambda row: self._add(row), axis=1)

    def _add(self, row: pd.Series) -> str:
        current_dyntaxa = row[self.col_to_set].strip()
        source_name = row[self.source_col].strip()
        new_dyntaxa = self.mapped_dyntaxa.setdefault(source_name, self.dyntaxa_id.get(source_name))
        if not new_dyntaxa:
            if current_dyntaxa:
                adm_logger.log_transformation(f'No dyntaxaId found for {source_name}. Keeping old id: {current_dyntaxa}',
                                              level='warning')
                return current_dyntaxa
            adm_logger.log_transformation(f'No dyntaxaId found for {source_name}', level='warning')
            return ''
        if current_dyntaxa and new_dyntaxa != current_dyntaxa:
            adm_logger.log_transformation(f'Replacing dyntaxaId: {current_dyntaxa} -> {new_dyntaxa}', level='warning')
            return new_dyntaxa
        adm_logger.log_transformation(f'Addding dyntaxaId {new_dyntaxa} translated from {source_name}')
        return new_dyntaxa


class AddDyntaxaIdPolars(Transformer):
    col_to_set = 'dyntaxa_id'
    source_col = 'scientific_name'
    dyntaxa_id = dyntaxa.get_dyntaxa_taxon_object()

    @staticmethod
    def get_transformer_description() -> str:
        return f'Adds dyntaxa_id.'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        if self.col_to_set not in data_holder.data.columns:
            adm_logger.log_transformation(f'Adding column {self.col_to_set} in {self.__class__.__name__}', level='debug')
            data_holder.data[self.col_to_set] = ''
        if all(data_holder.data[self.col_to_set]):
            adm_logger.log_transformation(f'All {self.col_to_set} reported. Will skip {self.__class__.__name__}.')
            return

        df = pl.from_pandas(data_holder.data)
        for source_name in df[self.source_col].unique():
            new_dyntaxa = self.dyntaxa_id.get(source_name)
            d = df.filter(pl.col(self.source_col) == source_name)
            nr_rows = len(d)
            nr_has_dyntaxa = len([item for item in d[self.col_to_set] if item])
            if nr_has_dyntaxa != nr_rows:
                adm_logger.log_transformation(f'{source_name}: {nr_has_dyntaxa} out of {nr_rows} rows already have'
                                              f' {self.col_to_set}')
                

            # if not new_dyntaxa:
            #     if current_dyntaxa:
            #         adm_logger.log_transformation(
            #             f'No dyntaxaId found for {source_name}. Keeping old id: {current_dyntaxa}',
            #             level='warning')
            #         return current_dyntaxa
            #     adm_logger.log_transformation(f'No dyntaxaId found for {source_name}', level='warning')


        data_holder.data[self.col_to_set] = data_holder.data.apply(lambda row: self._add(row), axis=1)

    def _add(self, row: pd.Series) -> str:
        current_dyntaxa = row[self.col_to_set].strip()
        source_name = row[self.source_col].strip()
        new_dyntaxa = self.mapped_dyntaxa.setdefault(source_name, self.dyntaxa_id.get(source_name))
        if not new_dyntaxa:
            if current_dyntaxa:
                adm_logger.log_transformation(f'No dyntaxaId found for {source_name}. Keeping old id: {current_dyntaxa}',
                                              level='warning')
                return current_dyntaxa
            adm_logger.log_transformation(f'No dyntaxaId found for {source_name}', level='warning')
            return ''
        if current_dyntaxa and new_dyntaxa != current_dyntaxa:
            adm_logger.log_transformation(f'Replacing dyntaxaId: {current_dyntaxa} -> {new_dyntaxa}', level='warning')
            return new_dyntaxa
        adm_logger.log_transformation(f'Addding dyntaxaId {new_dyntaxa} translated from {source_name}')
        return new_dyntaxa

