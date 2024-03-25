import pandas as pd

from sharkadm import adm_logger
from .base import Transformer, DataHolderProtocol

try:
    import nodc_dyntaxa
except ModuleNotFoundError as e:
    module_name = str(e).split("'")[-2]
    adm_logger.log_workflow(f'Could not import package "{module_name}" in module {__name__}. You need to install this dependency if you want to use this module.', level=adm_logger.WARNING)


class AddDyntaxaScientificName(Transformer):
    invalid_data_types = ['physicalchemical', 'chlorophyll']
    col_to_set = 'dyntaxa_scientific_name'
    source_col = 'reported_scientific_name'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.translate_dyntaxa = nodc_dyntaxa.get_translate_dyntaxa_object()

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


class AddDyntaxaId(Transformer):
    invalid_data_types = ['physicalchemical', 'chlorophyll']
    col_to_set = 'dyntaxa_id'
    source_col = 'scientific_name'
    mapped_dyntaxa = dict()

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.dyntaxa_id = nodc_dyntaxa.get_dyntaxa_taxon_object()

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


# class AddDyntaxaIdPolars(Transformer):
#     col_to_set = 'dyntaxa_id'
#     source_col = 'scientific_name'
#     dyntaxa_id = nodc_dyntaxa.get_dyntaxa_taxon_object()
#
#     @staticmethod
#     def get_transformer_description() -> str:
#         return f'Adds dyntaxa_id.'
#
#     def _transform(self, data_holder: DataHolderProtocol) -> None:
#         if self.col_to_set not in data_holder.data.columns:
#             adm_logger.log_transformation(f'Adding column {self.col_to_set} in {self.__class__.__name__}', level='debug')
#             data_holder.data[self.col_to_set] = ''
#         if all(data_holder.data[self.col_to_set]):
#             adm_logger.log_transformation(f'All {self.col_to_set} reported. Will skip {self.__class__.__name__}.')
#             return
#
#         df = pl.from_pandas(data_holder.data)
#         for source_name in df[self.source_col].unique():
#             new_dyntaxa = self.dyntaxa_id.get(source_name)
#             d = df.filter(pl.col(self.source_col) == source_name)
#             nr_rows = len(d)
#             nr_has_dyntaxa = len([item for item in d[self.col_to_set] if item])
#             if nr_has_dyntaxa != nr_rows:
#                 adm_logger.log_transformation(f'{source_name}: {nr_has_dyntaxa} out of {nr_rows} rows already have'
#                                               f' {self.col_to_set}')
#
#
#             # if not new_dyntaxa:
#             #     if current_dyntaxa:
#             #         adm_logger.log_transformation(
#             #             f'No dyntaxaId found for {source_name}. Keeping old id: {current_dyntaxa}',
#             #             level='warning')
#             #         return current_dyntaxa
#             #     adm_logger.log_transformation(f'No dyntaxaId found for {source_name}', level='warning')
#
#
#         data_holder.data[self.col_to_set] = data_holder.data.apply(lambda row: self._add(row), axis=1)
#
#     def _add(self, row: pd.Series) -> str:
#         current_dyntaxa = row[self.col_to_set].strip()
#         source_name = row[self.source_col].strip()
#         new_dyntaxa = self.mapped_dyntaxa.setdefault(source_name, self.dyntaxa_id.get(source_name))
#         if not new_dyntaxa:
#             if current_dyntaxa:
#                 adm_logger.log_transformation(f'No dyntaxaId found for {source_name}. Keeping old id: {current_dyntaxa}',
#                                               level='warning')
#                 return current_dyntaxa
#             adm_logger.log_transformation(f'No dyntaxaId found for {source_name}', level='warning')
#             return ''
#         if current_dyntaxa and new_dyntaxa != current_dyntaxa:
#             adm_logger.log_transformation(f'Replacing dyntaxaId: {current_dyntaxa} -> {new_dyntaxa}', level='warning')
#             return new_dyntaxa
#         adm_logger.log_transformation(f'Addding dyntaxaId {new_dyntaxa} translated from {source_name}')
#         return new_dyntaxa

