import pandas as pd

from sharkadm import adm_logger
from .base import Transformer, DataHolderProtocol

try:
    import nodc_dyntaxa
except ModuleNotFoundError as e:
    module_name = str(e).split("'")[-2]
    adm_logger.log_workflow(f'Could not import package "{module_name}" in module {__name__}. You need to install this dependency if you want to use this module.', level=adm_logger.WARNING)


# class MoveDyntaxaIdInReportedScientificNameToDyntaxaId(Transformer):
#     source_col = 'reported_scientific_name'
#     col_to_set = 'dyntaxa_id'
#
#     @staticmethod
#     def get_transformer_description() -> str:
#         return (f'Moves {MoveDyntaxaIdInReportedScientificNameToDyntaxaId.source_col} to '
#                 f'{MoveDyntaxaIdInReportedScientificNameToDyntaxaId.col_to_set} if identified as a dyntaxa_id.')
#
#     def _transform(self, data_holder: DataHolderProtocol) -> None:
#         for _id in set(data_holder.data[self.source_col]):
#             if not _id.isdigit():
#                 continue
#             adm_logger.log_transformation(
#                 f'Moving dyntaxa_id from {self.source_col} to {self.col_to_set}', add=_id, level='warning')
#             boolean = data_holder.data[self.source_col] == _id
#             data_holder.data.loc[boolean, self.col_to_set] = _id


class AddReportedDyntaxaId(Transformer):
    invalid_data_types = ['physicalchemical', 'chlorophyll']
    col_to_set = 'reported_dyntaxa_id'
    source_col = 'dyntaxa_id'

    @staticmethod
    def get_transformer_description() -> str:
        return f'Adds reported_dyntaxa_id from dyntaxa_id if not given.'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        if self.source_col not in data_holder.data.columns:
            adm_logger.log_transformation(f'No source column {self.source_col}. Setting empty column {self.col_to_set}',
                                          level='debug')
            data_holder.data[self.col_to_set] = ''
            return
        if self.col_to_set in data_holder.data.columns:
            adm_logger.log_transformation(f'Column {self.col_to_set} already in data. Will not add',
                                          level='debug')
            return

        data_holder.data[self.col_to_set] = data_holder.data[self.source_col]


class AddTranslatedDyntaxaScientificName(Transformer):
    invalid_data_types = ['physicalchemical', 'chlorophyll']
    source_col = 'reported_scientific_name'
    col_to_set = 'translate_dyntaxa_scientific_name'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.translate_dyntaxa = nodc_dyntaxa.get_translate_dyntaxa_object()

    @staticmethod
    def get_transformer_description() -> str:
        return f'Adds {AddTranslatedDyntaxaScientificName.col_to_set} translated from nodc_dyntaxa. Source column is {AddTranslatedDyntaxaScientificName.source_col}'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data[self.col_to_set] = ''
        unique_scientific_names = set(data_holder.data[self.source_col])
        for name in unique_scientific_names:
            if name.isdigit():
                adm_logger.log_transformation(f'{self.source_col} seems to be a dyntaxa_id {name}. Will not translate.', level='warning')
                continue
            new_name = self.translate_dyntaxa.get(name)
            if not new_name:
                continue
            adm_logger.log_transformation(f'Translated: {name} -> {new_name}')
            boolean = data_holder.data[self.source_col] == name
            data_holder.data.loc[boolean, self.col_to_set] = new_name
        # data_holder.data[self.col_to_set] = data_holder.data.apply(lambda row: self._add(row), axis=1)

    # def _add(self, row: pd.Series) -> str:
    #     source_name = row[self.source_col].strip()
    #     new_name = self.translate_dyntaxa.get(source_name)
    #     if new_name:
    #         if new_name != source_name:
    #             adm_logger.log_transformation(f'Translated: {source_name} -> {new_name}')
    #         return new_name
    #     return source_name


class AddDyntaxaId(Transformer):
    invalid_data_types = ['physicalchemical', 'chlorophyll']
    col_to_set = 'dyntaxa_id'
    source_col = 'translate_dyntaxa_scientific_name'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._dyntaxa_id = nodc_dyntaxa.get_dyntaxa_taxon_object()

    @staticmethod
    def get_transformer_description() -> str:
        return f'Adds {AddDyntaxaId.col_to_set} translated from nodc_dyntaxa. Source column is {AddDyntaxaId.source_col}'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        if self.source_col not in data_holder.data.columns:
            adm_logger.log_transformation(f'Could not add column {self.col_to_set}. Source column {self.source_col} not in data.', level='error')
            return
        if self.col_to_set not in data_holder.data.columns:
            adm_logger.log_transformation(f'Adding empty column {self.col_to_set}', level='debug')
            data_holder.data[self.col_to_set] = ''

        for name, df in data_holder.data.groupby(self.source_col):
            if not str(name).strip():
                adm_logger.log_transformation(f'Missing {self.source_col}, {len(df)} rows.', level='warning')
                continue
            dyntaxa = self._dyntaxa_id.get(str(name))
            if not dyntaxa:
                adm_logger.log_transformation(f'No {self.col_to_set} found for {name}, {len(df)} rows.', level='warning')
                continue
            index = data_holder.data[self.source_col] == name
            adm_logger.log_transformation(f'Adding {self.col_to_set} {dyntaxa} translated from {name}, {len(df)} rows.')
            data_holder.data.loc[index, self.col_to_set] = dyntaxa


class old_AddDyntaxaId(Transformer):
    invalid_data_types = ['physicalchemical', 'chlorophyll']
    col_to_set = 'dyntaxa_id'
    source_col = 'translate_dyntaxa_scientific_name'
    mapped_dyntaxa = dict()

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.dyntaxa_id = nodc_dyntaxa.get_dyntaxa_taxon_object()

    @staticmethod
    def get_transformer_description() -> str:
        return f'Adds {AddDyntaxaId.col_to_set} translated from nodc_dyntaxa. Source column is {AddDyntaxaId.source_col}'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        if self.source_col not in data_holder.data.columns:
            adm_logger.log_transformation(f'Column {self.source_col} not in data. Could not add column {self.col_to_set} in {self.__class__.__name__}', level='error')
            return
        if self.col_to_set not in data_holder.data.columns:
            adm_logger.log_transformation(f'Adding column {self.col_to_set}', level=adm_logger.DEBUG)
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

