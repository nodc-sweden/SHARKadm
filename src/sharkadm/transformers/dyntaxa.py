import pandas as pd

from sharkadm import adm_logger
from .base import Transformer, DataHolderProtocol

try:
    import nodc_dyntaxa
    translate_dyntaxa = nodc_dyntaxa.get_translate_dyntaxa_object()
    dyntaxa_id = nodc_dyntaxa.get_dyntaxa_taxon_object()
except ModuleNotFoundError as e:
    module_name = str(e).split("'")[-2]
    adm_logger.log_workflow(f'Could not import package "{module_name}" in module {__name__}. You need to install this dependency if you want to use this module.', level=adm_logger.WARNING)


class AddReportedDyntaxaId(Transformer):
    invalid_data_types = ['physicalchemical', 'chlorophyll']
    col_to_set = 'reported_dyntaxa_id'
    source_col = 'dyntaxa_id'

    @staticmethod
    def get_transformer_description() -> str:
        return f'Adds {AddReportedDyntaxaId.col_to_set} from {AddReportedDyntaxaId.source_col} if not given.'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        if self.source_col not in data_holder.data.columns:
            adm_logger.log_transformation(f'No source column {self.source_col}. Setting empty column {self.col_to_set}',
                                          level=adm_logger.DEBUG)
            data_holder.data[self.col_to_set] = ''
            return
        if self.col_to_set in data_holder.data.columns:
            adm_logger.log_transformation(f'Column {self.col_to_set} already in data. Will not add',
                                          level=adm_logger.DEBUG)
            return

        data_holder.data[self.col_to_set] = data_holder.data[self.source_col]


class AddTranslatedDyntaxaScientificName(Transformer):
    invalid_data_types = ['physicalchemical', 'chlorophyll']
    source_col = 'reported_scientific_name'
    col_to_set = 'dyntaxa_scientific_name'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @staticmethod
    def get_transformer_description() -> str:
        return f'Adds {AddTranslatedDyntaxaScientificName.col_to_set} translated from nodc_dyntaxa. Source column is {AddTranslatedDyntaxaScientificName.source_col}'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data[self.col_to_set] = ''
        for name, df in data_holder.data.groupby(self.source_col):
            name = str(name)
            # if name.isdigit():
            #     adm_logger.log_transformation(f'{self.source_col} seems to be a dyntaxa_id {name}. Will not translate.', level=adm_logger.WARNING)
            #     continue
            new_name = translate_dyntaxa.get(name)
            if new_name:
                adm_logger.log_transformation(f'Translated from dyntaxa: {name} -> {new_name} ({len(df)} places)', level=adm_logger.INFO)
            else:
                adm_logger.log_transformation(f'No translation for: {name} ({len(df)} places)',
                                              level=adm_logger.DEBUG)
                new_name = name

            boolean = data_holder.data[self.source_col] == name
            data_holder.data.loc[boolean, self.col_to_set] = new_name


class AddDyntaxaId(Transformer):
    invalid_data_types = ['physicalchemical', 'chlorophyll']
    source_col = 'dyntaxa_scientific_name'
    col_to_set = 'dyntaxa_id'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @staticmethod
    def get_transformer_description() -> str:
        return f'Adds {AddDyntaxaId.col_to_set} translated from nodc_dyntaxa. Source column is {AddDyntaxaId.source_col}'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        if self.source_col not in data_holder.data.columns:
            adm_logger.log_transformation(f'Could not add column {self.col_to_set}. Source column {self.source_col} not in data.', level=adm_logger.ERROR)
            return
        if self.col_to_set not in data_holder.data.columns:
            adm_logger.log_transformation(f'Adding empty column {self.col_to_set}', level=adm_logger.DEBUG)
            data_holder.data[self.col_to_set] = ''

        for name, df in data_holder.data.groupby(self.source_col):
            if not str(name).strip():
                adm_logger.log_transformation(f'Missing {self.source_col}, {len(df)} rows.', level=adm_logger.WARNING)
                continue
            dyntaxa = dyntaxa_id.get(str(name))
            if not dyntaxa:
                adm_logger.log_transformation(f'No {self.col_to_set} found for {name}, {len(df)} rows.', level=adm_logger.WARNING)
                continue
            index = data_holder.data[self.source_col] == name
            adm_logger.log_transformation(f'Adding {self.col_to_set} {dyntaxa} translated from {name}, {len(df)} rows.', level=adm_logger.INFO)
            data_holder.data.loc[index, self.col_to_set] = dyntaxa


