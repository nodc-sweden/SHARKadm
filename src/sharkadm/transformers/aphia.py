import pandas as pd
from sharkadm import adm_logger

from .base import Transformer, DataHolderProtocol

try:
    import nodc_worms
except ModuleNotFoundError as e:
    module_name = str(e).split("'")[-2]
    adm_logger.log_workflow(f'Could not import package "{module_name}" in module {__name__}. You need to install this dependency if you want to use this module.', level=adm_logger.WARNING)


class AddAphiaId(Transformer):
    invalid_data_types = ['physicalchemical', 'chlorophyll']
    col_to_set = 'aphia_id'
    reported_aphia_id_col = 'reported_aphia_id'
    source_col = 'scientific_name'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.taxa_worms = nodc_worms.get_taxa_worms_object()

    @staticmethod
    def get_transformer_description() -> str:
        return f'Adds aphia_id.'

    @adm_logger.log_time
    def _transform(self, data_holder: DataHolderProtocol) -> None:
        if self.col_to_set not in data_holder.data.columns:
            adm_logger.log_transformation(f'Adding column {self.col_to_set} in {self.__class__.__name__}',
                                          level=adm_logger.DEBUG)
            data_holder.data[self.col_to_set] = ''
        elif all(data_holder.data[self.col_to_set]):
            adm_logger.log_transformation(f'All {self.col_to_set} reported. Will skip {self.__class__.__name__}.')
            return

        data_holder.data[self.reported_aphia_id_col] = data_holder.data[self.col_to_set]  # Save reported so we can compare in validator

        for source_name, df in data_holder.data.groupby(self.source_col):
            aphia_id = self.taxa_worms.get_aphia_id(source_name)
            if not aphia_id:
                adm_logger.log_transformation(f'No aphia_id found for species', add=source_name, level=adm_logger.WARNING)
                continue
            boolean = data_holder.data[self.source_col] == source_name
            data_holder.data.loc[boolean, self.col_to_set] = aphia_id

    def _add(self, row: pd.Series) -> str:
        current_aphia_id = row[self.col_to_set].strip()
        source_name = row[self.source_col].strip()
        new_aphia_id = self.mapped_aphia_id.setdefault(source_name, self.taxa_worms.get_aphia_id(source_name))
        if current_aphia_id:
            if current_aphia_id == new_aphia_id:
                adm_logger.log_transformation(f'Given AphiaID {current_aphia_id} is the same as the translated one for '
                                              f'species: {source_name}',
                                              level=adm_logger.INFO)
                return current_aphia_id
            else:
                adm_logger.log_transformation(f'Given AphiaID {current_aphia_id} is NOT the same as the translated '
                                              f'{new_aphia_id} (for species {source_name}). Will keep the given id!',
                                              level=adm_logger.WARNING)
                return current_aphia_id

        if not new_aphia_id:
            # adm_logger.log_transformation(f'No AphiaID found for {source_name}', level=adm_logger.WARNING)
            adm_logger.log_transformation(f'No AphiaID found', level=adm_logger.WARNING, add=source_name)
            return ''

        adm_logger.log_transformation(f'Adding AphiaID {new_aphia_id} translated from {source_name}')
        return new_aphia_id


class old_AddAphiaId(Transformer):
    invalid_data_types = ['physicalchemical', 'chlorophyll']
    col_to_set = 'aphia_id'
    source_col = 'scientific_name'
    mapped_aphia_id = dict()

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.taxa_worms = nodc_worms.get_taxa_worms_object()

    @staticmethod
    def get_transformer_description() -> str:
        return f'Adds aphia_id.'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        if self.col_to_set not in data_holder.data.columns:
            adm_logger.log_transformation(f'Adding column {self.col_to_set} in {self.__class__.__name__}',
                                          level=adm_logger.DEBUG)
            data_holder.data[self.col_to_set] = ''
        elif all(data_holder.data[self.col_to_set]):
            adm_logger.log_transformation(f'All {self.col_to_set} reported. Will skip {self.__class__.__name__}.')
            return
        data_holder.data[self.col_to_set] = data_holder.data.apply(lambda row: self._add(row), axis=1)

    def _add(self, row: pd.Series) -> str:
        current_aphia_id = row[self.col_to_set].strip()
        source_name = row[self.source_col].strip()
        new_aphia_id = self.mapped_aphia_id.setdefault(source_name, self.taxa_worms.get_aphia_id(source_name))
        if current_aphia_id:
            if current_aphia_id == new_aphia_id:
                adm_logger.log_transformation(f'Given AphiaID {current_aphia_id} is the same as the translated one for '
                                              f'species: {source_name}',
                                              level=adm_logger.INFO)
                return current_aphia_id
            else:
                adm_logger.log_transformation(f'Given AphiaID {current_aphia_id} is NOT the same as the translated '
                                              f'{new_aphia_id} (for species {source_name}). Will keep the given id!',
                                              level=adm_logger.WARNING)
                return current_aphia_id

        if not new_aphia_id:
            # adm_logger.log_transformation(f'No AphiaID found for {source_name}', level=adm_logger.WARNING)
            adm_logger.log_transformation(f'No AphiaID found', level=adm_logger.WARNING, add=source_name)
            return ''

        adm_logger.log_transformation(f'Addding AphiaID {new_aphia_id} translated from {source_name}')
        return new_aphia_id


