from sharkadm import adm_logger
from .base import Transformer, DataHolderProtocol

try:
    import nodc_dyntaxa
    translate_dyntaxa = nodc_dyntaxa.get_translate_dyntaxa_object()
    dyntaxa_taxon = nodc_dyntaxa.get_dyntaxa_taxon_object()
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


class AddDyntaxaScientificName(Transformer):
    invalid_data_types = ['physicalchemical', 'chlorophyll']
    source_col = 'reported_scientific_name'
    col_to_set = 'dyntaxa_scientific_name'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @staticmethod
    def get_transformer_description() -> str:
        return f'Adds {AddDyntaxaScientificName.col_to_set} translated from nodc_dyntaxa. Source column is {AddDyntaxaScientificName.source_col}'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data[self.col_to_set] = ''
        for name, df in data_holder.data.groupby(self.source_col):
            name = str(name)
            new_name = translate_dyntaxa.get(name)
            if new_name:
                if name.isdigit():
                    new_name_2 = translate_dyntaxa.get(new_name)
                    if new_name_2:
                        adm_logger.log_transformation(
                            f'Translated from dyntaxa: {name} -> {new_name} -> {new_name_2} ({len(df)} places)',
                            level=adm_logger.INFO)
                    else:
                        adm_logger.log_transformation(f'No translation for: {name} ({len(df)} places)',
                                                      level=adm_logger.DEBUG)
                else:
                    adm_logger.log_transformation(f'Translated from dyntaxa: {name} -> {new_name} ({len(df)} places)',
                                                  level=adm_logger.INFO)
            else:
                if name.isdigit():
                    adm_logger.log_transformation(f'{self.source_col} {name} seems to be a dyntaxa_id and could not be translation ({len(df)} places)',
                                                  level=adm_logger.WARNING)
                else:
                    adm_logger.log_transformation(f'No translation for: {name} ({len(df)} places)',
                                              level=adm_logger.DEBUG)
                new_name = name

            boolean = data_holder.data[self.source_col] == name
            data_holder.data.loc[boolean, self.col_to_set] = new_name


class AddDyntaxaScientificNameDyntaxaId(Transformer):
    invalid_data_types = ['physicalchemical', 'chlorophyll']
    source_col = 'reported_scientific_name'
    col_to_set = 'dyntaxa_scientific_name_dyntaxa_id'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @staticmethod
    def get_transformer_description() -> str:
        return f'Adds {AddDyntaxaScientificName.col_to_set} translated from nodc_dyntaxa. Source column is {AddDyntaxaScientificName.source_col}'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data[self.col_to_set] = ''
        for name, df in data_holder.data.groupby(self.source_col):
            name = str(name)
            _id = translate_dyntaxa.get_dyntaxa_id(name)
            if not _id:
                continue
            adm_logger.log_transformation(f'Adding {_id} to {self.col_to_set} ({len(df)} places)', level=adm_logger.INFO)

            boolean = data_holder.data[self.source_col] == name
            data_holder.data.loc[boolean, self.col_to_set] = _id


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
            dyntaxa_id = dyntaxa_taxon.get(str(name))
            if not dyntaxa_id:
                adm_logger.log_transformation(f'No {self.col_to_set} found for {name}, {len(df)} rows.', level=adm_logger.WARNING)
                continue
            index = data_holder.data[self.source_col] == name
            adm_logger.log_transformation(f'Adding {self.col_to_set} {dyntaxa_id} translated from {name}, {len(df)} rows.', level=adm_logger.INFO)
            data_holder.data.loc[index, self.col_to_set] = dyntaxa_id


class AddReportedScientificNameDyntaxaId(Transformer):
    invalid_data_types = ['physicalchemical', 'chlorophyll']
    source_col = 'reported_scientific_name'
    col_to_set = 'reported_scientific_name_dyntaxa_id'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @staticmethod
    def get_transformer_description() -> str:
        return f'Adds {AddReportedScientificNameDyntaxaId.col_to_set} from {AddReportedScientificNameDyntaxaId.source_col}'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data[self.col_to_set] = ''
        for name, df in data_holder.data.groupby(self.source_col):
            name = str(name)
            if not name.isdigit():
                continue
            adm_logger.log_transformation(f'Adding dyntaxa_id {name} to {self.col_to_set} from {self.source_col} ({len(df)} places)', level=adm_logger.DEBUG)

            boolean = data_holder.data[self.source_col] == name
            data_holder.data.loc[boolean, self.col_to_set] = name


class AddTaxonRanks(Transformer):
    invalid_data_types = ['physicalchemical', 'chlorophyll', 'bacterioplankton']
    ranks = ['kingdom', 'phylum', 'class', 'order', 'family', 'genus', 'species']
    cols_to_set = [f'taxon_{rank}' for rank in ranks]
    source_col = 'translate_dyntaxa_scientific_name'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @staticmethod
    def get_transformer_description() -> str:
        return f'Adds taxon rank columns. Data from dyntaxa.'

    def _add_columns(self, data_holder: DataHolderProtocol):
        for col in self.cols_to_set:
            data_holder.data[col] = ''

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        self._add_columns(data_holder=data_holder)
        for name, df in data_holder.data.groupby(self.source_col):
            info = dyntaxa_taxon.get_info(scientificName=name, taxonomicStatus='accepted')
            if not info:
                adm_logger.log_transformation(f'Could not add information about taxon rank', add=name, level=adm_logger.WARNING)
                continue
            if type(info) is list:
                adm_logger.log_transformation(f'Several matches in dyntaxa', add=name, level=adm_logger.WARNING)
                continue
            adm_logger.log_transformation(f'Adding taxon rank for {name} ({len(df)} places)', level=adm_logger.INFO)
            for rank, col in zip(self.ranks, self.cols_to_set):
                value = info.get(rank, '')
                boolean = data_holder.data[self.source_col] == name
                data_holder.data.loc[boolean, col] = value


