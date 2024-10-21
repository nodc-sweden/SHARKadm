from sharkadm import adm_logger

from .base import Transformer, DataHolderProtocol

try:
    import nodc_dyntaxa
except ModuleNotFoundError as e:
    module_name = str(e).split("'")[-2]
    adm_logger.log_workflow(f'Could not import package "{module_name}" in module {__name__}. You need to install this dependency if you want to use this module.', level=adm_logger.WARNING)


class AddTaxonRanks(Transformer):
    invalid_data_types = ['physicalchemical', 'chlorophyll', 'bacterioplankton']
    ranks = ['kingdom', 'phylum', 'class', 'order', 'family', 'genus', 'species']
    cols_to_set = [f'taxon_{rank}' for rank in ranks]
    source_col = 'translate_dyntaxa_scientific_name'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.dyntaxa_taxon = nodc_dyntaxa.get_dyntaxa_taxon_object()

    @staticmethod
    def get_transformer_description() -> str:
        return f'Adds taxon rank columns. Data from dyntaxa.'

    def _add_columns(self, data_holder: DataHolderProtocol):
        for col in self.cols_to_set:
            data_holder.data[col] = ''

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        self._add_columns(data_holder=data_holder)
        unique_scientific_names = data_holder.data[self.source_col].unique()
        for name in unique_scientific_names:
            info = self.dyntaxa_taxon.get_info(scientificName=name, taxonomicStatus='accepted')
            if not info:
                adm_logger.log_transformation(f'Could not add information about taxon rank', add=name)
                continue
            if type(info) == list:
                adm_logger.log_transformation(f'Several matches in dyntaxa', add=name, level=adm_logger.WARNING)
                continue
            for rank, col in zip(self.ranks, self.cols_to_set):
                value = info.get(rank, '')
                data_holder.data.loc[data_holder.data[self.source_col] == name, col] = value



