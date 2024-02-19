import pandas as pd
from SHARKadm import adm_logger
import polars as pl

from .base import Transformer, DataHolderProtocol

import dyntaxa


class AddTaxonRanks(Transformer):
    invalid_data_types = ['physicalchemical', 'chlorophyll', 'bacterioplankton']
    ranks = ['kingdom', 'phylum', 'class', 'order', 'family', 'genus', 'species']
    cols_to_set = [f'taxon_{rank}' for rank in ranks]
    source_col = 'dyntaxa_scientific_name'
    dyntaxa_taxon = dyntaxa.get_dyntaxa_taxon_object()

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
            for rank, col in zip(self.ranks, self.cols_to_set):
                value = info.get(rank, '')
                data_holder.data.loc[data_holder.data[self.source_col] == name, col] = value



