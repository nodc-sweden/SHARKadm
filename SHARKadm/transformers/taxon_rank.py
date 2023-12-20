import pandas as pd
from SHARKadm import adm_logger
import polars as pl

from .base import Transformer, DataHolderProtocol

import dyntaxa


class AddTaxonRanks(Transformer):
    cols_to_set = ['kingdom', 'phylum', 'class', 'order', 'family', 'genus', 'species']
    source_col = 'dyntaxa_scientific_name'
    dyntaxa_taxon = dyntaxa.get_dyntaxa_taxon_object()

    @staticmethod
    def get_transformer_description() -> str:
        return f'Adds taxon rank columns. Data from dyntaxa.'

    def _add_columns(self, data_holder: DataHolderProtocol):
        for col in self.cols_to_set:
            data_holder.data[col] = ''

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        self._add_columns()
        unique_scientific_names = data_holder.data[self.source_col].unique()
        for name in unique_scientific_names:
            info = self.dyntaxa_taxon.get_info(scientificName=name, taxonomicStatus='accepted')
            for col in self.cols_to_set:
                value = info[col] or ''
                data_holder.data.loc[data_holder.data[self.source_col] == name, col] = value



