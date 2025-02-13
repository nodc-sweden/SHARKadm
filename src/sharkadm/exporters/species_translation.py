import pathlib

from .base import FileExporter, DataHolderProtocol
from sharkadm import utils, adm_logger
from sharkadm.utils.paths import get_next_incremented_file_path
from sharkadm.config import get_import_matrix_mapper, get_header_mapper_from_data_holder


class SpeciesTranslationTxt(FileExporter):
    """Creates a txt file with all translations of scientific_name to dyntaxa, worms and bvol names"""
    columns = [
        'reported_scientific_name',
        'scientific_name',
        'dyntaxa_id',
        'aphia_id',

        'reported_scientific_name_dyntaxa_id',
        'bvol_scientific_name_original',
        'bvol_scientific_name',
        'bvol_size_class',
        'bvol_ref_list',
        'bvol_aphia_id',

        'reported_dyntaxa_id',
        'dyntaxa_scientific_name',

        'taxon_hierarchy',
        'reported_aphia_id',
        'worms_scientific_name',

        'dyntaxa_translated_scientific_name_dyntaxa_id',
    ]

    @staticmethod
    def get_exporter_description() -> str:
        return 'Creates a txt file with all translations of scientific_name to dyntaxa, worms and bvol names.'

    def _export(self, data_holder: DataHolderProtocol) -> None:
        if not self.export_file_name:
            self._export_file_name = f'species_translation_{data_holder.dataset_name}.txt'
        columns = [col for col in self.columns if col in data_holder.data]
        df = data_holder.data[data_holder.data['reported_scientific_name'] != ''].drop_duplicates(columns).sort_values(['reported_scientific_name'])
        df.loc[:, columns].to_csv(self.export_file_path, sep='\t', index=False)

