import datetime

import pandas as pd

from .base import Transformer, DataHolderProtocol
from SHARKadm.data import archive
from SHARKadm import config
from SHARKadm import adm_logger


class PhysicalChemicalMapper(Transformer):
    valid_data_types = ['physicalchemical']

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._mapper = config.get_physical_chemical_mapper()

    @staticmethod
    def get_transformer_description() -> str:
        return f'Maps the data header using lims mapper'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        mapped_header = []
        for item in data_holder.data.columns:
            mapped_header.append(self._mapper.get_internal_name(item))
        data_holder.data.columns = mapped_header


class ArchiveMapper(Transformer):
    valid_data_holders = archive.get_archive_data_holder_names()

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @staticmethod
    def get_transformer_description() -> str:
        return f'Maps the data header using import matrix'

    def _transform(self, data_holder: archive.ArchiveDataHolder) -> None:
        import_matrix = config.get_import_matrix_config(data_type=data_holder.delivery_note.data_type)
        if not import_matrix:
            import_matrix = config.get_import_matrix_config(data_type=data_holder.delivery_note.data_format)
        mapper = import_matrix.get_mapper(data_holder.delivery_note.import_matrix_key)

        mapped_header = []
        for item in data_holder.data.columns:
            mapped_header.append(mapper.get_internal_name(item))
        data_holder.data.columns = mapped_header
