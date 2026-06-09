from sharkadm import config
from sharkadm.data import archive

from ..data.data_holder import PolarsDataHolder
from .base import PolarsTransformer


class ArchiveMapper(PolarsTransformer):
    valid_data_holders = archive.get_polars_archive_data_holder_names()

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @staticmethod
    def get_transformer_description() -> str:
        return "Maps the data header using import matrix"

    def _transform(self, data_holder: archive.PolarsArchiveDataHolder) -> None:
        import_matrix = config.get_import_matrix_config(
            data_type=data_holder.delivery_note.data_type
        )
        if not import_matrix:
            import_matrix = config.get_import_matrix_config(
                data_type=data_holder.delivery_note.data_format
            )
        mapper = import_matrix.get_mapper(data_holder.delivery_note.import_matrix_key)

        mapped_header = []
        for item in data_holder.data.columns:
            mapped_header.append(mapper.get_internal_name(item))
        data_holder.data.columns = mapped_header


class ExternalMapper(PolarsTransformer):
    def __init__(self, export_column=None, **kwargs):
        self._export_column = export_column
        super().__init__(**kwargs)

    @staticmethod
    def get_transformer_description() -> str:
        return "Maps the data header using import matrix"

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        import_matrix = config.get_import_matrix_config(
            data_type=data_holder.data_type_internal
        )
        mapper = import_matrix.get_mapper(self._export_column)
        mapped_header = []
        for item in data_holder.data.columns:
            mapped_header.append(mapper.get_external_name(item))
        data_holder.data.columns = mapped_header
