from sharkadm import config
from sharkadm.data import archive
from .base import Transformer


class ArchiveMapper(Transformer):
    valid_data_holders = archive.get_archive_data_holder_names()

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @staticmethod
    def get_transformer_description() -> str:
        return f"Maps the data header using import matrix"

    def _transform(self, data_holder: archive.ArchiveDataHolder) -> None:
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
