from SHARKadm import config
from SHARKadm.data import archive
from .base import Transformer


class MapperParameterColumn(Transformer):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @staticmethod
    def get_transformer_description() -> str:
        return f'Maps parameter column using import matrix'

    def _transform(self, data_holder: archive.ArchiveDataHolder) -> None:
        import_matrix = config.get_import_matrix_config(data_type=data_holder.data_type)
        if not import_matrix:
            import_matrix = config.get_import_matrix_config(data_type=data_holder.delivery_note.data_format)
        import_column = self._kwargs.get('import_column')
        mapper = import_matrix.get_mapper(import_column)

        for par in set(data_holder.data['parameter']):
            boolean = data_holder.data['parameter'] == par
            data_holder.data.loc[boolean, 'parameter'] = mapper.get_external_name(par)
