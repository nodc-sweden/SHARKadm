from .base import Transformer
from sharkadm.data import PandasDataHolder


class AddDatasetName(Transformer):
    datatype_column_name = 'dataset_name'

    @staticmethod
    def get_transformer_description() -> str:
        return f'Adds dataset_name column'

    def _transform(self, data_holder: PandasDataHolder) -> None:
        data_holder.data[self.datatype_column_name] = data_holder.zip_archive_base


class AddDatasetFileName(Transformer):
    datatype_column_name = 'dataset_file_name'

    @staticmethod
    def get_transformer_description() -> str:
        return f'Adds dataset_name column'

    def _transform(self, data_holder: PandasDataHolder) -> None:
        data_holder.data[self.datatype_column_name] = data_holder.zip_archive_name + '.zip'


