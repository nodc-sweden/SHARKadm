from .base import Transformer
from sharkadm.data import DataHolder
import polars as pl

class AddDatasetName(Transformer):
    datatype_column_name = 'dataset_name'

    @staticmethod
    def get_transformer_description() -> str:
        return f'Adds dataset_name column'

    def _transform(self, data_holder: DataHolder) -> None:
        data_holder.data[self.datatype_column_name] = data_holder.zip_archive_base


class AddDatasetFileName(Transformer):
    datatype_column_name = 'dataset_file_name'

    @staticmethod
    def get_transformer_description() -> str:
        return f'Adds dataset_name column'

    def _transform(self, data_holder: DataHolder) -> None:
        data_holder.data[self.datatype_column_name] = data_holder.zip_archive_name + '.zip'


class PolarsAddDatasetName(Transformer):
    col_to_set = 'dataset_name'

    @staticmethod
    def get_transformer_description() -> str:
        return f'Adds {PolarsAddDatasetName.col_to_set} column'

    def _transform(self, data_holder: DataHolder) -> None:
        data_holder.data = data_holder.data.with_columns(pl.lit(data_holder.zip_archive_base).alias(self.col_to_set))


class PolarsAddDatasetFileName(Transformer):
    col_to_set = 'dataset_file_name'

    @staticmethod
    def get_transformer_description() -> str:
        return f'Adds {PolarsAddDatasetFileName.col_to_set} column'

    def _transform(self, data_holder: DataHolder) -> None:
        data_holder.data = data_holder.data.with_columns(pl.lit(data_holder.zip_archive_name + '.zip').alias(self.col_to_set))


