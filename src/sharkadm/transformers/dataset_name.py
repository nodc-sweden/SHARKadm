import pathlib

import polars as pl

from sharkadm.data import PolarsDataHolder
from sharkadm.utils import archive

from .base import PolarsTransformer


class PolarsAddDatasetName(PolarsTransformer):
    col_to_set = "dataset_name"

    def __init__(self, use_source_folder: bool = False, **kwargs):
        self._use_source_folder = use_source_folder
        super().__init__(**kwargs)

    @staticmethod
    def get_transformer_description() -> str:
        return f"Adds {PolarsAddDatasetName.col_to_set} column"

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        if self._use_source_folder:
            data_holder.data = data_holder.data.with_columns(
                pl.lit(pathlib.Path(data_holder.data[0, "source"]).name).alias(
                    self.col_to_set
                )
            )
        else:
            data_holder.data = data_holder.data.with_columns(
                pl.lit(archive.get_zip_archive_base(data_holder)).alias(self.col_to_set)
            )


class PolarsAddDatasetFileName(PolarsTransformer):
    col_to_set = "dataset_file_name"

    def __init__(self, use_source_folder: bool = False, **kwargs):
        self._use_source_folder = use_source_folder
        super().__init__(**kwargs)

    @staticmethod
    def get_transformer_description() -> str:
        return f"Adds {PolarsAddDatasetFileName.col_to_set} column"

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        if self._use_source_folder:
            data_holder.data = data_holder.data.with_columns(
                pl.lit(pathlib.Path(data_holder.data[0, "source"]).name + ".zip").alias(
                    self.col_to_set
                )
            )
        else:
            data_holder.data = data_holder.data.with_columns(
                pl.lit(archive.get_zip_archive_file_base(data_holder) + ".zip").alias(
                    self.col_to_set
                )
            )
