import hashlib

import polars as pl

from sharkadm import config
from sharkadm.data import PolarsDataHolder
from sharkadm.sharkadm_logger import adm_logger
from sharkadm.transformers.base import DataHolderProtocol, PolarsTransformer, Transformer


class AddSharkId(Transformer):
    def __init__(self, add_md5: bool = True):
        super().__init__()
        self._add_md5 = add_md5
        self._cached_md5 = {}

    @staticmethod
    def get_transformer_description() -> str:
        return "Adds shark_id and shark_md5_id"

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        import_matrix = config.get_import_matrix_config(data_type=data_holder.data_type)
        for level, cols in import_matrix.get_columns_by_level().items():
            if level == "variable":
                continue
            col_name = f"shark_{level}_id"
            cols = self._filter_cols(cols)
            if not all([col in data_holder.data.columns for col in cols]):
                self._log(
                    "Can not create shark_id. All columns are not in data",
                    item=", ".join(cols),
                    level=adm_logger.WARNING,
                )
                continue
            data_holder.data[col_name] = (
                data_holder.data[cols]
                .astype(str)
                .apply("_".join, axis=1)
                .replace("/", "_")
            )
            if self._add_md5:
                col_name_md5 = f"{col_name}_md5"
                data_holder.data[col_name_md5] = data_holder.data[col_name].apply(
                    self.get_md5
                )

    @staticmethod
    def _filter_cols(cols: list[str]) -> list[str]:
        new_cols = []
        for col in cols:
            if col in ["parameter", "unit", "value", "quality_flag"]:
                continue
            if col.startswith("COPY_VARIABLE"):
                continue
            if col.startswith("QFLAG"):
                continue
            if col.startswith("TEMP"):
                continue
            new_cols.append(col)
        return new_cols

    def get_md5(self, x) -> str:
        return self._cached_md5.setdefault(x, hashlib.md5(x.encode("utf-8")).hexdigest())


class PolarsAddSharkId(PolarsTransformer):
    def __init__(self, add_md5: bool = True):
        super().__init__()
        self._add_md5 = add_md5
        self._cached_md5 = {}

    @staticmethod
    def get_transformer_description() -> str:
        return "Adds shark_id and shark_md5_id"

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        import_matrix = config.get_import_matrix_config(data_type=data_holder.data_type)
        for level, cols in import_matrix.get_columns_by_level().items():
            if level == "variable":
                continue
            col_name = f"shark_{level}_id"
            cols = self._filter_cols(cols)
            if not all([col in data_holder.data.columns for col in cols]):
                self._log(
                    "Can not create shark_id. All columns are not in data",
                    item=", ".join(cols),
                    level=adm_logger.WARNING,
                )
                continue
            data_holder.data = data_holder.data.with_columns(
                pl.concat_str(
                    [pl.col(col) for col in cols],
                    separator="_",
                )
                .replace("/", "_")
                .alias(col_name)
            )
            if self._add_md5:
                col_name_md5 = f"{col_name}_md5"
                self._add_empty_col(data_holder, col_name_md5)
                for (_id,), df in data_holder.data.group_by(col_name):
                    print(f"{_id}")
                    data_holder.data = data_holder.data.with_columns(
                        pl.when(pl.col(col_name) == _id)
                        .then(pl.lit(self.get_md5(str(_id))))
                        .otherwise(pl.col(col_name_md5))
                        .alias(col_name_md5)
                    )

    @staticmethod
    def _filter_cols(cols: list[str]) -> list[str]:
        new_cols = []
        for col in cols:
            if col in ["parameter", "unit", "value", "quality_flag"]:
                continue
            if col.startswith("COPY_VARIABLE"):
                continue
            if col.startswith("QFLAG"):
                continue
            if col.startswith("TEMP"):
                continue
            new_cols.append(col)
        return new_cols

    def get_md5(self, x) -> str:
        return self._cached_md5.setdefault(x, hashlib.md5(x.encode("utf-8")).hexdigest())
