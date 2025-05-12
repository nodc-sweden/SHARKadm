import hashlib

import polars as pl

from sharkadm import config
from sharkadm.sharkadm_logger import adm_logger
from .base import DataHolderProtocol, Transformer, PolarsTransformer
from ..data import PolarsDataHolder


class AddCustomId(Transformer):
    _id_handler = config.get_custom_id_handler()

    def __init__(self, add_md5: bool = False):
        super().__init__()
        self._add_md5 = add_md5

    @staticmethod
    def get_transformer_description() -> str:
        return "Adds custom key and md5 id if add_md5=True"

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        """custom_id in taken from self._id_handler"""
        for level in self._id_handler.get_levels_for_datatype(data_holder.data_type):
            id_handler = self._id_handler.get_level_handler(
                data_type=data_holder.data_type,
                level=level,
            )
            col_name = f"custom_{level}_id"
            missing = set(id_handler.id_columns) - set(data_holder.data.columns)
            if missing:
                adm_logger.log_transformation(
                    f"Missing columns for creating {col_name}: "
                    f"{', '.join(list(missing))}",
                    level=adm_logger.WARNING,
                )
                continue
            data_holder.data[col_name] = data_holder.data.apply(
                lambda row: id_handler.get_id(row), axis=1
            )
            if self._add_md5:
                col_name_md5 = f"{col_name}_md5"
                data_holder.data[col_name_md5] = data_holder.data[col_name].apply(get_md5)

    @staticmethod
    def get_md5(x) -> str:
        return hashlib.md5(x.encode("utf-8")).hexdigest()


class AddSharkSampleMd5(Transformer):
    col_to_set = "shark_sample_md5"
    _id_handler = config.get_custom_id_handler()

    @staticmethod
    def get_transformer_description() -> str:
        return f"Adds column {AddSharkSampleMd5.col_to_set}"

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        id_handler = self._id_handler.get_level_handler(
            data_type=data_holder.data_type_internal,
            level="sample",
        )
        missing = set(id_handler.id_columns) - set(data_holder.data.columns)
        if missing:
            adm_logger.log_transformation(
                f"Missing columns for creating {self.col_to_set}: "
                f"{', '.join(list(missing))}",
                level=adm_logger.WARNING,
            )
            return
        building_blocks_col = f"{self.col_to_set}_building_blocks"
        data_holder.data[building_blocks_col] = data_holder.data.apply(
            lambda row: id_handler.get_id(row), axis=1
        )
        data_holder.data[self.col_to_set] = data_holder.data[building_blocks_col].apply(
            get_md5
        )

    @staticmethod
    def get_md5(x) -> str:
        return hashlib.md5(x.encode("utf-8")).hexdigest()


class PolarsAddCustomId(PolarsTransformer):
    _id_handler = config.get_custom_id_handler()

    def __init__(self, add_md5: bool = False):
        super().__init__()
        self._add_md5 = add_md5

    @staticmethod
    def get_transformer_description() -> str:
        return "Adds custom key and md5 id if add_md5=True"

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        """custom_id in taken from self._id_handler"""
        for level in self._id_handler.get_levels_for_datatype(data_holder.data_type):
            id_handler = self._id_handler.get_level_handler(
                data_type=data_holder.data_type,
                level=level,
            )
            col_name = f"custom_{level}_id"
            missing = set(id_handler.id_columns) - set(data_holder.data.columns)
            if missing:
                adm_logger.log_transformation(
                    f"Missing columns for creating {col_name}: "
                    f"{', '.join(list(missing))}",
                    level=adm_logger.WARNING,
                )
                continue
            concat_cols = []
            for col in id_handler.id_columns:
                concat_cols.append(pl.col(col).str.replace_all("/", "_"))

            data_holder.data = data_holder.data.with_columns(
                pl.concat_str(concat_cols, separator="_").alias(col_name)
            )
            if self._add_md5:
                col_name_md5 = f"{col_name}_md5"
                data_holder.data = data_holder.data.with_columns(
                    pl.lit("").alias(col_name_md5)
                )
                for (_id,), df in data_holder.data.group_by(col_name):
                    data_holder.data.with_columns(
                        pl.when(pl.col(col_name) == _id)
                        .then(pl.lit(get_md5(str(_id))))
                        .otherwise(pl.col(col_name_md5))
                        .alias(col_name_md5)
                    )


class PolarsAddSharkSampleMd5(PolarsTransformer):
    col_to_set = "shark_sample_md5"
    _id_handler = config.get_custom_id_handler()

    @staticmethod
    def get_transformer_description() -> str:
        return f"Adds column {PolarsAddSharkSampleMd5.col_to_set}"

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        id_handler = self._id_handler.get_level_handler(
            data_type=data_holder.data_type_internal,
            level="sample",
        )
        missing = set(id_handler.id_columns) - set(data_holder.data.columns)
        if missing:
            adm_logger.log_transformation(
                f"Missing columns for creating {self.col_to_set}: "
                f"{', '.join(list(missing))}",
                level=adm_logger.WARNING,
            )
            return
        building_blocks_col = f"{self.col_to_set}_building_blocks"
        concat_cols = []
        for col in id_handler.id_columns:
            concat_cols.append(pl.col(col).str.replace_all("/", "_"))

        data_holder.data = data_holder.data.with_columns(
            pl.concat_str(concat_cols, separator="_").alias(building_blocks_col)
        )
        data_holder.data = data_holder.data.with_columns(
            pl.lit("").alias(self.col_to_set)
        )
        for (_id,), df in data_holder.data.group_by(building_blocks_col):
            data_holder.data.with_columns(
                pl.when(pl.col(building_blocks_col) == _id)
                .then(pl.lit(get_md5(str(_id))))
                .otherwise(pl.col(self.col_to_set))
                .alias(self.col_to_set)
            )


def get_md5(x) -> str:
    return hashlib.md5(x.encode("utf-8")).hexdigest()
