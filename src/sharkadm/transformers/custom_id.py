import hashlib

import polars as pl

from sharkadm import config
from sharkadm.data import PolarsDataHolder
from sharkadm.sharkadm_logger import adm_logger
from sharkadm.sharkadm_operator import OperatorsInfo, get_single_operators_info

from .base import PolarsTransformer


class PolarsAddCustomId(PolarsTransformer):
    def __init__(
        self, *names, add_column_if_missing: bool = False, add_md5: bool = False
    ):
        super().__init__()
        self._id_handler = config.get_custom_id_handler()
        self._names = names
        self._add_column_if_missing = add_column_if_missing
        self._add_md5 = add_md5

    @staticmethod
    def get_transformer_description() -> str:
        return "Adds custom key and md5 id if add_md5=True"

    def _transform(self, data_holder: PolarsDataHolder) -> OperatorsInfo:
        infos = OperatorsInfo()
        for level in self._id_handler.get_levels_for_datatype(
            data_holder.data_type_internal
        ):
            if self._names and level not in self._names:
                continue
            id_handler = self._id_handler.get_level_handler(
                data_type=data_holder.data_type_internal,
                level=level,
            )

            if not id_handler:
                msg = (
                    f"No id handler found for {data_holder.data_type_internal} "
                    f"and {level}."
                )
                self._log(
                    msg,
                    level=adm_logger.WARNING,
                )
                return get_single_operators_info(
                    operator=self, msg=msg, cause_for_termination=False, success=False
                )
            missing = set(id_handler.id_columns) - set(data_holder.data.columns)
            # print(f"{missing=}")
            if missing:
                if self._add_column_if_missing:
                    self._log(
                        f"Adding missing columns to be able to create {id_handler.name}: "
                        f"{', '.join(list(missing))}",
                        level=adm_logger.WARNING,
                    )
                    for col in missing:
                        data_holder.data = data_holder.data.with_columns(
                            pl.lit("").alias(col)
                        )
                else:
                    msg = (
                        f"Missing columns for creating {id_handler.name}: "
                        f"{', '.join(list(missing))}"
                    )

                    info = get_single_operators_info(
                        operator=self, msg=msg, cause_for_termination=False, success=False
                    )
                    infos.add(info)
                    continue

            missing = set(id_handler.id_columns) - set(data_holder.data.columns)
            if missing:
                msg = (
                    f"Missing columns for creating {id_handler.name}: "
                    f"{', '.join(list(missing))}"
                )
                self._log(
                    msg,
                    level=adm_logger.WARNING,
                )
                info = get_single_operators_info(
                    operator=self, msg=msg, cause_for_termination=False, success=False
                )
                infos.add(info)
                continue
            concat_cols = []
            for col in id_handler.id_columns:
                concat_cols.append(pl.col(col).str.replace_all("/", "_"))
            data_holder.data = data_holder.data.with_columns(
                pl.concat_str(concat_cols, separator=id_handler.separator).alias(
                    id_handler.name
                )
            )
            data_holder.data = data_holder.data.with_columns(
                pl.concat_str(
                    [pl.lit(id_handler.prefix or ""), pl.col(id_handler.name)],
                    separator=id_handler.separator,
                ).alias(id_handler.name)
            )
            if self._add_md5:
                col_name_md5 = f"{id_handler.name}_md5"
                data_holder.data = data_holder.data.with_columns(
                    pl.lit("").alias(col_name_md5)
                )
                for (_id,), df in data_holder.data.group_by(id_handler.name):
                    data_holder.data = data_holder.data.with_columns(
                        pl.when(pl.col(id_handler.name) == _id)
                        .then(pl.lit(get_md5(str(_id))))
                        .otherwise(pl.col(col_name_md5))
                        .alias(col_name_md5)
                    )
        return infos


class PolarsAddSharkSampleMd5(PolarsAddCustomId):
    def __init__(self, add_column_if_missing: bool = False):
        super().__init__(
            "shark_md5", add_md5=True, add_column_if_missing=add_column_if_missing
        )


# class PolarsAddSharkSampleMd5(PolarsTransformer):
#     col_to_set = "shark_sample_md5"
#
#     def __init__(self, add_column_if_missing: bool = False):
#         super().__init__()
#         self._add_column_if_missing = add_column_if_missing
#         self._id_handler = config.get_custom_id_handler()
#
#     @staticmethod
#     def get_transformer_description() -> str:
#         return f"Adds column {PolarsAddSharkSampleMd5.col_to_set}"
#
#     def _transform(self, data_holder: PolarsDataHolder) -> None:
#         level = "shark_md5"
#         id_handler = self._id_handler.get_level_handler(
#             data_type=data_holder.data_type_internal,
#             level=level,
#         )
#         if not id_handler:
#             self._log(
#                 f"No id handler found for {data_holder.data_type_internal} "
#                 f"and {level}.",
#                 level=adm_logger.WARNING,
#             )
#             return
#         missing = set(id_handler.id_columns) - set(data_holder.data.columns)
#         print(f"{missing=}")
#         if missing:
#             if self._add_column_if_missing:
#                 self._log(
#                     f"Adding missing columns to be able to create {self.col_to_set}: "
#                     f"{', '.join(list(missing))}",
#                     level=adm_logger.WARNING,
#                 )
#                 for col in missing:
#                     data_holder.data = data_holder.data.with_columns(pl.lit("").alias(
#                         col))
#             else:
#                 self._log(
#                     f"Missing columns for creating {self.col_to_set}: "
#                     f"{', '.join(list(missing))}",
#                     level=adm_logger.WARNING,
#                 )
#                 return
#         building_blocks_col = f"{self.col_to_set}_building_blocks"
#         concat_cols = []
#         for col in id_handler.id_columns:
#             concat_cols.append(pl.col(col).str.replace_all("/", "_"))
#
#         data_holder.data = data_holder.data.with_columns(
#             pl.concat_str(concat_cols, separator="_").alias(building_blocks_col)
#         )
#         data_holder.data = data_holder.data.with_columns(
#             pl.lit("").alias(self.col_to_set)
#         )
#         for (_id,), df in data_holder.data.group_by(building_blocks_col):
#             data_holder.data = data_holder.data.with_columns(
#                 pl.when(pl.col(building_blocks_col) == _id)
#                 .then(pl.lit(get_md5(str(_id))))
#                 .otherwise(pl.col(self.col_to_set))
#                 .alias(self.col_to_set)
#             )


def get_md5(x) -> str:
    return hashlib.md5(x.encode("utf-8")).hexdigest()
