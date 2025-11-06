import nodc_bvol
import polars as pl

from sharkadm.sharkadm_logger import adm_logger

from ..data import PolarsDataHolder
from .base import Validator

# class PolarsBvolScientificNameInDyntaxa(Validator):
#     invalid_data_types = ("physicalchemical", "chlorophyll")
#     col_to_check = "dyntaxa_scientific_name"
#
#     def __init__(self, **kwargs):
#         super().__init__(**kwargs)
#
#     @staticmethod
#     def get_validator_description() -> str:
#         return (
#             f"Checks if species in "
#             f"{PolarsBvolScientificNameInDyntaxa.col_to_check} are in "
#             f"dyntaxa (taxon.csv)"
#         )
#
#     def _validate(self, data_holder: PolarsDataHolder) -> None:
#         if not nodc_dyntaxa:
#             adm_logger.log_validation(
#                 "Could not check name in dyntaxa. "
#                 "Package nodc-dyntaxa not found/installed!",
#                 level=adm_logger.ERROR,
#             )
#             return
#         if self.col_to_check not in data_holder.data.columns:
#             adm_logger.log_validation(
#                 f"Could not check name in dyntaxa.
#                 Missing column {self.col_to_check}.",
#                 level=adm_logger.ERROR,
#             )
#             return
#         dyntaxa_taxon = nodc_dyntaxa.get_dyntaxa_taxon_object()
#         mapper = dict((name, name) for name in dyntaxa_taxon.get_name_list())
#
#         df = data_holder.data.with_columns(
#             pl.col(self.col_to_check).replace_strict(mapper,
#             default="").alias("mapped")
#         ).filter(pl.col("mapped") == "")
#         for (name,), d in df.group_by(self.col_to_check):
#             if not name:
#                 adm_logger.log_validation("Empty scientific name")
#             else:
#                 adm_logger.log_validation(
#                     f"{name} is not a valid scientific name "
#                     f"for dyntaxa ({len(d)} places)",
#                     level=adm_logger.WARNING,
#                 )


class ValidateBvolSizeClass(Validator):
    scientific_name_col = "bvol_scientific_name"
    size_class_col = "bvol_size_class"
    joined_col = f"{scientific_name_col}:{size_class_col}"

    @staticmethod
    def get_validator_description() -> str:
        return "Check if bvol size class is is valid in nomp-list"

    def _validate(self, data_holder: PolarsDataHolder) -> None:
        if self.scientific_name_col not in data_holder.data.columns:
            adm_logger.log_validation(
                f"Could not validate bvol size class. "
                f"No column named {self.scientific_name_col}"
            )
            return
        if self.size_class_col not in data_holder.data.columns:
            adm_logger.log_validation(
                f"Could not validate bvol size class. "
                f"No column named {self.size_class_col}"
            )
            return
        self._add_joined_column(data_holder)
        self._map_and_validate(data_holder)

    def _add_joined_column(self, data_holder: PolarsDataHolder):
        data_holder.data = data_holder.data.with_columns(
            pl.concat_str(
                [
                    pl.col(self.scientific_name_col),
                    pl.col(self.size_class_col),
                ],
                separator=":",
            ).alias(self.joined_col)
        )

    def _map_and_validate(self, data_holder: PolarsDataHolder):
        bvol_nomp = nodc_bvol.get_bvol_nomp_object()
        _mapper = bvol_nomp.get_species_and_size_class_to_aphia_id_mapper()
        # TODO: One species might map to multiple aphia_ids. Check this!
        # Species = 1138
        # AphiaID = 1133
        # Kombination = 1136
        temp_col = "_temp"
        df = data_holder.data.with_columns(
            pl.col(self.joined_col).replace_strict(_mapper, default="").alias(temp_col)
        ).filter(pl.col(temp_col) == "")
        for (name, size_class), dd in df.group_by(
            self.scientific_name_col, self.size_class_col
        ):
            self._log_fail(
                f"Invalid bvol size class {size_class} for {name} ({len(dd)} places)",
                level=adm_logger.WARNING,
            )
