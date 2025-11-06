import nodc_bvol
import polars as pl

from sharkadm.sharkadm_logger import adm_logger

from ..data import PolarsDataHolder
from .base import Validator


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
