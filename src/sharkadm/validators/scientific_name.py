import polars as pl

from sharkadm.sharkadm_logger import adm_logger

from ..data import PolarsDataHolder
from .base import Validator


class ValidateScientificNameIsTranslated(Validator):
    from_col = "reported_scientific_name"
    to_col = "scientific_name"

    @staticmethod
    def get_validator_description() -> str:
        return (
            f"Checks if {ValidateScientificNameIsTranslated.from_col} "
            f"differs from {ValidateScientificNameIsTranslated.to_col}"
        )

    def _validate(self, data_holder: PolarsDataHolder) -> None:
        if self.to_col not in data_holder.data:
            adm_logger.log_validation_failed(
                f"Could not validate scientific_name. Missing column {self.to_col}",
                level=adm_logger.WARNING,
            )
            return
        for (fr, to), df in data_holder.data.filter(
            pl.col(self.from_col) != pl.col(self.to_col)
        ).group_by([self.from_col, self.to_col]):
            adm_logger.log_validation(
                f"Scientific name translated: {fr} -> {to} ({len(df)} places)",
                level=adm_logger.INFO,
            )


class ValidateAphiaIdDiffersFromBvolAphiaId(Validator):
    valid_data_types = ("phytoplankton",)
    from_col = "aphia_id"
    to_col = "bvol_aphia_id"

    @staticmethod
    def get_validator_description() -> str:
        return (
            f"Checks if {ValidateAphiaIdDiffersFromBvolAphiaId.from_col} "
            f"differs from {ValidateAphiaIdDiffersFromBvolAphiaId.to_col}"
        )

    def _validate(self, data_holder: PolarsDataHolder) -> None:
        missing = []
        if self.from_col not in data_holder.data:
            missing.append(self.from_col)
        if self.to_col not in data_holder.data:
            missing.append(self.to_col)
        if missing:
            adm_logger.log_validation_failed(
                f"Could not validate aphia_id. Missing column(s) {', '.join(missing)}",
                level=adm_logger.WARNING,
            )
            return
        for (fr, to), df in data_holder.data.filter(
            pl.col(self.from_col) != pl.col(self.to_col)
        ).group_by([self.from_col, self.to_col]):
            adm_logger.log_validation(
                f"AphiaId differs: {fr} ({self.from_col}) -> "
                f"{to} ({self.to_col}) ({len(df)} places)",
                level=adm_logger.INFO,
            )
