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


class ValidateScientificNameAndSizeClassDiffersFromBvol(Validator):
    valid_data_types = ("phytoplankton",)
    from_name_col = "reported_scientific_name"
    to_name_col = "bvol_scientific_name"
    from_size_class_col = "size_class"
    to_size_class_col = "bvol_size_class"

    @staticmethod
    def get_validator_description() -> str:
        return (
            f"Checks if "
            f"{ValidateScientificNameAndSizeClassDiffersFromBvol.from_name_col} "
            f"and {ValidateScientificNameAndSizeClassDiffersFromBvol.from_size_class_col}"
            f"differs from "
            f"{ValidateScientificNameAndSizeClassDiffersFromBvol.to_name_col} "
            f"and {ValidateScientificNameAndSizeClassDiffersFromBvol.to_size_class_col}"
        )

    def _validate(self, data_holder: PolarsDataHolder) -> None:
        missing = [
            col
            for col in [
                self.from_name_col,
                self.from_size_class_col,
                self.to_name_col,
                self.to_size_class_col,
            ]
            if col not in data_holder.data.columns
        ]
        if missing:
            adm_logger.log_validation_failed(
                f"Could not validate scientific and size_class. Missing column(s) "
                f"{', '.join(missing)}",
                level=adm_logger.WARNING,
            )
            return

        sep = ":"
        data = data_holder.data.with_columns(
            pl.concat_str(
                [pl.col(self.from_name_col), pl.col(self.from_size_class_col)],
                separator=sep,
            ).alias("from"),
            pl.concat_str(
                [pl.col(self.to_name_col), pl.col(self.to_size_class_col)], separator=sep
            ).alias("to"),
        )

        for (fr, to), df in data.filter(pl.col("from") != pl.col("to")).group_by(
            ["from", "to"]
        ):
            adm_logger.log_validation(
                f"Scientific name and size class combination differs: {fr} (reported) -> "
                f"{to} (bvol) ({len(df)} places)",
                level=adm_logger.INFO,
            )
