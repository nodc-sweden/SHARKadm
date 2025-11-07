import polars as pl

from sharkadm.sharkadm_logger import adm_logger

from ..data import PolarsDataHolder
from .base import Validator


class ValidateCalculatedValueDiffersToMuchFromReportedValue(Validator):
    @staticmethod
    def get_validator_description() -> str:
        return "Checks if calculated value differs to much from reported value"

    def _validate(self, data_holder: PolarsDataHolder) -> None:
        mandatory_column = "calc_by_dc"
        if mandatory_column not in data_holder.data.columns:
            adm_logger.log_validation(
                f"Could not check calculated value. "
                f"Missing column {mandatory_column}. "
                f"Seems like no calculator transformer "
                f"has been applied",
                level=adm_logger.ERROR,
            )
            return
        filt = pl.col("calc_by_dc") != ""
        for row in data_holder.data.filter(filt).to_dicts():
            adm_logger.log_validation(
                f"Calculated value differs to much from reported"
                f"value. "
                f"Parameter: {row['parameter']}. "
                f"Calculated value: {row['value']}. "
                f"Reported value: {row['reported_value']}. "
                f"({float(row['value']) / float(row['reported_value'])})"
                f"Species/size: {row['bvol_scientific_name']} / "
                f"{row['bvol_size_class']}.",
                level=adm_logger.WARNING,
                item=float(row["value"]) / float(row["reported_value"]),
            )
