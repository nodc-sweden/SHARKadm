import polars as pl

from sharkadm.sharkadm_logger import adm_logger
from ..config.translate_codes_new import get_valid_size_class_ref_list_codes

from ..data import PolarsDataHolder
from .base import Validator


class ValidateSizeClassIsPresent(Validator):
    valid_data_types = ("phytoplankton", )
    col_to_check = "size_class"

    @staticmethod
    def get_validator_description() -> str:
        return (
            f"Checks if {ValidateSizeClassIsPresent.col_to_check} has values"
        )

    def _validate(self, data_holder: PolarsDataHolder) -> None:
        df = data_holder.data.filter(pl.col(self.col_to_check).str.strip_chars() != "")
        if not len(df):
            return
        for name, d in df.group_by("reported_scientific_name"):
            self._log_fail(f"No size class given for {name} ({len(d)} places)")


class ValidateSizeClassRefListCode(Validator):
    valid_data_types = ("phytoplankton", "picoplankton")
    col_to_check = "size_class_ref_list_code"

    @staticmethod
    def get_validator_description() -> str:
        return (
            f"Checks if codes in {ValidateSizeClassRefListCode.col_to_check} are valid"
        )

    def _validate(self, data_holder: PolarsDataHolder) -> None:
        if self.col_to_check not in data_holder.data.columns:
            adm_logger.log_validation(f"Could not validate size class ref list codes "
                                      f"Missing column {self.col_to_check}.",
                                      level=adm_logger.ERROR)
            return
        valid_codes = get_valid_size_class_ref_list_codes()
        for (code, ), d in data_holder.data.group_by(self.col_to_check):
            if code in valid_codes:
                continue
            if not code:
                self._log_fail(f"Missing {self.col_to_check} ({len(d)} places)")
                continue
            self._log_fail(f"Invalid {self.col_to_check}: {code} ({len(d)} places)")