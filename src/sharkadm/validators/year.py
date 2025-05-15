from sharkadm.sharkadm_logger import adm_logger

from .base import DataHolderProtocol, Validator


class ValidateYearNrDigits(Validator):
    _display_name = "Formatting of years"

    @staticmethod
    def get_validator_description() -> str:
        return "Checks that year is a valid four digit number"

    def _validate(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data["visit_year"].apply(self.check)
        adm_logger.log_validation_succeeded(
            "All years have valid formats.",
            item="ValidateYearNrDigits",
            level="error",
        )

    @staticmethod
    def check(x):
        if len(x) == 4:
            return
        adm_logger.log_validation_failed(
            f"Year '{x}' is not of length 4.",
            item="ValidateYearNrDigits",
            level="info",
        )
