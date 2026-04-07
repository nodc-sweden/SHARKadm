from sharkadm.sharkadm_logger import adm_logger

from .base import DataHolderProtocol, Validator


class ValidateYearNrDigits(Validator):
    _display_name = "Formatting of years"

    @staticmethod
    def get_validator_description() -> str:
        return "Checks that year is a valid four digit number"

    def _validate(self, data_holder: DataHolderProtocol) -> None:
        if data_holder.data["visit_year"].apply(self.check):
            self._log_success(
                "All years have valid formats.",
                item="ValidateYearNrDigits",
                level=adm_logger.INFO,
            )
        else:
            self._log_fail(
                msg=f"Year {data_holder.data['visit_year']} is not of length 4.",
                item="ValidateYearNrDigits",
                level=adm_logger.WARNING,
            )

    @staticmethod
    def check(x):
        if len(x) == 4:
            return True
