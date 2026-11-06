import polars as pl

from sharkadm.sharkadm_operator import OperatorInfo

from ..data import PolarsDataHolder, PolarsDvNewDPSTemplateDataHolder
from .base import Validator


class ValidateListValues(Validator):
    valid_data_holders = ("PolarsDvNewDPSTemplateDataHolder",)
    col_to_check = ""
    validation_list = ""

    @staticmethod
    def get_validator_description() -> str:
        return (
            f"Validate that the values in {ValidateListValues.col_to_check} belong to "
            "the list of valid values."
        )

    def _get_valid_values(self) -> list[str]:
        pass

    def _validate(self, data_holder: PolarsDvNewDPSTemplateDataHolder) -> OperatorInfo:
        valid_values = self._get_valid_values()

        # TODO what to do if col to check does not exist?
        invalid_values = (
            data_holder.data.filter(~pl.col(self.col_to_check).is_in(valid_values))
            .select(self.col_to_check)
            .unique()
            .to_series()
            .to_list()
        )

        data_holder.update_checked_validation_lists(self.col_to_check)

        if invalid_values:
            invalid_values_formatted = "\n- ".join(invalid_values)
            invalid_values_formatted = "\n- ".join(invalid_values)
            msg = (
                f"Invalid values found in column '{self.col_to_check}'. "
                f"The following values are not included in the corresponding "
                f"validation list '{self.validation_list}':\n"
                f"- {invalid_values_formatted}"
            )
            self._log_fail(msg)
        else:
            msg = f"All values in column '{self.col_to_check}' are valid."
            self._log_success(msg)
        return OperatorInfo(msg=msg)


# For each column that should be validated against a validation list:
class ValidateMonitoringProgramCode(ValidateListValues):
    col_to_check = "monitoring_program_code"
    validation_list = "unknown"  # TODO

    def _get_valid_values(self):
        return ["OVRIGT"]


# TODO namn..................................................
class ValidateThatAllValidationListsHaveBeenChecked(Validator):
    valid_data_holders = ("PolarsDvNewDPSTemplateDataHolder",)

    @staticmethod
    def get_validator_description() -> str:
        return (
            "Check that all columns listed as 'Värdelistor' (validation lists) have "
            "been validated."
        )

    def _validate(self, data_holder: PolarsDataHolder) -> OperatorInfo:
        checked_cols = data_holder.checked_validation_list_columns
        cols = data_holder.validation_list_columns

        unchecked_cols = set(cols) - set(checked_cols)

        missing_cols = set(checked_cols) - set(cols)

        if unchecked_cols:
            unchecked_cols_formatted = "\n- ".join(unchecked_cols)
            msg = (
                f"Unvalidated columns: The following columns contain values that have "
                "not been validated against their corresponding validation lists:\n"
                f"- {unchecked_cols_formatted}"
            )
            self._log_fail(msg)

        elif missing_cols:
            missing_cols_formatted = "\n- ".join(missing_cols)
            msg = (
                f"Missing validation columns. The following columns, required for "
                f"validation against the corresponding value lists, were not found "
                f"in the data. Could this be due to an outdated template?\n"
                f"- {missing_cols_formatted}"
            )
            self._log_fail(msg)

        else:
            msg = "All columns with validation lists have been validated."
            self._log_success(msg)

        return OperatorInfo(msg=msg)
