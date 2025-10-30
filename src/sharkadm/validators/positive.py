from sharkadm.validators.base import DataHolderProtocol, Validator

from .. import adm_logger


class ValidatePositiveValues(Validator):
    _display_name = "Positive values"

    def __init__(self, columns_to_validate: tuple[str] | None = None) -> None:
        super().__init__()
        self.columns_to_validate = columns_to_validate
        if not self.columns_to_validate:
            self._log_workflow(
                "Not enough input, will do nothing ",
                level=adm_logger.DEBUG,
            )
            return

    @staticmethod
    def get_validator_description() -> str:
        return "Checks that all values are positive in columns specified by user. "

    def _validate(self, data_holder: DataHolderProtocol) -> None:
        for column in self.columns_to_validate:
            if column not in data_holder.data:
                continue
            self._log_workflow(
                f"Checking that all values are positive in column {column}",
            )
            error = False
            for (value,), df in data_holder.data.group_by(column):
                if not value:
                    continue
                if float(value) < 0:
                    self._log_fail(
                        f"Negative values found in colum {column}: {set(df[column])}",
                        column=column,
                        row_numbers=list(df["row_number"]),
                    )
                    error = True
            if not error:
                self._log_success(
                    f"No negative values found in column {column}.",
                    column=column,
                )
