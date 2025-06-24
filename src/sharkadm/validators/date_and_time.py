import re

from sharkadm.sharkadm_logger import adm_logger

from .base import DataHolderProtocol, Validator


class MissingTime(Validator):
    source_cols = ("sample_time",)

    @staticmethod
    def get_validator_description() -> str:
        cols_str = ", ".join(MissingTime.source_cols)
        return f"Checks if values are missing in column(s): {cols_str}"

    def _validate(self, data_holder: DataHolderProtocol) -> None:
        for col in MissingTime.source_cols:
            if col not in data_holder.data.columns:
                adm_logger.log_validation_failed(
                    f'Missing column "{col}"', level=adm_logger.ERROR
                )
                continue
            df = data_holder.data[data_holder.data[col].str.strip() == ""]
            if df.empty:
                continue
            nr_missing = len(df)
            rows_str = ", ".join(list(df["row_number"].astype(str)))
            adm_logger.log_validation_failed(
                f"Missing {col} if {nr_missing} rows. Att rows: {rows_str}",
                level=adm_logger.ERROR,
            )

    @staticmethod
    def check(x):
        if len(x) == 4:
            return
        adm_logger.log_validation_failed(f'Year "{x}" is not of length 4')


class ValidateDateFormat(Validator):
    _display_name = "Valid date format"
    _source_cols = ("visit_date", "sample_date")
    _date_pattern = re.compile(r"\d{4}-\d{2}-\d{2}")

    @staticmethod
    def get_validator_description() -> str:
        return "Check if the date is in ISO-8601 format (YYYY-MM-DD)."

    def _validate(self, data_holder: DataHolderProtocol) -> None:
        errors = []
        for date_column in self._source_cols:
            if date_column not in data_holder.data.columns:
                errors.append(f"Missing column '{date_column}'.")
                continue

            for date_value, data in data_holder.data.groupby(date_column):
                if self._date_pattern.match(date_value):
                    self._log_success(
                        f"'{date_value}' is a valid date.",
                        column=date_column,
                        row_numbers=list(data["row_number"]),
                    )
                else:
                    self._log_fail(
                        f"'{date_value}' is not a valid date.",
                        column=date_column,
                        row_numbers=list(data["row_number"]),
                    )
