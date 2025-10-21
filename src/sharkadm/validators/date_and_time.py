import datetime
import re

from sharkadm.sharkadm_logger import adm_logger

from ..data import PolarsDataHolder
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


class ValidateDateAndTime(Validator):
    _display_name = "Validate date and time"
    _earliest_valid_datetime = datetime.datetime(1800, 1, 1, 0, 0)

    _time_pattern = re.compile(r"^(\d{2}):(\d{2})$")
    _date_pattern = re.compile(r"^(\d{4})-(\d{2})-(\d{2})$")

    @staticmethod
    def get_validator_description() -> str:
        return "Checks that visit date and sample time are valid."

    def _validate(self, data_holder: PolarsDataHolder) -> None:
        self._log_workflow(
            "Checking that visit date and sample time.",
        )

        error = False
        date_col = "visit_date"
        time_col = "sample_time"
        if date_col not in data_holder.data.columns:
            date_col = "sample_date"
        for (visit_date, sample_time), df in data_holder.data.group_by(
            [date_col, time_col]
        ):
            if not (time_component := self._time_component(sample_time)):
                self._log_fail(
                    f"Sample time ({time_col}) not valid: '{sample_time}'",
                    row_numbers=list(df["row_number"]),
                )
                error = True

            if not (date_component := self._date_component(visit_date)):
                self._log_fail(
                    f"Visit date {date_col} not valid: '{visit_date}'",
                    row_numbers=list(df["row_number"]),
                )
                error = True

            if date_component and time_component:
                date_and_time = datetime.datetime.combine(date_component, time_component)
                if date_and_time < self._earliest_valid_datetime:
                    self._log_fail(
                        f"Visit date and sample time before earliest valid value: "
                        f"{date_and_time} < {self._earliest_valid_datetime}"
                    )
                    error = True
                elif date_and_time > datetime.datetime.now():
                    self._log_fail(
                        f"Visit date and sample time in future: {date_and_time}"
                    )
                    error = True
        if not error:
            self._log_success("All visit date and sample time valid.")

    def _time_component(self, time_string: str) -> datetime.time | None:
        if match := self._time_pattern.match(time_string):
            hours, minutes = match.groups()
            try:
                return datetime.time(int(hours), int(minutes))
            except ValueError:
                pass
        return None

    def _date_component(self, date_string: str) -> datetime.date | None:
        if match := self._date_pattern.match(date_string):
            year, month, day = match.groups()
            try:
                return datetime.date(int(year), int(month), int(day))
            except ValueError:
                pass
        return None
