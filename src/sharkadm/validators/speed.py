import polars as pl

from sharkadm.validators import Validator
from sharkadm.validators.base import DataHolderProtocol

MICROSECONDS_IN_AN_HOUR = 3_600_000_000
ONE_KILOMETER = 1000


class ValidateSpeed(Validator):
    _display_name = "Speed between visits"

    _date_column = "visit_date"
    _latitude_column = "LATIT"
    _longitude_column = "LONGI"
    _time_column = "sample_time"
    _visit_id_column = "visit_id"

    MIN_SPEED = 3.704  # 2 kn in km/h
    MAX_SPEED = 55.56  # 30 kn in km/h

    @staticmethod
    def get_validator_description() -> str:
        return "Checks that time between visits are realistic."

    def _validate(self, data_holder: DataHolderProtocol) -> None:
        speed = (
            data_holder.data.unique(
                [
                    self._visit_id_column,
                    self._longitude_column,
                    self._latitude_column,
                    self._date_column,
                    self._time_column,
                ]
            )
            .sort(self._visit_id_column)
            .with_columns(
                (
                    (
                        (
                            pl.col(self._longitude_column).cast(pl.Float64)
                            - pl.col(self._longitude_column).shift(1).cast(pl.Float64)
                        )
                        ** 2
                        + (
                            pl.col(self._latitude_column).cast(pl.Float64)
                            - pl.col(self._latitude_column).shift(1).cast(pl.Float64)
                        )
                        ** 2
                    ).sqrt()  # Pythagorean formula for distance
                    / ONE_KILOMETER
                ).alias("distance"),
                (
                    (
                        (
                            pl.col(self._date_column) + " " + pl.col(self._time_column)
                        ).str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S")
                        - (
                            pl.col(self._date_column).shift(1)
                            + " "
                            + pl.col(self._time_column).shift(1)
                        ).str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S")
                    ).cast(pl.Float64)
                    / MICROSECONDS_IN_AN_HOUR
                ).alias("duration"),
            )
            .with_columns((pl.col("distance") / pl.col("duration")).alias("speed"))
        )

        too_slow = speed.filter(pl.col("speed").fill_nan(0) < self.MIN_SPEED)
        too_fast = speed.filter(pl.col("speed").fill_nan(0) > self.MAX_SPEED)

        if too_slow.is_empty() and too_fast.is_empty():
            self._log_success(
                "All visits are realistically spaced in time.",
            )
        else:
            if not too_slow.is_empty():
                self._log_fail(
                    "The speed between some visits is too slow.",
                    row_numbers=list(too_slow["row_number"]),
                )
            if not too_fast.is_empty():
                self._log_fail(
                    "The speed between some visits is too high.",
                    row_numbers=list(too_fast["row_number"]),
                )
