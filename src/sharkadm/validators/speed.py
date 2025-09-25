import math

import polars as pl

from sharkadm.validators import Validator
from sharkadm.validators.base import DataHolderProtocol

MICROSECONDS_IN_AN_HOUR = 3_600_000_000
ONE_KILOMETER = 1000
KNOTS_PER_KM = 1.852


class ValidateSpeed(Validator):
    _display_name = "Speed between visits"

    _ship_column = "platform_code"
    _datetime_column = "datetime"
    _latitude_column = "sample_latitude_dd"
    _longitude_column = "sample_longitude_dd"
    _visit_key_column = "visit_key"
    _row_number_column = "row_number"

    MAX_SPEED = 55.56  # 30 kn in km/h

    @staticmethod
    def get_validator_description() -> str:
        return "Checks that time between visits are realistic."

    def _validate(self, data_holder: DataHolderProtocol) -> None:
        speed = (
            approximate_distance(
                data_holder.data.unique(
                    [
                        self._ship_column,
                        self._longitude_column,
                        self._latitude_column,
                        self._datetime_column,
                    ]
                ).sort([self._ship_column, self._datetime_column])
            )
            .with_columns(
                (
                    (
                        pl.col(self._datetime_column)
                        - pl.col(self._datetime_column).shift(1)
                    ).cast(pl.Float64)
                    / MICROSECONDS_IN_AN_HOUR
                )
                .over(self._ship_column)
                .alias("_duration"),
            )
            .with_columns(
                (pl.col("_distance") / 1000 / pl.col("_duration")).alias("_speed")
            )
        )

        too_fast = speed.filter(pl.col("_speed").fill_nan(0) > self.MAX_SPEED)

        if too_fast.is_empty():
            self._log_success(
                "All visits are realistically spaced in time.",
            )
        else:
            for row in too_fast.iter_rows(named=True):
                self._log_fail(
                    f"The speed to reach '{row[self._visit_key_column]}' "
                    f"is at least {row['_speed'] / KNOTS_PER_KM:.1f} knots "
                    f"({row['_distance'] / 1000:.1f} km in {row['_duration']:.1f} h).",
                    row_numbers=row[self._row_number_column],
                )


def approximate_distance(df: pl.DataFrame) -> pl.DataFrame:
    """Distance between points using haversine formula."""
    earth_radius = 6_371_008.8
    radians_per_degree = math.pi / 180.0

    latitude = pl.col("sample_latitude_dd").cast(pl.Float64) * radians_per_degree
    longitude = pl.col("sample_longitude_dd").cast(pl.Float64) * radians_per_degree

    previous_latitude = (
        pl.col("sample_latitude_dd").shift(1).cast(pl.Float64) * radians_per_degree
    )
    previous_longitude = (
        pl.col("sample_longitude_dd").shift(1).cast(pl.Float64) * radians_per_degree
    )

    haversine = ((latitude - previous_latitude) / 2).sin() ** 2 + (
        previous_latitude.cos()
        * latitude.cos()
        * ((longitude - previous_longitude) / 2).sin() ** 2
    )
    distance = (2 * earth_radius * haversine.sqrt().arcsin()).alias("_distance")

    return df.with_columns(distance)
