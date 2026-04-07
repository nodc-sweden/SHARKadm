import polars as pl

from sharkadm.validators.base import DataHolderProtocol, Validator


class ValidateAirpres(Validator):
    _display_name = "Air pressure (hPa)"
    lower_limit = 900
    upper_limit = 1100

    @staticmethod
    def get_validator_description() -> str:
        return (
            "Checks that air pressure (hPa) is within "
            "reasonable ranges (900 to 1100 hPa)."
        )

    def _validate(self, data_holder: DataHolderProtocol) -> None:
        self._log_workflow(
            "Checking that air pressure (hPa) is within "
            "reasonable ranges (900 to 1100 hPa).",
        )

        if "air_pressure_hpa" not in data_holder.data.columns:
            self._log_fail(
                "Could not validate air pressure (hPa), column is missing.",
            )
            return
        if (
            "visit_date" not in data_holder.data.columns
            or "reported_station_name" not in data_holder.data.columns
        ):
            self._log_fail("Missing visit date or reported station name columns.")
            return

        unique_rows = (
            data_holder.data.select(
                ["visit_date", "reported_station_name", "air_pressure_hpa", "row_number"]
            )
            .group_by(["visit_date", "reported_station_name", "air_pressure_hpa"])
            .agg(pl.col("row_number").alias("row_numbers"))
        )
        unique_rows = unique_rows.with_columns(
            pl.col("air_pressure_hpa")
            .cast(pl.Float64, strict=False)
            .alias("air_pressure_float")
        )
        unique_rows = unique_rows.with_columns(
            [
                pl.when(pl.col("air_pressure_float").is_null())
                .then(
                    pl.format(
                        "{} on {}: Missing or invalid air pressure (hPa): {}",
                        pl.col("reported_station_name"),
                        pl.col("visit_date"),
                        pl.col("air_pressure_hpa"),
                    )
                )
                .when(
                    pl.col("air_pressure_float").is_between(
                        self.lower_limit, self.upper_limit, closed="both"
                    )
                )
                .then(pl.lit("Air pressure (hPa) is ok"))
                .otherwise(
                    pl.format(
                        "{} on {}: "
                        "Air pressure (hPa) is outside acceptable ranges:"
                        "{} > {} > {}",
                        pl.col("reported_station_name"),
                        pl.col("visit_date"),
                        self.lower_limit,
                        pl.col("air_pressure_hpa"),
                        self.upper_limit,
                    )
                )
                .alias("message")
            ]
        )

        if (
            unique_rows.filter(pl.col("message") != "Air pressure (hPa) is ok").height
            == 0
        ):
            self._log_success("All air pressures (hPa) are ok")
        else:
            for (msg,), df in unique_rows.filter(
                pl.col("message") != "Air pressure (hPa) is ok"
            ).group_by("message"):
                self._log_fail(msg=msg, row_numbers=df["row_numbers"][0])
