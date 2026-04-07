import polars as pl

from sharkadm.validators.base import DataHolderProtocol, Validator


class ValidateAirtemp(Validator):
    _display_name = "Air temperature (degC)"
    lower_limit = -20
    upper_limit = 40

    @staticmethod
    def get_validator_description() -> str:
        return (
            "Checks that air temperature (degC) is within "
            "reasonable ranges (-20 to 40 degC)."
        )

    def _validate(self, data_holder: DataHolderProtocol) -> None:
        self._log_workflow(
            "Checking that air temperature (degC) is within "
            "reasonable ranges (-20 to 40 degC).",
        )

        if "air_temperature_degc" not in data_holder.data.columns:
            self._log_fail(
                "Could not validate air temperature (degC), column is missing.",
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
                [
                    "visit_date",
                    "reported_station_name",
                    "air_temperature_degc",
                    "row_number",
                ]
            )
            .group_by(["visit_date", "reported_station_name", "air_temperature_degc"])
            .agg(pl.col("row_number").alias("row_numbers"))
        )
        unique_rows = unique_rows.with_columns(
            pl.col("air_temperature_degc")
            .cast(float, strict=False)
            .alias("air_temperature_float")
        )
        unique_rows = unique_rows.with_columns(
            [
                pl.when(pl.col("air_temperature_float").is_null())
                .then(
                    pl.format(
                        "{} on {}: Missing or invalid air temperature (degC): {}",
                        pl.col("reported_station_name"),
                        pl.col("visit_date"),
                        pl.col("air_temperature_degc"),
                    )
                )
                .when(
                    pl.col("air_temperature_float").is_between(
                        self.lower_limit, self.upper_limit, closed="both"
                    )
                )
                .then(pl.lit("Air temperature is ok"))
                .otherwise(
                    pl.format(
                        "{} on {}: "
                        "Air temperature (degC) is outside acceptable ranges:"
                        "{} > {} > {}",
                        pl.col("reported_station_name"),
                        pl.col("visit_date"),
                        self.lower_limit,
                        pl.col("air_temperature_degc"),
                        self.upper_limit,
                    )
                )
                .alias("message")
            ]
        )

        if unique_rows.filter(pl.col("message") != "Air temperature is ok").height == 0:
            self._log_success("All air temperatures (degC) are ok")
        else:
            for (msg,), df in unique_rows.filter(
                pl.col("message") != "Air temperature is ok"
            ).group_by("message"):
                self._log_fail(msg=msg, row_numbers=df["row_numbers"][0])
