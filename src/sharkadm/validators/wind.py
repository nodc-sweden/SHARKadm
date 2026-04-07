import polars as pl

from sharkadm.validators.base import DataHolderProtocol, Validator


class ValidateWindir(Validator):
    _display_name = "Wind direction"

    @staticmethod
    def get_validator_description() -> str:
        return "Checks that wind direction code is in correct format."

    def _validate(self, data_holder: DataHolderProtocol) -> None:
        self._log_workflow(
            "Checking that wind direction code is in correct format.",
        )

        if "wind_direction_code" not in data_holder.data.columns:
            self._log_fail(
                "Could not validate wind direction code, column is missing.",
            )
            return

        if (
            "visit_date" not in data_holder.data.columns
            or "reported_station_name" not in data_holder.data.columns
        ):
            self._log_fail("Missing visit date or reported station name columns.")
            return

        valid_values = (
            ["00"]
            + [str(i) for i in range(1, 37)]
            + [f"{i:02d}" for i in range(1, 10)]
            + ["99"]
        )
        unique_rows = (
            data_holder.data.select(
                [
                    "visit_date",
                    "reported_station_name",
                    "wind_direction_code",
                    "row_number",
                ]
            )
            .group_by(["visit_date", "reported_station_name", "wind_direction_code"])
            .agg(pl.col("row_number").alias("row_numbers"))
        )

        unique_rows = unique_rows.with_columns(
            pl.when(pl.col("wind_direction_code").is_in(valid_values))
            .then(pl.lit("Wind direction code is ok"))
            .when(
                pl.col("wind_direction_code").is_null()
                | (pl.col("wind_direction_code").str.strip_chars() == "")
            )
            .then(
                pl.format(
                    "{} on {}: Missing wind direction code: {}",
                    pl.col("reported_station_name"),
                    pl.col("visit_date"),
                    pl.col("wind_direction_code"),
                )
            )
            .otherwise(
                pl.format(
                    "{} on {}: Wind direction code has unexpected value: {}",
                    pl.col("reported_station_name"),
                    pl.col("visit_date"),
                    pl.col("wind_direction_code"),
                )
            )
            .alias("message")
        )

        if (
            unique_rows.filter(pl.col("message") != "Wind direction code is ok").height
            == 0
        ):
            self._log_success("All wind direction codes are ok")
        else:
            for (msg,), df in unique_rows.filter(
                pl.col("message") != "Wind direction code is ok"
            ).group_by("message"):
                self._log_fail(msg=msg, row_numbers=df["row_numbers"][0])


class ValidateWinsp(Validator):
    _display_name = "Wind speed (m/s)"
    lower_limit = 0
    upper_limit = 40

    @staticmethod
    def get_validator_description() -> str:
        return "Checks that wind speed (m/s) is within reasonable ranges (0-40 m/s)."

    def _validate(self, data_holder: DataHolderProtocol) -> None:
        self._log_workflow(
            "Checking that wind speed (m/s) is within reasonable ranges (0-40 m/s).",
        )

        if "wind_speed_ms" not in data_holder.data.columns:
            self._log_fail(
                "Could not validate wind speed (m/s), column is missing.",
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
                    "wind_speed_ms",
                    "row_number",
                ]
            )
            .group_by(["visit_date", "reported_station_name", "wind_speed_ms"])
            .agg(pl.col("row_number").alias("row_numbers"))
        )

        unique_rows = unique_rows.with_columns(
            pl.col("wind_speed_ms")
            .cast(pl.Float64, strict=False)
            .alias("wind_speed_float")
        )
        unique_rows = unique_rows.with_columns(
            [
                pl.when(pl.col("wind_speed_float").is_null())
                .then(
                    pl.format(
                        "{} on {}: Missing or invalid wind speed (m/s): {}",
                        pl.col("reported_station_name"),
                        pl.col("visit_date"),
                        pl.col("wind_speed_ms"),
                    )
                )
                .when(
                    pl.col("wind_speed_float").is_between(
                        self.lower_limit, self.upper_limit, closed="both"
                    )
                )
                .then(pl.lit("Wind speed (m/s) is ok"))
                .otherwise(
                    pl.format(
                        "{} on {}: "
                        "Wind speed (m/s) is outside acceptable ranges:"
                        "{} > {} > {}",
                        pl.col("reported_station_name"),
                        pl.col("visit_date"),
                        self.lower_limit,
                        pl.col("wind_speed_ms"),
                        self.upper_limit,
                    )
                )
                .alias("message")
            ]
        )

        if unique_rows.filter(pl.col("message") != "Wind speed (m/s) is ok").height == 0:
            self._log_success("All wind speeds (m/s) are ok")
        else:
            for (msg,), df in unique_rows.filter(
                pl.col("message") != "Wind speed (m/s) is ok"
            ).group_by("message"):
                self._log_fail(msg=msg, row_numbers=df["row_numbers"][0])
