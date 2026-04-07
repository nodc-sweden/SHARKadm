import polars as pl

from sharkadm.validators.base import DataHolderProtocol, Validator


class ValidateWeath(Validator):
    _display_name = "Weather observation code"

    @staticmethod
    def get_validator_description() -> str:
        return "Checks that the weather observation code has correct format."

    def _validate(self, data_holder: DataHolderProtocol) -> None:
        self._log_workflow(
            "Checking that the weather observation code has correct format.",
        )

        if "weather_observation_code" not in data_holder.data.columns:
            self._log_fail(
                "Could not validate the weather observation code, column is missing.",
            )
            return
        if (
            "visit_date" not in data_holder.data.columns
            or "reported_station_name" not in data_holder.data.columns
        ):
            self._log_fail("Missing visit date or reported station name columns.")
            return

        valid_values = [str(i) for i in range(0, 10)]

        unique_rows = (
            data_holder.data.select(
                [
                    "visit_date",
                    "reported_station_name",
                    "weather_observation_code",
                    "row_number",
                ]
            )
            .group_by(["visit_date", "reported_station_name", "weather_observation_code"])
            .agg(pl.col("row_number").alias("row_numbers"))
        )
        unique_rows = unique_rows.with_columns(
            pl.when(pl.col("weather_observation_code").is_in(valid_values))
            .then(pl.lit("Weather observation code is ok"))
            .when(
                pl.col("weather_observation_code").is_null()
                | (pl.col("weather_observation_code").str.strip_chars() == "")
            )
            .then(
                pl.format(
                    "{} on {}: Missing weather observation code: {}",
                    pl.col("reported_station_name"),
                    pl.col("visit_date"),
                    pl.col("weather_observation_code"),
                )
            )
            .otherwise(
                pl.format(
                    "{} on {}: Weather observation code has unexpected value: {}",
                    pl.col("reported_station_name"),
                    pl.col("visit_date"),
                    pl.col("weather_observation_code"),
                )
            )
            .alias("message")
        )

        if (
            unique_rows.filter(
                pl.col("message") != "Weather observation code is ok"
            ).height
            == 0
        ):
            self._log_success("All weather observation codes are ok")
        else:
            for (msg,), df in unique_rows.filter(
                pl.col("message") != "Weather observation code is ok"
            ).group_by("message"):
                self._log_fail(msg=msg, row_numbers=df["row_numbers"][0])


class ValidateWeatherConsistency(Validator):
    _display_name = "Weather and cloud observation code consistency"

    @staticmethod
    def get_validator_description() -> str:
        return "Checks that the weather and cloud observation codes are consistent."

    def _validate(self, data_holder: DataHolderProtocol) -> None:
        self._log_workflow(
            "Checking that the weather and cloud observation codes are consistent.",
        )

        if not all(
            col in data_holder.data.columns
            for col in ["weather_observation_code", "cloud_observation_code"]
        ):
            self._log_fail(
                "Could not validate the weather and cloud observation code consistency, "
                "missing required column(s).",
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
                    "weather_observation_code",
                    "cloud_observation_code",
                    "row_number",
                ]
            )
            .group_by(
                [
                    "visit_date",
                    "reported_station_name",
                    "weather_observation_code",
                    "cloud_observation_code",
                ]
            )
            .agg(pl.col("row_number").alias("row_numbers"))
        )

        unique_rows = unique_rows.with_columns(
            [
                pl.col("cloud_observation_code").cast(pl.Int32, strict=False),
                pl.col("weather_observation_code").cast(pl.Int32, strict=False),
            ]
        )

        unique_rows = unique_rows.with_columns(
            pl.when(
                pl.col("weather_observation_code").is_null()
                | pl.col("cloud_observation_code").is_null()
            )
            .then(
                pl.format(
                    "{} on {}: "
                    "Missing weather observation code ({}) "
                    "and/or cloud observation code ({})",
                    pl.col("reported_station_name"),
                    pl.col("visit_date"),
                    pl.col("weather_observation_code"),
                    pl.col("cloud_observation_code"),
                )
            )
            .when(
                (
                    (pl.col("weather_observation_code") == 0)
                    & pl.col("cloud_observation_code").is_between(1, 8, closed="both")
                )
                | (
                    (pl.col("weather_observation_code") == 1)
                    & (
                        (pl.col("cloud_observation_code") == 0)
                        | pl.col("cloud_observation_code").is_between(7, 8, closed="both")
                    )
                )
                | (
                    (pl.col("weather_observation_code") == 2)
                    & (pl.col("cloud_observation_code") < 7)
                )
            )
            .then(
                pl.format(
                    "{} on {}: "
                    "Weather observation code ({}) is inconsistent with"
                    " cloud observation code ({})",
                    pl.col("reported_station_name"),
                    pl.col("visit_date"),
                    pl.col("weather_observation_code"),
                    pl.col("cloud_observation_code"),
                )
            )
            .otherwise(pl.lit("Weather and cloud observation codes are consistent"))
            .alias("message")
        )

        if (
            unique_rows.filter(
                pl.col("message") != "Weather and cloud observation codes are consistent"
            ).height
            == 0
        ):
            self._log_success("All weather and cloud observation codes are consistent")
        else:
            for (msg,), df in unique_rows.filter(
                pl.col("message") != "Weather and cloud observation codes are consistent"
            ).group_by("message"):
                self._log_fail(msg=msg, row_numbers=df["row_numbers"][0])
