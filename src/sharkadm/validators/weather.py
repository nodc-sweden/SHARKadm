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

        valid_values = [str(i) for i in range(0, 10)]

        unique_rows = data_holder.data.select(
            ["visit_key", "weather_observation_code"]
        ).unique()

        unique_rows = unique_rows.with_columns(
            [
                pl.when(
                    pl.col("weather_observation_code").is_null()
                    | (pl.col("weather_observation_code") == "")
                )
                .then(True)
                .when(pl.col("weather_observation_code").is_in(valid_values))
                .then(True)
                .otherwise(False)
                .alias("is_valid")
            ]
        )

        if unique_rows["is_valid"].all():
            self._log_success("Weather observation code is ok")
        else:
            erroneous_rows = (
                unique_rows.filter(~pl.col("is_valid"))
                .select(["visit_key", "weather_observation_code"])
                .to_dicts()
            )

            self._log_fail(
                f"Weather observation code has unexpected values:\n{erroneous_rows}"
            )


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
        unique_rows = data_holder.data.select(
            ["visit_key", "weather_observation_code", "cloud_observation_code"]
        ).unique()

        unique_rows = unique_rows.with_columns(
            [
                pl.col("cloud_observation_code").cast(pl.Int32, strict=False),
                pl.col("weather_observation_code").cast(pl.Int32, strict=False),
            ]
        )

        unique_rows = unique_rows.with_columns(
            [
                pl.when(
                    pl.col("weather_observation_code").is_null()
                    | pl.col("cloud_observation_code").is_null()
                )
                .then(True)
                .when(
                    (pl.col("weather_observation_code") == 0)
                    & pl.col("cloud_observation_code").is_between(1, 8, closed="both")
                )
                .then(False)
                .when(
                    (pl.col("weather_observation_code") == 1)
                    & (
                        (pl.col("cloud_observation_code") == 0)
                        | pl.col("cloud_observation_code").is_between(7, 8, closed="both")
                    )
                )
                .then(False)
                .when(
                    (pl.col("weather_observation_code") == 2)
                    & (pl.col("cloud_observation_code") < 7)
                )
                .then(False)
                .otherwise(True)
                .alias("is_valid")
            ]
        )

        if unique_rows["is_valid"].all():
            self._log_success("Weather and cloud observation codes are consistent")
        else:
            erroneous_rows = (
                unique_rows.filter(~pl.col("is_valid"))
                .select(
                    ["visit_key", "weather_observation_code", "cloud_observation_code"]
                )
                .to_dicts()
            )

            self._log_fail(
                f"Weather and cloud observation codes are not consistent:"
                f"\n{erroneous_rows}"
            )
