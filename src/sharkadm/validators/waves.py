import polars as pl

from sharkadm.validators.base import DataHolderProtocol, Validator


class ValidateWaves(Validator):
    _display_name = "Wave observation code"

    @staticmethod
    def get_validator_description() -> str:
        return "Checks that the wave observation code has correct format."

    def _validate(self, data_holder: DataHolderProtocol) -> None:
        self._log_workflow(
            "Checking that the wave observation code has correct format.",
        )

        if "wave_observation_code" not in data_holder.data.columns:
            self._log_fail(
                "Could not validate the wave observation code, column is missing.",
            )
            return
        if (
            "visit_date" not in data_holder.data.columns
            or "reported_station_name" not in data_holder.data.columns
        ):
            self._log_fail("Missing visit date or reported station name columns.")

        valid_values = [str(i) for i in range(0, 10)]
        unique_rows = data_holder.data.select(
            ["visit_date", "reported_station_name", "wave_observation_code"]
        ).unique()
        unique_rows = unique_rows.with_columns(
            [
                pl.when(
                    pl.col("wave_observation_code").is_null()
                    | (pl.col("wave_observation_code") == "")
                )
                .then(True)
                .when(pl.col("wave_observation_code").is_in(valid_values))
                .then(True)
                .otherwise(False)
                .alias("is_valid")
            ]
        )

        if unique_rows["is_valid"].all():
            self._log_success("Wave observation code is ok")
        else:
            erroneous_rows = (
                unique_rows.filter(~pl.col("is_valid"))
                .select(["visit_date", "reported_station_name", "wave_observation_code"])
                .to_dicts()
            )

            self._log_fail(
                f"Wave observation code has unexpected values:\n{erroneous_rows}"
            )
