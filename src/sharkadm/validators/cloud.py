import polars as pl

from sharkadm.validators.base import DataHolderProtocol, Validator


class ValidateCloud(Validator):
    _display_name = "Cloud observation code"

    @staticmethod
    def get_validator_description() -> str:
        return "Checks that the cloud observation code has correct format."

    def _validate(self, data_holder: DataHolderProtocol) -> None:
        self._log_workflow(
            "Checking that the cloud observation code has correct format.",
        )

        if "cloud_observation_code" not in data_holder.data.columns:
            self._log_fail(
                "Could not validate the cloud observation code, column is missing.",
            )
            return

        valid_values = [str(i) for i in range(0, 11)]
        unique_rows = data_holder.data.select(
            ["visit_key", "cloud_observation_code"]
        ).unique()
        unique_rows = unique_rows.with_columns(
            [
                pl.when(
                    pl.col("cloud_observation_code").is_null()
                    | (pl.col("cloud_observation_code") == "")
                )
                .then(True)
                .when(pl.col("cloud_observation_code").is_in(valid_values))
                .then(True)
                .otherwise(False)
                .alias("is_valid")
            ]
        )

        if unique_rows["is_valid"].all():
            self._log_success("Cloud observation code is ok")
        else:
            erroneous_rows = (
                unique_rows.filter(~pl.col("is_valid"))
                .select(["visit_key", "cloud_observation_code"])
                .to_dicts()
            )

            self._log_fail(
                f"Cloud observation code has unexpected values:\n{erroneous_rows}"
            )
