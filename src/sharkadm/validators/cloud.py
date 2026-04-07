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
        if (
            "visit_date" not in data_holder.data.columns
            or "reported_station_name" not in data_holder.data.columns
        ):
            self._log_fail("Missing visit date or reported station name columns.")
            return

        valid_values = [str(i) for i in range(0, 11)]
        unique_rows = (
            data_holder.data.select(
                [
                    "visit_date",
                    "reported_station_name",
                    "cloud_observation_code",
                    "row_number",
                ]
            )
            .group_by(["visit_date", "reported_station_name", "cloud_observation_code"])
            .agg(pl.col("row_number").alias("row_numbers"))
        )
        unique_rows = unique_rows.with_columns(
            pl.when(pl.col("cloud_observation_code").is_in(valid_values))
            .then(pl.lit("Cloud observation code is ok"))
            .when(
                pl.col("cloud_observation_code").is_null()
                | (pl.col("cloud_observation_code").str.strip_chars() == "")
            )
            .then(
                pl.format(
                    "{} on {}: Missing cloud observation code: {}",
                    pl.col("reported_station_name"),
                    pl.col("visit_date"),
                    pl.col("cloud_observation_code"),
                )
            )
            .otherwise(
                pl.format(
                    "{} on {}: Cloud observation code has unexpected value: {}",
                    pl.col("reported_station_name"),
                    pl.col("visit_date"),
                    pl.col("cloud_observation_code"),
                )
            )
            .alias("message")
        )

        if (
            unique_rows.filter(pl.col("message") != "Cloud observation code is ok").height
            == 0
        ):
            self._log_success("All cloud observation codes are ok")
        else:
            for (msg,), df in unique_rows.filter(
                pl.col("message") != "Cloud observation code is ok"
            ).group_by("message"):
                self._log_fail(msg=msg, row_numbers=df["row_numbers"][0])
