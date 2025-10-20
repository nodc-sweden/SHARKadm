import polars as pl

from sharkadm.validators.base import DataHolderProtocol, Validator


class ValidateAirtemp(Validator):
    _display_name = "Air temperature (degC)"

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

        lower_limit = -20
        upper_limit = 40
        unique_rows = data_holder.data.select(
            ["visit_key", "air_temperature_degc"]
        ).unique()
        unique_rows = unique_rows.with_columns(
            [
                pl.when(
                    pl.col("air_temperature_degc").is_null()
                    | (pl.col("air_temperature_degc") == "")
                )
                .then(True)
                .when(
                    pl.col("air_temperature_degc")
                    .cast(pl.Float64, strict=False)
                    .is_between(lower_limit, upper_limit, closed="both")
                )
                .then(True)
                .otherwise(False)
                .alias("is_valid")
            ]
        )

        if unique_rows["is_valid"].all():
            self._log_success("Air temperature (degC) is ok")
        else:
            erroneous_rows = (
                unique_rows.filter(~pl.col("is_valid"))
                .select(["visit_key", "air_temperature_degc"])
                .to_dicts()
            )

            self._log_fail(
                f"Air temperature (degC) has unexpected values:\n{erroneous_rows}"
            )
