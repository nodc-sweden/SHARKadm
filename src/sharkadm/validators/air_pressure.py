import polars as pl

from sharkadm.validators.base import DataHolderProtocol, Validator


class ValidateAirpres(Validator):
    _display_name = "Air pressure (hPa)"

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

        lower_limit = 900
        upper_limit = 1100
        unique_rows = data_holder.data.select(["visit_key", "air_pressure_hpa"]).unique()
        unique_rows = unique_rows.with_columns(
            [
                pl.when(
                    pl.col("air_pressure_hpa").is_null()
                    | (pl.col("air_pressure_hpa") == "")
                )
                .then(True)
                .when(
                    pl.col("air_pressure_hpa")
                    .cast(pl.Float64, strict=False)
                    .is_between(lower_limit, upper_limit, closed="both")
                )
                .then(True)
                .otherwise(False)
                .alias("is_valid")
            ]
        )

        if unique_rows["is_valid"].all():
            self._log_success("Air pressure (hPa) is ok")
        else:
            erroneous_rows = (
                unique_rows.filter(~pl.col("is_valid"))
                .select(["visit_key", "air_pressure_hpa"])
                .to_dicts()
            )

            self._log_fail(f"Air pressure (hPa) has unexpected values:\n{erroneous_rows}")
