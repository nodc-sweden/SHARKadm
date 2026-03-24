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

        df = data_holder.data.with_columns(
            [pl.col("air_pressure_hpa").cast(pl.Float64, strict=False)]
        )
        df = df.with_columns(
            [
                (pl.col("air_pressure_hpa").is_null()).alias("fail_no_air_pressure"),
                (
                    ~pl.col("air_pressure_hpa").is_between(
                        self.lower_limit, self.upper_limit, closed="both"
                    )
                ).alias("fail_air_pressure_out_of_range"),
            ]
        )

        missing_groups = (
            df.filter(pl.col("fail_no_air_pressure"))
            .group_by(
                [
                    "visit_date",
                    "reported_station_name",
                ]
            )
            .agg(pl.col("row_number").alias("row_numbers"))
        )

        for row in missing_groups.iter_rows(named=True):
            message = (
                f"{row['reported_station_name']} on {row['visit_date']}: "
                f"Missing air pressure ({row['air_pressure_hpa']})"
            )
            self._log_fail(msg=message, row_numbers=row["row_numbers"])

        out_of_range_groups = (
            df.filter(pl.col("fail_air_pressure_out_of_range"))
            .group_by(["visit_date", "reported_station_name", "air_pressure_hpa"])
            .agg(pl.col("row_number").alias("row_numbers"))
        )

        for row in out_of_range_groups.iter_rows(named=True):
            message = (
                f"{row['reported_station_name']} on {row['visit_date']}: "
                f"air pressure out of range: "
                f"{self.lower_limit} > {row['air_pressure_hpa']} > {self.upper_limit}"
            )
            self._log_fail(msg=message, row_numbers=row["row_numbers"])

        if missing_groups.height == 0 and out_of_range_groups.height == 0:
            self._log_success("All airpressures are accepted.")
