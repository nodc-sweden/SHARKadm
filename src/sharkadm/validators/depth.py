import polars as pl

from sharkadm.validators.base import DataHolderProtocol, Validator


class ValidateWaterDepth(Validator):
    _display_name = "Water depth"
    lower_limit = 0
    upper_limit = 500

    @staticmethod
    def get_validator_description() -> str:
        return "Checks that the water depth is within reasonable ranges (0 to 500 m)."

    def _validate(self, data_holder: DataHolderProtocol) -> None:
        self._log_workflow(
            "Checking that the water depth is within reasonable ranges (0 to 500 m).",
        )

        if "water_depth_m" not in data_holder.data.columns:
            self._log_fail(
                "Could not validate water depth because water depth column missing.",
            )
            return

        unique_rows = data_holder.data.select(
            ["visit_key", "water_depth_m"],
        ).unique()
        unique_rows = unique_rows.with_columns(
            [
                pl.when(
                    pl.col("water_depth_m")
                    .cast(pl.Float64, strict=False)
                    .is_between(self.lower_limit, self.upper_limit, closed="none")
                )
                .then(True)
                .when(pl.col("water_depth_m").is_null() | (pl.col("water_depth_m") == ""))
                .then(False)
                .otherwise(False)
                .alias("is_valid")
            ]
        )

        if unique_rows["is_valid"].all():
            self._log_success("Water depth is ok")
        else:
            erroneous_rows = (
                unique_rows.filter(~pl.col("is_valid"))
                .select(["visit_key", "water_depth_m"])
                .to_dicts()
            )

            self._log_fail(f"Water depth has unexpected values:\n{erroneous_rows}")


class ValidateSampleDepth(Validator):
    _display_name = "Sample depth"

    @staticmethod
    def get_validator_description() -> str:
        return "Checks that sample depth is never below water depth."

    def _validate(self, data_holder: DataHolderProtocol) -> None:
        self._log_workflow(
            "Checking that sample depth is never below water depth.",
        )

        if "water_depth_m" not in data_holder.data.columns:
            self._log_fail(
                "Could not validate sample depth because water depth column missing.",
            )
            return

        if "sample_depth_m" not in data_holder.data.columns:
            self._log_fail(
                "Could not validate sample depth because sample depth column missing.",
            )
            return

        error = False
        for (sample_depth, water_depth), df in data_holder.data.group_by(
            ["sample_depth_m", "water_depth_m"]
        ):
            if float(sample_depth) >= float(water_depth):
                self._log_fail(
                    f"Sample depth below water depth: {sample_depth} >= {water_depth}"
                    f" at visit keys: {df['visit_key'].unique().to_list()}",
                    row_numbers=list(df["row_number"]),
                )
                error = True

        if not error:
            self._log_success("All sample depth above water depth.")


class ValidateSecchiDepth(Validator):
    _display_name = "Secchi depth"

    @staticmethod
    def get_validator_description() -> str:
        return "Checks that secchi depth is never below water depth."

    def _validate(self, data_holder: DataHolderProtocol) -> None:
        self._log_workflow(
            "Checking that secchi depth is never below water depth.",
        )

        if "water_depth_m" not in data_holder.data.columns:
            self._log_fail(
                "Could not validate secchi depth because water depth column missing.",
            )
            return

        if (
            "parameter" not in data_holder.data.columns
            or "value" not in data_holder.data.columns
        ):
            self._log_fail(
                "Could not validate secchi depth because secchi depth is missing.",
            )
            return

        error = False
        secchi_value = False
        for (value, water_depth), df in data_holder.data.filter(
            data_holder.data["parameter"] == "SECCHI"
        ).group_by(["value", "water_depth_m"]):
            secchi_value = True
            if float(value) >= float(water_depth):
                self._log_fail(
                    f"Sample depth below water depth: {value} >= {df['water_depth_m']}"
                    f" at visit keys: {df['visit_key'].unique().to_list()}",
                    row_numbers=list(df["row_number"]),
                )
                error = True

        if not secchi_value:
            self._log_fail(
                "Could not validate secchi depth because secchi depth is missing.",
            )
            return

        if not error:
            self._log_success("All sample depth above water depth.")
