from sharkadm.validators.base import DataHolderProtocol, Validator


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
        for (sample_depth, water_depth), df in data_holder.data.groupby(
            ["sample_depth_m", "water_depth_m"]
        ):
            if float(sample_depth) >= float(water_depth):
                self._log_fail(
                    f"Sample depth below water depth: {sample_depth} >= {water_depth}",
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
        for value, df in data_holder.data[
            data_holder.data["parameter"] == "SECCHI"
        ].groupby("value"):
            secchi_value = True
            if float(value) >= float(df["water_depth_m"]):
                self._log_fail(
                    f"Sample depth below water depth: {value} >= {df['water_depth_m']}",
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
