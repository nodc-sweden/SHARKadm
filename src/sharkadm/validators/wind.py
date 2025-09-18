from sharkadm.validators.base import DataHolderProtocol, Validator


class ValidateWindir(Validator):
    _display_name = "Wind direction"

    @staticmethod
    def get_validator_description() -> str:
        return "Checks that wind direction is in correct format."

    def _validate(self, data_holder: DataHolderProtocol) -> None:
        self._log_workflow(
            "Checking that wind direction is in correct format.",
        )

        if "wind_direction_code" not in data_holder.data.columns:
            self._log_fail(
                "Could not validate WINDIR, column is missing.",
            )
            return

        error = False

        valid_values = (
            ["00"]
            + [str(i) for i in range(1, 37)]
            + [f"{i:02d}" for i in range(1, 10)]
            + ["99"]
        )
        unique_rows = data_holder.data.select(
            ["visit_key", "wind_direction_code"]
        ).unique()

        for row in unique_rows.iter_rows(named=True):
            visit_info = row["visit_key"]
            code_str = row["wind_direction_code"]

            if not code_str or code_str == "":
                continue
            if code_str not in valid_values:
                error = True
                self._log_fail(
                    f"WINDIR: {code_str} not in correct format at {visit_info}",
                )

        if not error:
            self._log_success("No incorrect WINDIR code found")
