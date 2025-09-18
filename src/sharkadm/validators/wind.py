from sharkadm.validators.base import DataHolderProtocol, Validator


class ValidateWindir(Validator):
    _display_name = "Wind direction"

    @staticmethod
    def get_validator_description() -> str:
        return "Checks that wind direction code is in correct format."

    def _validate(self, data_holder: DataHolderProtocol) -> None:
        self._log_workflow(
            "Checking that wind direction code is in correct format.",
        )

        if "wind_direction_code" not in data_holder.data.columns:
            self._log_fail(
                "Could not validate wind direction code, column is missing.",
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
                    f"Wind direction code: {code_str} not in correct format "
                    f"at {visit_info}",
                )

        if not error:
            self._log_success("Wind direction code is ok")


class ValidateWinsp(Validator):
    _display_name = "Wind speed (m/s)"

    @staticmethod
    def get_validator_description() -> str:
        return "Checks that wind speed (m/s) is within reasonable ranges (0-40 m/s)."

    def _validate(self, data_holder: DataHolderProtocol) -> None:
        self._log_workflow(
            "Checking that wind speed (m/s) is within reasonable ranges (0-40 m/s).",
        )

        if "wind_speed_ms" not in data_holder.data.columns:
            self._log_fail(
                "Could not validate wind speed (m/s), column is missing.",
            )
            return

        error = False

        lower_limit = 0
        upper_limit = 40
        unique_rows = data_holder.data.select(["visit_key", "wind_speed_ms"]).unique()

        for row in unique_rows.iter_rows(named=True):
            visit_info = row["visit_key"]
            winsp = row["wind_speed_ms"]

            if not winsp or winsp == "":
                continue
            try:
                winsp = float(winsp)
                if lower_limit <= float(winsp) <= upper_limit:
                    continue
                else:
                    error = True
                    self._log_fail(
                        f"Wind speed (m/s): {winsp} is outside reasonable "
                        f"ranges (0-40 m/s) at {visit_info}",
                    )
            except ValueError:
                error = True
                self._log_fail(
                    f"Wind speed (m/s): {winsp} has unexpected format at {visit_info}",
                )

        if not error:
            self._log_success("Wind speed (m/s) is ok")
