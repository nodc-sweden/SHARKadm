from sharkadm.validators.base import DataHolderProtocol, Validator


class ValidatePositiveValues(Validator):
    _display_name = "Positive values"

    cols_to_validate = (
        "air_pressure_hpa",
        "wind_direction_code",
        "weather_observation_code",
        "cloud_observation_code",
        "wave_observation_code",
        "ice_observation_code",
        "wind_speed_ms",
        "water_depth_m",
    )

    @staticmethod
    def get_validator_description() -> str:
        return (
            f"Checks that all values are positive in columns: "
            f"{ValidatePositiveValues.cols_to_validate}"
        )

    def _validate(self, data_holder: DataHolderProtocol) -> None:
        for col in self.cols_to_validate:
            if col not in data_holder.data:
                continue
            self._log_workflow(
                f"Checking that all values are positive in column {col}",
            )
            error = False
            for val, df in data_holder.data.groupby(col):
                if not val:
                    continue
                if float(val) < 0:
                    self._log_fail(
                        f"Negative values found in colum {col} LINES: "
                        f"{sorted(df['row_number'])}",
                        column=col,
                        row_numbers=list(df["row_number"]),
                    )
                    error = True
            if not error:
                self._log_success(
                    "No negative values found in column {col}.",
                    column=col,
                )
