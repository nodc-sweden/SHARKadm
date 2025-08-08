from sharkadm.validators.base import DataHolderProtocol, Validator


class ValidatePositiveValues(Validator):
    _display_name = "Positive values"

    columns_to_validate = (
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
            f"{ValidatePositiveValues.columns_to_validate}"
        )

    def _validate(self, data_holder: DataHolderProtocol) -> None:
        for column in self.columns_to_validate:
            if column not in data_holder.data:
                continue
            self._log_workflow(
                f"Checking that all values are positive in column {column}",
            )
            error = False
            for (value,), df in data_holder.data.group_by(column):
                if not value:
                    continue
                if float(value) < 0:
                    self._log_fail(
                        f"Negative values found in colum {column}: {set(df[column])}",
                        column=column,
                        row_numbers=list(df["row_number"]),
                    )
                    error = True
            if not error:
                self._log_success(
                    f"No negative values found in column {column}.",
                    column=column,
                )
