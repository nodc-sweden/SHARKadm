import re

from sharkadm.validators import Validator
from sharkadm.validators.base import DataHolderProtocol


class ValidateCoordinatesDm(Validator):
    _display_name = "Valid degrees decimal minutes"

    DD_PATTERN = re.compile(r"^-?(\d+)(\d{2})(?:\.(\d+))?$")

    def __init__(
        self,
        longitude_dm_column: str = "LONGI",
        latitude_dm_column: str = "LATIT",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._latitude_dm_column = latitude_dm_column
        self._longitude_dm_column = longitude_dm_column

    @staticmethod
    def get_validator_description() -> str:
        return "Checks if station coordinates are valid DM coordinates."

    def _validate(self, data_holder: DataHolderProtocol) -> None:
        if (
            self._latitude_dm_column not in data_holder.data
            or self._longitude_dm_column not in data_holder.data
        ):
            self._log_fail("No coordinates found in data.")
            return

        for (latitude, longitude), data in data_holder.data.groupby(
            [self._latitude_dm_column, self._longitude_dm_column]
        ):
            errors = self._validate_longitude(longitude)
            errors += self._validate_latitude(latitude)
            if errors:
                self._log_fail(
                    f"Bad DM coordinates. {' '.join(errors)}",
                    row_numbers=list(data["row_number"]),
                )
            else:
                self._log_success(
                    "Valid DM coordinates.", row_numbers=list(data["row_number"])
                )

    @staticmethod
    def _validate_longitude(longitude: str) -> list:
        errors = []
        if match := ValidateCoordinatesDm.DD_PATTERN.match(longitude):
            degrees = int(match.group(1))
            minutes_whole = int(match.group(2))
            minutes_decimals = int(match.group(3) or 0)

            minutes = minutes_whole + minutes_decimals / 10000
            if minutes >= 60:
                errors.append(f"Minutes in '{longitude}' not strictly below 60.")
            if degrees > 180 or (degrees == 180 and minutes > 0):
                errors.append(f"Value '{longitude}' exceeds 180 degrees.")
        else:
            errors.append(f"Value '{longitude}' is not a valid longitude.")
        return errors

    @staticmethod
    def _validate_latitude(latitude: str) -> list:
        errors = []
        if match := ValidateCoordinatesDm.DD_PATTERN.match(latitude):
            degrees = int(match.group(1))
            minutes_whole = int(match.group(2))
            minutes_decimals = int(match.group(3) or 0)

            minutes = minutes_whole + minutes_decimals / 10000
            if minutes >= 60:
                errors.append(f"Minutes in '{latitude}' not strictly below 60.")
            if degrees > 90 or (degrees == 90 and minutes > 0):
                errors.append(f"Value '{latitude}' exceeds 90 degrees.")
        else:
            errors.append(f"Value '{latitude}' is not a valid latitude.")
        return errors
