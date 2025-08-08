import re

from sharkadm.validators import Validator
from sharkadm.validators.base import DataHolderProtocol


class ValidateCoordinatesSweref99(Validator):
    _display_name = "Valid Sweref 99"
    _longitude_dm_column = "LONGI"
    _latitude_dm_column = "LATIT"
    _min_longitude = -150000
    _max_longitude = 1400000
    _min_latitude = 5800000
    _max_latitude = 7500000

    SWEREF99_PATTERN = re.compile(r"(-?\d+(?:\.\d+)?)")

    @staticmethod
    def get_validator_description() -> str:
        return "Checks if station coordinates are valid Sweref 99 coordinates."

    def _validate(self, data_holder: DataHolderProtocol) -> None:
        for (latitude, longitude), data in data_holder.data.group_by(
            [self._latitude_dm_column, self._longitude_dm_column]
        ):
            errors = self._validate_longitude(longitude)
            errors += self._validate_latitude(latitude)
            if errors:
                self._log_fail(
                    f"Bad Sweref 99 coordinates. {' '.join(errors)}",
                    row_numbers=list(data["row_number"]),
                )
            else:
                self._log_success(
                    "Valid Sweref 99 coordinates.", row_numbers=list(data["row_number"])
                )

    @staticmethod
    def _validate_longitude(longitude: str) -> list:
        errors = []
        if match := ValidateCoordinatesSweref99.SWEREF99_PATTERN.match(longitude):
            meters = float(match.group(1))
            if ValidateCoordinatesSweref99._min_longitude - meters > 1e-8:
                errors.append(
                    f"Value '{longitude}' "
                    f"below {ValidateCoordinatesSweref99._min_longitude}"
                )
            if meters - ValidateCoordinatesSweref99._max_longitude > 1e-8:
                errors.append(
                    f"Value '{longitude}' "
                    f"below {ValidateCoordinatesSweref99._max_longitude}"
                )
        else:
            errors.append(f"Value '{longitude}' is not a valid longitude.")
        return errors

    @staticmethod
    def _validate_latitude(latitude: str) -> list:
        errors = []
        if match := ValidateCoordinatesSweref99.SWEREF99_PATTERN.match(latitude):
            meters = float(match.group(1))
            if ValidateCoordinatesSweref99._min_latitude - meters > 1e-8:
                errors.append(
                    f"Value '{latitude}' "
                    f"below {ValidateCoordinatesSweref99._min_latitude}"
                )
            if meters - ValidateCoordinatesSweref99._max_latitude > 1e-8:
                errors.append(
                    f"Value '{latitude}' "
                    f"above {ValidateCoordinatesSweref99._min_latitude}"
                )
        else:
            errors.append(f"Value '{latitude}' is not a valid latitude.")
        return errors
