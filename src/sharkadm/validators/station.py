import re
from collections import defaultdict

from sharkadm.validators.base import DataHolderProtocol, Validator


class ValidateNameInMaster(Validator):
    _display_name = "Known station name"

    def __init__(
        self,
        station_names: set | None = None,
        station_name_column: str = "statn",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._station_name_column = station_name_column
        self._station_names = set(map(str.lower, station_names or set()))

    @staticmethod
    def get_validator_description() -> str:
        return "Checks if station names are known stations."

    def _validate(self, data_holder: DataHolderProtocol) -> None:
        if not self._station_names:
            self._log_fail(
                "Could not validate station name "
                "because validator has no station collection."
            )
            return

        for station_name, data in data_holder.data.groupby(self._station_name_column):
            if station_name.lower() not in self._station_names:
                self._log_fail(
                    f"Unknown station. "
                    f"Station '{station_name}' is not in the station collection."
                )
            else:
                self._log_success(
                    f"Known station. "
                    f"Station '{station_name}' is in the station collection."
                )


class ValidateSynonymsInMaster(Validator):
    _display_name = "Known station alias"

    def __init__(
        self,
        station_aliases: dict[str, set] | None = None,
        station_name_column: str = "statn",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._station_name_column = station_name_column
        station_aliases = station_aliases or {}
        self._station_aliases = defaultdict(set)
        for station, aliases in station_aliases.items():
            for alias in aliases:
                self._station_aliases[alias.lower()].add(station)

    @staticmethod
    def get_validator_description() -> str:
        return "Checks if station names are synonyms of known stations."

    def _validate(self, data_holder: DataHolderProtocol) -> None:
        if not self._station_aliases:
            self._log_fail(
                "Could not validate station name "
                "because validator has no stations collection."
            )
            return

        for station_name, data in data_holder.data.groupby(self._station_name_column):
            if station_name.lower() not in self._station_aliases:
                self._log_fail(
                    f"Unknown station. "
                    f"Station '{station_name}' is not in the station collection.",
                    row_numbers=list(data["row_number"]),
                )
            else:
                stations = self._station_aliases[station_name.lower()]
                station_list = "', '".join(stations)
                self._log_success(
                    f"Known station. Station '{station_name}' is synonym for "
                    f"station{'s' * (len(stations) > 1)} '{station_list}'.",
                    row_numbers=list(data["row_number"]),
                )


class ValidateCoordinatesDm(Validator):
    _display_name = "Valid degrees decimal minutes"

    DD_PATTERN = re.compile(r"-?(\d+)(\d{2})\.(\d+)")

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
            degrees, minutes_whole, minutes_decimals = map(int, match.groups())
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
            degrees, minutes_whole, minutes_decimals = map(int, match.groups())
            minutes = minutes_whole + minutes_decimals / 10000
            if minutes >= 60:
                errors.append(f"Minutes in '{latitude}' not strictly below 60.")
            if degrees > 90 or (degrees == 90 and minutes > 0):
                errors.append(f"Value '{latitude}' exceeds 90 degrees.")
        else:
            errors.append(f"Value '{latitude}' is not a valid latitude.")
        return errors


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
        for (latitude, longitude), data in data_holder.data.groupby(
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
