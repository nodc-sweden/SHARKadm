import re
from collections import defaultdict

from sharkadm.validators.base import DataHolderProtocol, Validator

something = [
    "REG_ID",
    "REG_ID_GROUP",
    "STATION_NAME",
    "SYNONYM_NAMES",
    "ICES_STATION_NAME",
    "LAT_DM",
    "LONG_DM",
    "LATITUDE_WGS84_SWEREF99_DD",
    "LONGITUDE_WGS84_SWEREF99_DD",
    "LATITUDE_SWEREF99TM",
    "LONGITUDE_SWEREF99TM",
    "OUT_OF_BOUNDS_RADIUS",
    "WADEP",
    "EU_CD",
    "MEDIA",
    "COMNT",
    "OLD_SHARK_ID",
]


class ValidateNameInMaster(Validator):
    _station_name_column = "statn"

    def __init__(self, station_names: set | None = None, **kwargs):
        super().__init__(**kwargs)
        self._station_names = set(map(str.lower, station_names))

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
    _station_name_column = "statn"

    def __init__(self, station_aliases: dict[str, set] | None = None, **kwargs):
        super().__init__(**kwargs)
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
                self._log_success(
                    f"Known station. Station '{station_name}' is synonym for "
                    f"station{'s' * (len(stations) > 1)} '{"', '".join(stations)}'.",
                    row_numbers=list(data["row_number"]),
                )


class ValidateCoordinatesDd(Validator):
    _longitude_dm_column = "LONGI"
    _latitude_dm_column = "LATIT"

    DD_PATTERN = re.compile(r"-?(\d+)\.(\d+)")

    @staticmethod
    def get_validator_description() -> str:
        return "Checks if station coordinates are valid DD coordinates."

    def _validate(self, data_holder: DataHolderProtocol) -> None:
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
        if match := ValidateCoordinatesDd.DD_PATTERN.match(longitude):
            degrees, decimals = map(int, match.groups())
            if degrees + decimals / 10000 > 180:
                errors.append(f"Value '{longitude}' exceeds 180 degrees.")
        else:
            errors.append(f"Value '{longitude}' is not a valid longitude.")
        return errors

    @staticmethod
    def _validate_latitude(latitude: str) -> list:
        errors = []
        if match := ValidateCoordinatesDd.DD_PATTERN.match(latitude):
            degrees, decimals = map(int, match.groups())
            if degrees + decimals / 10000 > 90:
                errors.append(f"Value '{latitude}' exceeds 90 degrees.")
        else:
            errors.append(f"Value '{latitude}' is not a valid latitude.")
        return errors
