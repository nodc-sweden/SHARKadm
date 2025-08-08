from collections import defaultdict

from sharkadm.validators import Validator
from sharkadm.validators.base import DataHolderProtocol


class ValidateSynonymsInMaster(Validator):
    _display_name = "Known station alias"

    def __init__(
        self,
        station_aliases: dict[str, set] | None = None,
        station_name_column: str = "reported_station_name",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._station_name_column = station_name_column
        station_aliases = station_aliases or {}
        self._station_aliases = defaultdict(set)
        for station, aliases in station_aliases.items():
            for alias in aliases:
                self._station_aliases[alias.upper()].add(station)

    @staticmethod
    def get_validator_description() -> str:
        return "Checks if station name is a known station synonym."

    def _validate(self, data_holder: DataHolderProtocol) -> None:
        if not self._station_aliases:
            self._log_fail(
                "Could not validate station name "
                "because validator has no synonym dictionary."
            )
            return

        for (station_name,), data in data_holder.data.group_by(self._station_name_column):
            if station_name.upper() not in self._station_aliases:
                self._log_fail(
                    f"Unknown station. "
                    f"Station '{station_name}' is not in the synonym dictionary.",
                    row_numbers=list(data["row_number"]),
                )
            else:
                stations = self._station_aliases[station_name.upper()]
                station_list = "', '".join(stations)
                self._log_success(
                    f"Known station. Station '{station_name}' is synonym for "
                    f"station{'s' * (len(stations) > 1)} '{station_list}'.",
                    row_numbers=list(data["row_number"]),
                )
