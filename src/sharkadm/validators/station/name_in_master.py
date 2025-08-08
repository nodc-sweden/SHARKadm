from sharkadm.validators import Validator
from sharkadm.validators.base import DataHolderProtocol


class ValidateNameInMaster(Validator):
    _display_name = "Known station name"

    def __init__(
        self,
        station_names: set | None = None,
        station_name_column: str = "reported_station_name",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._station_name_column = station_name_column
        self._station_names = set(map(str.upper, station_names or set()))

    @staticmethod
    def get_validator_description() -> str:
        return "Checks if station name is a known station."

    def _validate(self, data_holder: DataHolderProtocol) -> None:
        if not self._station_names:
            self._log_fail(
                "Could not validate station name "
                "because validator has no station collection."
            )
            return

        for (station_name,), data in data_holder.data.group_by(self._station_name_column):
            if station_name.upper() not in self._station_names:
                self._log_fail(
                    f"Unknown station. "
                    f"Station '{station_name}' is not in the station collection."
                )
            else:
                self._log_success(
                    f"Known station. "
                    f"Station '{station_name}' is in the station collection."
                )
