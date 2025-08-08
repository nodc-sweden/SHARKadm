from nodc_station.station_file import StationFile
from nodc_station.utils import transform_ref_system

from sharkadm.validators import Validator
from sharkadm.validators.base import DataHolderProtocol


class ValidateStationIdentity(Validator):
    _display_name = "Station identity"

    def __init__(
        self,
        stations: StationFile = None,
        station_name_key="reported_station_name",
        visit_key="visit_key",
        latitude_key="LATIT",
        longitude_key="LONGI",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._station_name_key = station_name_key
        self._visit_key = visit_key
        self._latitude_key = latitude_key
        self._longitude_key = longitude_key
        self._stations = stations

    @staticmethod
    def get_validator_description() -> str:
        return "Checks if station name (or synonym) and position matches known stations."

    def _validate(self, data_holder: DataHolderProtocol) -> None:
        for (name, visit_key, latitude, longitude), data in data_holder.data.group_by(
            [
                self._station_name_key,
                self._visit_key,
                self._latitude_key,
                self._longitude_key,
            ]
        ):
            latitude_dd, longitude_dd = transform_ref_system(latitude, longitude)
            matching_stations = self._stations.get_matching_stations(
                name, latitude_dd, longitude_dd
            )
            if matching_stations:
                if station := matching_stations.get_accepted_station():
                    if name != station.station:
                        name = f"{station.station}(a.k.a. {name})"
                    self._log_success(f"{visit_key}: {name} is a known station.")
                elif name_matches := [
                    station for station in matching_stations if station.accepted_name
                ]:
                    station = next(iter(name_matches))
                    if name != station.station:
                        f"{name} ({station.station})"
                    self._log_fail(f"{visit_key}: {name} is not near any known stations.")
                elif position_matches := [
                    station for station in matching_stations if station.accepted_position
                ]:
                    nearby_stations = ", ".join(
                        f"'{station.station}'" for station in position_matches
                    )
                    self._log_fail(
                        f"{visit_key}: Unknown station '{name}' "
                        f"is near {nearby_stations}."
                    )
            else:
                self._log_fail(
                    f"{visit_key}: Unknown station '{name}' "
                    "is not near any known station."
                )
