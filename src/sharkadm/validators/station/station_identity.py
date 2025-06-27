import geopandas as gp
from shapely import Point

from sharkadm.validators import Validator
from sharkadm.validators.base import DataHolderProtocol


class ValidateStationIdentity(Validator):
    _display_name = "Station identity"

    def __init__(
        self,
        stations: tuple[str, set[str], str, str, int] = (),
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

        self._stations = set(station.upper() for station, *_ in stations)
        self._station_aliases = {}
        for station, aliases, *_ in stations:
            for alias in aliases:
                self._station_aliases[alias.upper()] = station.upper()

        self._positions = gp.GeoDataFrame(
            (
                {
                    "station": station_name.upper(),
                    "geometry": Point(longitude, latitude).buffer(radius),
                }
                for station_name, _, longitude, latitude, radius in stations
            )
        )

    @staticmethod
    def get_validator_description() -> str:
        return "Checks if station name (or synonym) and position matches known stations."

    def _validate(self, data_holder: DataHolderProtocol) -> None:
        for (name, visit_key, latitude, longitude), data in data_holder.data.groupby(
            [
                self._station_name_key,
                self._visit_key,
                self._latitude_key,
                self._longitude_key,
            ]
        ):
            if name.upper() in self._stations:
                station_name = name.upper()
                name = f"'{name}'"
            elif name.upper() in self._station_aliases:
                station_name = self._station_aliases[name.upper()]
                name = f"'{name}' (a.k.a '{station_name}')"
            else:
                name = f"Unknown station '{name}'"

            point = Point(longitude, latitude)
            if (selected := self._positions.contains(point)).any():
                nearby_stations = set(
                    station for station in self._positions[selected]["station"]
                )
                if station_name in nearby_stations:
                    self._log_success(
                        f"{visit_key}: {name} is within radius for expected location."
                    )
                else:
                    known_station_string = "'" + "', '".join(nearby_stations) + "'"
                    self._log_fail(
                        f"{visit_key}: {name} is within radius for "
                        f"{known_station_string}."
                    )
            else:
                self._log_fail(f"{visit_key}: {name} is not near any known station.")
