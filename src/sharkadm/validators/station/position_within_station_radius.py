import geopandas as gp
from shapely import Point

from sharkadm.validators import Validator
from sharkadm.validators.base import DataHolderProtocol


class ValidatePositionWithinStationRadius(Validator):
    _display_name = "Position within any station radius"

    def __init__(
        self,
        stations: tuple[str, str, str] = (),
        station_name_key="statn",
        latitude_key="LATIT",
        longitude_key="LONGI",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._station_name_key = station_name_key
        self._latitude_key = latitude_key
        self._longitude_key = longitude_key
        self._stations = gp.GeoDataFrame(
            (
                {
                    "station": station_name,
                    "geometry": Point(longitude, latitude).buffer(radius),
                }
                for station_name, longitude, latitude, radius in stations
            )
        )

    @staticmethod
    def get_validator_description() -> str:
        return "Checks if station is within the radius of any known stations."

    def _validate(self, data_holder: DataHolderProtocol) -> None:
        if self._stations.empty:
            self._log_fail(
                "Could not validate station position "
                "because validator has no list of station locations."
            )
            return

        for (name, latitude, longitude), data in data_holder.data.groupby(
            [self._station_name_key, self._latitude_key, self._longitude_key]
        ):
            point = Point(longitude, latitude)
            if (selected := self._stations.contains(point)).any():
                nearby_stations = list(self._stations[selected]["station"])
                self._log_success(
                    f"Measurements for '{name}' is within radius of known stations: "
                    + ", ".join(
                        f"'{nearby_station}'" for nearby_station in nearby_stations
                    )
                )
            else:
                self._log_fail(
                    f"Measurements for '{name}' (at {point.x}, {point.y}) "
                    f"is not near a known station."
                )
