import geopandas as gp
from shapely import Point

from sharkadm.validators import Validator
from sharkadm.validators.base import DataHolderProtocol


class ValidatePositionInOcean(Validator):
    _display_name = "Position in ocean"

    def __init__(
        self,
        ocean_shapefile: gp.GeoDataFrame,
        station_name_key="reported_station_name",
        latitude_key="LATIT",
        longitude_key="LONGI",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._ocean_shapefile = ocean_shapefile
        self._station_name_key = station_name_key
        self._latitude_key = latitude_key
        self._longitude_key = longitude_key

    @staticmethod
    def get_validator_description() -> str:
        return "Checks if station coordinates are in the ocean."

    def _validate(self, data_holder: DataHolderProtocol) -> None:
        if self._ocean_shapefile.empty:
            self._log_fail(
                "Could not validate if positions are in ocean "
                "because validator has no ocean shapefile."
            )
            return

        for (name, latitude, longitude), data in data_holder.data.groupby(
            [self._station_name_key, self._latitude_key, self._longitude_key]
        ):
            point = Point(longitude, latitude)
            if self._ocean_shapefile.contains(point).any():
                self._log_success(f"Station '{name}' is inside ocean.")
            else:
                self._log_fail(
                    f"Station '{name}' (at {point.x}, {point.y}) is not inside ocean."
                )
