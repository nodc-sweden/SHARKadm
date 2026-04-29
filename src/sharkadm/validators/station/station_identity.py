import polars as pl

from sharkadm.sharkadm_logger import adm_logger
from sharkadm.validators import Validator
from sharkadm.validators.base import DataHolderProtocol

nodc_station = None
try:
    from nodc_station.station_file import StationFile
    from nodc_station.utils import transform_ref_system
except ModuleNotFoundError as e:
    module_name = str(e).split("'")[-2]
    adm_logger.log_workflow(
        f'Could not import package "{module_name}" in module {__name__}. You need to '
        f"install this dependency if you want to use this module.",
        level=adm_logger.WARNING,
    )


class ValidateStationIdentity(Validator):
    _display_name = "Station identity"

    def __init__(
        self,
        stations: StationFile = None,
        station_name_key="reported_station_name",
        latitude_key="LATIT",
        longitude_key="LONGI",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._station_name_key = station_name_key
        self._latitude_key = latitude_key
        self._longitude_key = longitude_key
        self._stations = stations

    @staticmethod
    def get_validator_description() -> str:
        return "Checks if station name (or synonym) and position matches known stations."

    def _validate(self, data_holder: DataHolderProtocol) -> None:
        df = data_holder.data.with_columns(
            [
                pl.col(self._latitude_key).cast(pl.Float64, strict=False),
                pl.col(self._longitude_key).cast(pl.Float64, strict=False),
            ]
        )

        for (name, lat_orig, lon_orig), group in df.group_by(
            [
                self._station_name_key,
                self._latitude_key,
                self._longitude_key,
            ]
        ):
            row_numbers = group["row_number"].to_list()

            try:
                lat_dd, lon_dd = transform_ref_system(lat_orig, lon_orig)
            except Exception:
                self._log_fail(
                    msg=f"{name} at {lat_orig:.2f} N {lon_orig:.2f}"
                    f" E could not be transformed",
                )
                continue

            try:
                matching_stations = self._stations.get_matching_stations(
                    name, lat_dd, lon_dd
                )
            except Exception:
                self._log_fail(
                    msg=f"{name} at {lat_orig:.2f} N"
                    f" {lon_orig:.2f} E could not be validated",
                    row_numbers=row_numbers,
                )
                continue

            if matching_stations:
                if station := matching_stations.get_accepted_station():
                    self._log_success(
                        msg=f"{name} at {lat_dd:.2f} N {lon_dd:.2f} E "
                        "is a known station within accepted radius. "
                        f"{station}",
                        row_numbers=row_numbers,
                    )
                elif station := matching_stations.get_closest_station():
                    self._log_fail(
                        msg=f"{name} at {lat_dd:.2f} N {lon_dd:.2f} E "
                        f"is close to known stations. {station}",
                        row_numbers=row_numbers,
                    )
                elif name_matches := [s for s in matching_stations if s.accepted_name]:
                    station = next(iter(name_matches))
                    self._log_fail(
                        msg=f"{name} at {lat_dd:.2f} N {lon_dd:.2f} E "
                        "is not near any known station.",
                        row_numbers=row_numbers,
                    )
                elif position_matches := [
                    s for s in matching_stations if s.accepted_position
                ]:
                    nearby_stations = ", ".join(
                        f"'{s.station}'" for s in position_matches
                    )
                    self._log_fail(
                        msg=f"{name} at {lat_dd} N {lon_dd} E "
                        f"unknown station near {nearby_stations}.",
                        row_numbers=row_numbers,
                    )
            else:
                self._log_success(
                    msg=f"{name} at {lat_dd:.2f} N {lon_dd:.2f} E "
                    "is an unknown station & not near any known station.",
                    row_numbers=row_numbers,
                )
