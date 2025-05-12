import pathlib

from sharkadm import config

from .shark_api import SHARKapiDataHolder
from .shark_format import PolarsSharkDataHolder

mapper = {
    "LATIT_DD": "sample_latitude_dd",
    "Provets latitud (DD)": "sample_latitude_dd",
    "Sample latitude (DD)": "sample_latitude_dd",
    "LONGI_DD": "sample_longitude_dd",
    "Provets longitud (DD)": "sample_longitude_dd",
    "Sample longitude (DD)": "sample_longitude_dd",
    "Rapporterat stationsnamn": "reported_station_name",
    "STATN": "reported_station_name",
    "Station name": "reported_station_name",
}


def rename_columns(name: str) -> str:
    return mapper.get(name, name)


def get_shark_api_data_holder(**kwargs) -> SHARKapiDataHolder:
    mapper = config.get_import_matrix_mapper(**kwargs)
    return SHARKapiDataHolder(header_mapper=mapper, **kwargs)


def get_polars_shark_data_holder(
    path: str | pathlib.Path, **kwargs
) -> PolarsSharkDataHolder:
    holder = PolarsSharkDataHolder(path=path, **kwargs)
    holder.data = holder.data.rename(rename_columns)
    return holder


def file_is_from_shark(path: str | pathlib.Path) -> bool:
    """
    For now just a simple check if file is txt.
    """
    path = pathlib.Path(path)
    if path.exists() and path.suffix == ".txt":
        return True
    return False
