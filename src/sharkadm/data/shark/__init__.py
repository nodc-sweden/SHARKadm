from typing import Union

from sharkadm import config
import pathlib

from .shark_api import SHARKapiDataHolder
from .shark_format import PolarsSharkDataHolder


def get_shark_api_data_holder(**kwargs) -> SHARKapiDataHolder:
    mapper = config.get_import_matrix_mapper(**kwargs)
    return SHARKapiDataHolder(header_mapper=mapper, **kwargs)


def get_polars_shark_data_holder(path: str | pathlib.Path, **kwargs) \
        -> PolarsSharkDataHolder:
    return PolarsSharkDataHolder(path=path, **kwargs)


def file_is_from_shark(path: str | pathlib.Path) -> bool:
    """
    For now just a simple check if file is txt.
    """
    path = pathlib.Path(path)
    if path.exists() and path.suffix == '.txt':
        return True
    return False
