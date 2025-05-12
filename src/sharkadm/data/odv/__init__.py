import pathlib
from typing import Union

from sharkadm import config

from .odv_data_holder import PolarsOdvDataHolder


def get_polars_odv_data_holder(path: str | pathlib.Path, **kwargs) -> PolarsOdvDataHolder:
    path = pathlib.Path(path)
    mapper = config.get_import_matrix_mapper(
        data_type="physicalchemical", import_column="ODV"
    )
    return PolarsOdvDataHolder(path=path, header_mapper=mapper, **kwargs)


def path_has_or_is_odv_data(path: str | pathlib.Path) -> Union[pathlib.Path, False]:
    """Returns True if path is odv file or if
    path is a directory containing odv file/files. Else returns False.
    Does not look deep in file structure.
    """

    def is_odv_file(pp):
        with open(pp) as fid:
            for line in fid:
                if not line.startswith("//"):
                    return False
                if "subject><object>SDN:P01" in line:
                    return True

    root_path = pathlib.Path(path)
    if not root_path.exists():
        return False

    if root_path.is_file():
        if is_odv_file(root_path):
            return True
        return False
    for p in root_path.iterdir():
        if is_odv_file(p):
            return True
    return False
