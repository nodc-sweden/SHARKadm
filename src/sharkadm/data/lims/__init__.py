import pathlib
from typing import Union

from sharkadm import config

from .lims_data_holder import LimsDataHolder, PolarsLimsDataHolder


def get_polars_lims_data_holder(
    path: str | pathlib.Path, **kwargs
) -> PolarsLimsDataHolder:
    path = pathlib.Path(path)
    if path.name == "data.txt" and path.parent.name.lower() == "raw_data":
        path = path.parent.parent
    if path.name.lower() == "raw_data":
        path = path.parent
    mapper = None
    if not kwargs.get("keep_header"):
        mapper = config.get_import_matrix_mapper(
            data_type="physicalchemical", import_column="LIMS"
        )
    return PolarsLimsDataHolder(lims_root_directory=path, header_mapper=mapper)


def get_lims_data_holder(path: str | pathlib.Path) -> LimsDataHolder:
    path = pathlib.Path(path)
    if path.name == "data.txt" and path.parent.name.lower() == "raw_data":
        path = path.parent.parent
    if path.name.lower() == "raw_data":
        path = path.parent
    mapper = config.get_import_matrix_mapper(
        data_type="physicalchemical", import_column="LIMS"
    )
    return LimsDataHolder(lims_root_directory=path, header_mapper=mapper)


def is_lims_directory(directory: str | pathlib.Path) -> Union[pathlib.Path, False]:
    """Returns path to lims directory if it is recognised as a lims directory. Else
    returns False
    directory"""
    directory = pathlib.Path(directory)
    if directory.name == "data.txt" and directory.parent.name.lower() == "raw_data":
        return directory.parent.parent
    if directory.name.lower() == "raw_data":
        return directory.parent
    raw_data_directory = directory / "Raw_data"
    if raw_data_directory.exists():
        return directory
    return False
