import pathlib
from typing import Union

from .zip_archive_data_holder import PolarsZipArchiveDataHolder, ZipArchiveDataHolder


def get_zip_archive_data_holder(path: str | pathlib.Path) -> ZipArchiveDataHolder:
    return ZipArchiveDataHolder(path)


def get_polars_zip_archive_data_holder(
    path: str | pathlib.Path,
) -> PolarsZipArchiveDataHolder:
    return PolarsZipArchiveDataHolder(path)


def path_is_zip_archive(path: str | pathlib.Path) -> Union[pathlib.Path, False]:
    """Returns path to zip archive if it is recognised as a zip archive.
    Else returns False"""
    path = pathlib.Path(path)
    if path.suffix == ".zip" and path.stem.startswith("SHARK"):
        return path
    return False
