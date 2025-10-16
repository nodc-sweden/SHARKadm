import pathlib
from typing import Union

from sharkadm import config
from sharkadm.data.profile.cnv_data_holder import PolarsCnvDataHolder
from sharkadm.data.profile.standard_format_data_holder import (
    PolarsProfileStandardFormatDataHolder,
)


def get_polars_profile_standard_format_data_holder(
    path: str | pathlib.Path, **kwargs
) -> PolarsProfileStandardFormatDataHolder:
    path = pathlib.Path(path)
    mapper = config.get_import_matrix_mapper(data_type="profile", import_column="PROFILE")
    return PolarsProfileStandardFormatDataHolder(
        path=path, header_mapper=mapper, **kwargs
    )


def path_has_or_is_standard_format_profile_data(
    path: str | pathlib.Path,
) -> Union[pathlib.Path, False]:
    """Returns True if path is standard format file or if
    path is a directory containing standard format file/files. Else returns False.
    Does not look deep in file structure.
    """
    root_path = pathlib.Path(path)
    if not root_path.exists():
        return False

    if root_path.is_file():
        if root_path.suffix == ".txt" and root_path.name.upper().startswith("SBE"):
            return True
        if root_path.suffix == ".txt" and root_path.name.lower().startswith(
            "ctd_profile"
        ):
            return True
        return False
    for p in root_path.iterdir():
        if p.suffix == ".txt" and p.name.upper().startswith("SBE"):
            return True
        if p.suffix == ".txt" and p.name.lower().startswith("ctd_profile"):
            return True
    return False


def path_has_or_is_cnv_profile_data(
    path: str | pathlib.Path,
) -> Union[pathlib.Path, False]:
    """Returns True if path is standard format file or if
    path is a directory containing standard format file/files. Else returns False.
    Does not look deep in file structure.
    """
    root_path = pathlib.Path(path)
    if not root_path.exists():
        return False

    if root_path.is_file():
        if root_path.suffix == ".cnv":
            return True
        return False
    for p in root_path.iterdir():
        if p.suffix == ".cnv":
            return True
    return False


def get_polars_profile_cnv_data_holder(
    path: str | pathlib.Path, **kwargs
) -> PolarsCnvDataHolder:
    path = pathlib.Path(path)
    mapper = config.get_import_matrix_mapper(data_type="profile", import_column="PROFILE")
    return PolarsCnvDataHolder(path=path, header_mapper=mapper, **kwargs)
