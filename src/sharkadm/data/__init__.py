import functools
import inspect
import pathlib
from typing import Type

import polars as pl

from sharkadm import sharkadm_exceptions, utils
from sharkadm.data.archive import (
    directory_is_archive,
    get_polars_archive_data_holder,
)
from sharkadm.data.data_holder import PolarsDataHolder
from sharkadm.data.df import (
    PolarsDataFrameDataHolder,
    get_data_frame_data_holder,
)
from sharkadm.data.dv_template import get_polars_dv_template_data_holder
from sharkadm.data.lims import (
    PolarsLimsDataHolder,
    get_polars_lims_data_holder,
    is_lims_directory,
)
from sharkadm.data.odv import (
    get_polars_odv_data_holder,
    path_has_or_is_odv_data,
)
from sharkadm.data.profile import (
    get_polars_profile_cnv_data_holder,
    get_polars_profile_standard_format_data_holder,
    path_has_or_is_cnv_profile_data,
    path_has_or_is_standard_format_profile_data,
)
from sharkadm.data.shark import file_is_from_shark, get_polars_shark_data_holder
from sharkadm.data.zip_archive import (
    get_polars_zip_archive_data_holder,
    path_is_zip_archive,
)


@functools.cache
def get_data_holder_list() -> list[str]:
    """Returns a sorted list of name of all available data_holders"""
    return sorted(utils.get_all_class_children_names(PolarsDataHolder))


def get_polars_data_holders() -> dict[str, Type[PolarsDataHolder]]:
    """Returns a dictionary with data_holders"""
    return utils.get_all_class_children(PolarsDataHolder)


def get_data_holder_object(trans_name: str, **kwargs) -> PolarsDataHolder:
    """Returns DataHolder object that matches the given data_holder names"""
    all_trans = get_polars_data_holders()
    tran = all_trans[trans_name]
    return tran(**kwargs)


def get_data_holders_description() -> dict[str, str]:
    """Returns a dictionary with data_holder name as key and the description as value"""
    result = dict()
    for name, tran in get_polars_data_holders().items():
        result[name] = tran.get_data_holder_description()
    return result


def get_data_holders_info() -> dict:
    result = dict()
    for name, tran in get_polars_data_holders().items():
        result[name] = dict()
        result[name]["name"] = name
        result[name]["description"] = tran.get_data_holder_description()
        result[name]["kwargs"] = dict()
        for key, value in inspect.signature(tran.__init__).parameters.items():
            if key in ["self", "kwargs"]:
                continue
            result[name]["kwargs"][key] = value.default
    return result


def get_data_holders_description_text() -> str:
    info = get_data_holders_description()
    line_length = 100
    lines = list()
    lines.append("=" * line_length)
    lines.append("Available data_holders:")
    lines.append("-" * line_length)
    for key in sorted(info):
        lines.append(f"{key.ljust(30)}{info[key]}")
    lines.append("=" * line_length)
    return "\n".join(lines)


def print_data_holders_description() -> None:
    """Prints all data_holders on screen"""
    print(get_data_holders_description_text())


def write_data_holders_description_to_file(path: str | pathlib.Path) -> None:
    """Prints all data_holders on screen"""
    with open(path, "w") as fid:
        fid.write(get_data_holders_description_text())


def get_polars_data_holder(
    path: str | pathlib.Path | pl.DataFrame | None = None,
    sharkweb: bool = False,
    **kwargs,
) -> PolarsDataHolder:
    if isinstance(path, pl.DataFrame):
        return get_data_frame_data_holder(path)
    if path:
        path = pathlib.Path(path)
        if not path.exists() and path.suffix:
            raise FileNotFoundError(path)
        if not path.exists():
            raise NotADirectoryError(path)
        if path.suffix == ".xlsx":
            return get_polars_dv_template_data_holder(path)
        if path_is_zip_archive(path):
            return get_polars_zip_archive_data_holder(path, **kwargs)
        if lims_directory := is_lims_directory(path):
            return get_polars_lims_data_holder(lims_directory, **kwargs)
        if path.is_file():
            if file_is_from_shark(path):
                return get_polars_shark_data_holder(path, **kwargs)
        if path.is_dir():
            if path_has_or_is_standard_format_profile_data(path):
                return get_polars_profile_standard_format_data_holder(path, **kwargs)
            if path_has_or_is_cnv_profile_data(path):
                return get_polars_profile_cnv_data_holder(path, **kwargs)
            archive_directory = directory_is_archive(path)
            if archive_directory:
                return get_polars_archive_data_holder(archive_directory)
        if path_has_or_is_standard_format_profile_data(path):
            return get_polars_profile_standard_format_data_holder(path, **kwargs)
        if path_has_or_is_cnv_profile_data(path):
            return get_polars_profile_cnv_data_holder(path, **kwargs)
        if path_has_or_is_odv_data(path):
            return get_polars_odv_data_holder(path, **kwargs)
    # if sharkweb:
    #     return get_shark_api_data_holder(**kwargs)
    raise sharkadm_exceptions.DataHolderError(f"Could not find dataholder for: {path}")


def is_valid_polars_data_holder(
    data_holder: PolarsDataHolder,
    valid: tuple[str, ...] | None = None,
    invalid: tuple[str, ...] | None = None,
) -> bool:
    if not any([valid, invalid]):
        return True
    holders = get_polars_data_holders()
    if any([val for val in invalid if isinstance(data_holder, holders[val])]):
        return False
    print(f"{valid=}")
    print(f"{holders.keys()=}")
    if any([val for val in valid if isinstance(data_holder, holders[val])]):
        return True
    return True
