# ruff: noqa: F401

import functools
import inspect
import logging
import pathlib
from typing import Type

from sharkadm import sharkadm_exceptions, utils
from sharkadm.data.archive import (
    directory_is_archive,
    get_archive_data_holder,
    get_polars_archive_data_holder,
)
from sharkadm.data.data_holder import DataHolder, PandasDataHolder, PolarsDataHolder
from sharkadm.data.df import PandasDataFrameDataHolder,PolarsDataFrameDataHolder
from sharkadm.data.dv_template import DvTemplateDataHolder, get_dv_template_data_holder
from sharkadm.data.lims import (
    LimsDataHolder,
    directory_is_lims,
    get_lims_data_holder,
    get_polars_lims_data_holder,
)
from sharkadm.data.profile import (
    get_polars_profile_standard_format_data_holder,
    path_has_or_is_standard_format_profile_data,
)
from sharkadm.data.odv import (
    get_polars_odv_data_holder,
    path_has_or_is_odv_data,
)
from sharkadm.data.shark import (
    get_shark_api_data_holder,
    get_polars_shark_data_holder,
    file_is_from_shark
)
from sharkadm.data.zip_archive import (
    get_polars_zip_archive_data_holder,
    get_zip_archive_data_holder,
    path_is_zip_archive,
)

logger = logging.getLogger(__name__)


@functools.cache
def get_data_holder_list() -> list[str]:
    """Returns a sorted list of name of all available data_holders"""
    return sorted(utils.get_all_class_children_names(PandasDataHolder))


def get_data_holders() -> dict[str, Type[PandasDataHolder]]:
    """Returns a dictionary with data_holders"""
    return utils.get_all_class_children(PandasDataHolder)


def get_polars_data_holders() -> dict[str, Type[PolarsDataHolder]]:
    """Returns a dictionary with data_holders"""
    return utils.get_all_class_children(PolarsDataHolder)


def get_data_holder_object(trans_name: str, **kwargs) -> PandasDataHolder:
    """Returns DataHolder object that matches the given data_holder names"""
    all_trans = get_data_holders()
    tran = all_trans[trans_name]
    return tran(**kwargs)


def get_data_holders_description() -> dict[str, str]:
    """Returns a dictionary with data_holder name as key and the description as value"""
    result = dict()
    for name, tran in get_data_holders().items():
        result[name] = tran.get_data_holder_description()
    return result


def get_data_holders_info() -> dict:
    result = dict()
    for name, tran in get_data_holders().items():
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


def get_data_holder(
    path: str | pathlib.Path | None = None, sharkweb: bool = False, **kwargs
) -> PandasDataHolder:
    if path:
        path = pathlib.Path(path)
        if not path.exists() and path.suffix:
            raise FileNotFoundError(path)
        if not path.exists():
            raise NotADirectoryError(path)
        if path.suffix == ".xlsx":
            return get_dv_template_data_holder(path)
        if path_is_zip_archive(path):
            return get_zip_archive_data_holder(path)
        if path.is_dir():
            archive_directory = directory_is_archive(path)
            if archive_directory:
                return get_archive_data_holder(archive_directory)
        lims_directory = directory_is_lims(path)
        if lims_directory:
            return get_lims_data_holder(lims_directory)
    if sharkweb:
        return get_shark_api_data_holder(**kwargs)
    raise sharkadm_exceptions.DataHolderError(f"Could not find dataholder for: {path}")


def get_polars_data_holder(
    path: str | pathlib.Path | None = None, sharkweb: bool = False, **kwargs
) -> PolarsDataHolder:
    if path:
        path = pathlib.Path(path)
        if not path.exists() and path.suffix:
            raise FileNotFoundError(path)
        if not path.exists():
            raise NotADirectoryError(path)
        # if path.suffix == ".xlsx":
        #     return get_dv_template_data_holder(path)
        if path_is_zip_archive(path):
            return get_polars_zip_archive_data_holder(path)
        if path.is_file():
            if file_is_from_shark(path):
                return get_polars_shark_data_holder(path, **kwargs)
        if path.is_dir():
            archive_directory = directory_is_archive(path)
            if archive_directory:
                return get_polars_archive_data_holder(archive_directory)
            if path_has_or_is_standard_format_profile_data(path):
                return get_polars_profile_standard_format_data_holder(path, **kwargs)
        lims_directory = directory_is_lims(path)
        if lims_directory:
            return get_polars_lims_data_holder(lims_directory)
        if path_has_or_is_standard_format_profile_data(path):
            return get_polars_profile_standard_format_data_holder(path, **kwargs)
        if path_has_or_is_odv_data(path):
            return get_polars_odv_data_holder(path, **kwargs)
    # if sharkweb:
    #     return get_shark_api_data_holder(**kwargs)
    raise sharkadm_exceptions.DataHolderError(f"Could not find dataholder for: {path}")


def get_valid_data_holders(
    valid: tuple[str, ...] | None = None, invalid: tuple[str, ...] | None = None
) -> list[str]:
    if not any([valid, invalid]):
        return get_data_holder_list()
    data_holder_list_lower = [item.lower() for item in get_data_holder_list()]
    data_holders = []
    if valid:
        valid_lower = [item.lower() for item in valid]
        for item, item_lower in zip(get_data_holder_list(), data_holder_list_lower):
            if item_lower in valid_lower:
                data_holders.append(item)
        return data_holders
    elif invalid:
        invalid_lower = [item.lower() for item in invalid]
        for item, item_lower in zip(get_data_holder_list(), data_holder_list_lower):
            if item_lower not in invalid_lower:
                data_holders.append(item)
        return data_holders


def is_valid_data_holder(
    data_holder: Type[DataHolder],
    valid: list[str] | None = None,
    invalid: list[str] | None = None,
) -> bool:
    if not any([valid, invalid]):
        return True
    holders = get_data_holders()
    if any([val for val in invalid if isinstance(data_holder, holders[val])]):
        return False
    if any([val for val in valid if isinstance(data_holder, holders[val])]):
        return True
    return True


def is_valid_polars_data_holder(
    data_holder: PolarsDataHolder,
    valid: tuple[str, ...] | None = None,
    invalid: tuple[str, ...] | None = None,
) -> bool:
    if not any([valid, invalid]):
        return True
    holders = get_data_holders()
    if any([val for val in invalid if isinstance(data_holder, holders[val])]):
        return False
    if any([val for val in valid if isinstance(data_holder, holders[val])]):
        return True
    return True
