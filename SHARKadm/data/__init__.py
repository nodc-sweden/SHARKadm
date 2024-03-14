import functools
import pathlib
import logging
from typing import Type
import inspect

# from SHARKadm import config
# from SHARKadm.data import data_source
# from SHARKadm.data.archive import delivery_note


from .data_holder import DataHolder
from .archive import get_archive_data_holder, directory_is_archive
from .lims import get_lims_data_holder, directory_is_lims
from .zip_archive import get_zip_archive_data_holder, path_is_zip_archive
from .dv_template import get_dv_template_data_holder
from .sharkweb import get_sharkweb_data_holder

from .archive import *
from .lims import LimsDataHolder
from .dv_template import DvTemplateDataHolder
from SHARKadm import utils

logger = logging.getLogger(__name__)


@functools.cache
def get_data_holder_list() -> list[str]:
    """Returns a sorted list of name of all available data_holders"""
    return sorted(utils.get_all_class_children_names(DataHolder))


def get_data_holders() -> dict[str, Type[DataHolder]]:
    """Returns a dictionary with data_holders"""
    trans = {}
    for cls in DataHolder.__subclasses__():
        trans[cls.__name__] = cls
    return trans


def get_data_holder_object(trans_name: str, **kwargs) -> DataHolder:
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
        result[name]['name'] = name
        result[name]['description'] = tran.get_data_holder_description()
        result[name]['kwargs'] = dict()
        for key, value in inspect.signature(tran.__init__).parameters.items():
            if key in ['self', 'kwargs']:
                continue
            result[name]['kwargs'][key] = value.default
    return result


def get_data_holders_description_text() -> str:
    info = get_data_holders_description()
    line_length = 100
    lines = list()
    lines.append('=' * line_length)
    lines.append('Available data_holders:')
    lines.append('-' * line_length)
    for key in sorted(info):
        lines.append(f'{key.ljust(30)}{info[key]}')
    lines.append('=' * line_length)
    return '\n'.join(lines)


def print_data_holders_description() -> None:
    """Prints all data_holders on screen"""
    print(get_data_holders_description_text())


def write_data_holders_description_to_file(path: str | pathlib.Path) -> None:
    """Prints all data_holders on screen"""
    with open(path, 'w') as fid:
        fid.write(get_data_holders_description_text())


def get_data_holder(path: str | pathlib.Path = None, sharkweb: bool = False, **kwargs) -> DataHolder:
    if path:
        path = pathlib.Path(path)
        if path.suffix == '.xlsx':
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
        return get_sharkweb_data_holder(**kwargs)
    raise sharkadm_exceptions.DataHolderError(f'Could not find dataholder for: {path}')


def get_valid_data_holders(valid: list[str] | None = None,
                           invalid: list[str] | None = None) -> list[str]:
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







