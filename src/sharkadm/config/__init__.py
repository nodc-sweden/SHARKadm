import pathlib
import functools
import sys
from typing import Protocol

from sharkadm import adm_config_paths
from .import_matrix import ImportMatrixConfig, ImportMatrixMapper
from .column_info import ColumnInfoConfig
from .column_views import ColumnViews
from .custom_id import CustomIdsHandler
from .data_type_mapper import DataTypeMapper
from .delivery_note_mapper import DeliveryNoteMapper

DATA_FORMATS = ['row', 'column']


if getattr(sys, 'frozen', False):
    THIS_DIR = pathlib.Path(sys.executable).parent
else:
    THIS_DIR = pathlib.Path(__file__).parent


class DataHolderProtocol(Protocol):
    data_type = None
    header_mapper = None


@functools.cache
def get_column_info_config(path: str | pathlib.Path = None) -> ColumnInfoConfig:
    path = path or adm_config_paths('column_info')
    return ColumnInfoConfig(path)


@functools.cache
def get_column_views_config(path: str | pathlib.Path = None) -> ColumnViews:
    path = path or adm_config_paths('column_views')
    return ColumnViews(path)


# def get_import_matrix_list(directory: str | pathlib.Path = None) -> list[str]:
#     directory = directory or DEFAULT_IMPORT_MATRIX_DIRECTORY
#     return [path.stem.split('_', 2)[-1] for path in pathlib.Path(directory).iterdir()]

@functools.cache
def get_import_matrix_config(data_type: str, directory: str | pathlib.Path = None) -> ImportMatrixConfig | None:
    directory = directory or adm_config_paths('import_matrix')
    for path in pathlib.Path(directory).iterdir():
        # if d_type_mapper.get(data_type) in path.name:
        if data_type.lower() in path.name.lower():
            return ImportMatrixConfig(path,
                                      data_type=data_type.lower(),
                                      )


@functools.cache
def get_import_matrix_mapper(data_type: str, import_column: str, directory: str | pathlib.Path = None, **kwargs) -> ImportMatrixMapper | None:
    directory = directory or adm_config_paths('import_matrix')
    for path in pathlib.Path(directory).iterdir():
        if data_type.lower() in path.name.lower():
            config = ImportMatrixConfig(path,
                                      data_type=data_type.lower(),
                                      )
            return config.get_mapper(import_column)


@functools.cache
def get_header_mapper_from_data_holder(data_holder: DataHolderProtocol, import_column: str) -> ImportMatrixMapper | None:
    if import_column == 'original':
        return data_holder.header_mapper
    return get_import_matrix_mapper(data_holder.data_type, import_column)


@functools.cache
def get_custom_id_handler(config_directory: str | pathlib.Path = None):
    config_directory = config_directory or adm_config_paths('ids')
    return CustomIdsHandler(config_directory)


@functools.cache
def get_delivery_note_mapper(path: str | pathlib.Path = None) -> DeliveryNoteMapper:
    path = path or adm_config_paths('delivery_note_mapping')
    return DeliveryNoteMapper(path)


@functools.cache
def get_all_data_types() -> list[str]:
    return [path.stem.split('_', 2)[-1].lower() for path in adm_config_paths('import_matrix').iterdir()]


# @functools.cache
def get_valid_data_types(valid: list[str] | None = None,
                         invalid: list[str] | None = None) -> list[str]:
    if not any([valid, invalid]):
        return get_all_data_types()
    if valid:
        return [item.lower() for item in valid if item.lower() in get_all_data_types()]
    elif invalid:
        invalid_lower = [item.lower() for item in invalid]
        return [item for item in get_all_data_types() if item not in invalid_lower]


def get_valid_data_formats(valid: list[str] | None = None,
                           invalid: list[str] | None = None) -> list[str]:
    if not any([valid, invalid]):
        return DATA_FORMATS
    if valid:
        return [item.lower() for item in valid if item.lower() in DATA_FORMATS]
    elif invalid:
        invalid_lower = [item.lower() for item in invalid]
        return [item for item in DATA_FORMATS if item not in invalid_lower]


if __name__ == '__main__':
    data = {}
    lev = 'visit'
    h = get_custom_id_handler()
    lh = h.get_level_handler('phytoplankton', 'visit')