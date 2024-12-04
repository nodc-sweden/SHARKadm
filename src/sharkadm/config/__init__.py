import pathlib
import functools
import sys
from typing import Protocol

from sharkadm import config_paths
from .import_matrix import ImportMatrixConfig, ImportMatrixMapper
# from .column_info import ColumnInfoConfig
from .column_views import ColumnViews
from .custom_id import CustomIdsHandler
from .data_type_mapper import DataTypeMapper
from .delivery_note_mapper import DeliveryNoteMapper

import functools
import logging
import pathlib
import os

import requests
import ssl


logger = logging.getLogger(__name__)

DATA_STRUCTURES = ['row', 'column']

CONFIG_SUBDIRECTORY = 'sharkadm'
CONFIG_FILE_NAMES = [
    'column_views.txt',
    'data_type_mapping.yaml',
    'delivery_note_mapping.txt',
    'delivery_note_status.yaml',
    'physical_chemical_mapping.txt',

    'ids/epibenthos_id.yaml',
    'ids/physicalchemichal_id.yaml',
    'ids/phytoplankton_id.yaml',
    'ids/zoobenthos_id.yaml',

    'import_matrix/import_matrix_bacterioplankton.txt',
    'import_matrix/import_matrix_chlorophyll.txt',
    'import_matrix/import_matrix_eelgrass.txt',
    'import_matrix/import_matrix_epibenthos.txt',
    'import_matrix/import_matrix_epibenthos_dropvideo.txt',
    'import_matrix/import_matrix_greyseal.txt',
    'import_matrix/import_matrix_harbourporpoise.txt',
    'import_matrix/import_matrix_harbourseal.txt',
    'import_matrix/import_matrix_jellyfish.txt',
    'import_matrix/import_matrix_physicalchemical.txt',
    'import_matrix/import_matrix_phytoplankton.txt',
    'import_matrix/import_matrix_picoplankton.txt',
    'import_matrix/import_matrix_plankton_barcoding.txt',
    'import_matrix/import_matrix_primaryproduction.txt',
    'import_matrix/import_matrix_profile.txt',
    'import_matrix/import_matrix_ringedseal.txt',
    'import_matrix/import_matrix_sealpathology.txt',
    'import_matrix/import_matrix_sedimentation.txt',
    'import_matrix/import_matrix_zoobenthos.txt',
    'import_matrix/import_matrix_zooplankton.txt',

    'workflow/workflow_dv.yaml',
    'workflow/workflow_dv_phytoplankton.yaml',
    'workflow/workflow_dv_validation.yaml',
    'workflow/workflow_ifcb_visualization.yaml',
    ]


CONFIG_DIRECTORY = None
if os.getenv('NODC_CONFIG') and pathlib.Path(os.getenv('NODC_CONFIG')).exists():
    CONFIG_DIRECTORY = pathlib.Path(os.getenv('NODC_CONFIG')) / CONFIG_SUBDIRECTORY
TEMP_CONFIG_DIRECTORY = pathlib.Path.home() / 'temp_nodc_config' / CONFIG_SUBDIRECTORY
TEMP_CONFIG_DIRECTORY.mkdir(exist_ok=True, parents=True)


CONFIG_URL = r'https://raw.githubusercontent.com/nodc-sweden/nodc_config/refs/heads/main/' + f'{CONFIG_SUBDIRECTORY}/'


def get_config_root_directory() -> pathlib.Path:
    return CONFIG_DIRECTORY or TEMP_CONFIG_DIRECTORY


def get_config_path(name: str) -> pathlib.Path:
    if name not in CONFIG_FILE_NAMES:
        raise FileNotFoundError(f'No config file with name "{name}" exists')
    if CONFIG_DIRECTORY:
        path = CONFIG_DIRECTORY / name
        if path.exists():
            return path
    temp_path = TEMP_CONFIG_DIRECTORY / name
    if temp_path.exists():
        return temp_path
    update_config_file(temp_path)
    if temp_path.exists():
        return temp_path
    raise FileNotFoundError(f'Could not find config file {name}')


def update_config_file(path: pathlib.Path) -> None:
    path.parent.mkdir(exist_ok=True, parents=True)
    url = CONFIG_URL + str(path.relative_to(TEMP_CONFIG_DIRECTORY)).replace('\\', '/')
    print(f'{url=}')
    try:
        res = requests.get(url, verify=ssl.CERT_NONE)
        with open(path, 'w', encoding='utf8') as fid:
            fid.write(res.text)
            logger.info(f'Config file "{path.name}" updated from {url}')
    except requests.exceptions.ConnectionError:
        logger.warning(f'Connection error. Could not update config file {path.name}')
        raise


def update_config_files() -> None:
    """Downloads config files from github"""
    for name in CONFIG_FILE_NAMES:
        target_path = TEMP_CONFIG_DIRECTORY / name
        update_config_file(target_path)


class DataHolderProtocol(Protocol):
    data_type = None
    header_mapper = None


# @functools.cache
# def get_column_info_config(path: str | pathlib.Path = None) -> ColumnInfoConfig:
#     path = path or adm_config_paths('column_info')
#     return ColumnInfoConfig(path)


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


def get_valid_data_structures(valid: list[str] | None = None,
                           invalid: list[str] | None = None) -> list[str]:
    if not any([valid, invalid]):
        return DATA_STRUCTURES
    if valid:
        return [item.lower() for item in valid if item.lower() in DATA_STRUCTURES]
    elif invalid:
        invalid_lower = [item.lower() for item in invalid]
        return [item for item in DATA_STRUCTURES if item not in invalid_lower]


def get_config_paths():
    root = get_config_root_directory()
    if not root.exists() or not list(root.iterdir()):
        update_config_files()
    return config_paths.ConfigPaths(root)


adm_config_paths = get_config_paths()


if __name__ == '__main__':
    update_config_files()