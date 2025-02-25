import functools
import logging
import os
import pathlib
from typing import Protocol

from sharkadm import config_paths
from .column_views import ColumnViews
from .custom_id import CustomIdsHandler
from .data_type_mapper import DataTypeMapper
from .delivery_note_mapper import DeliveryNoteMapper
from .import_matrix import ImportMatrixConfig, ImportMatrixMapper

logger = logging.getLogger(__name__)

DATA_STRUCTURES = ['row', 'column']

CONFIG_ENV = 'NODC_CONFIG'

# CONFIG_FILE_NAMES = [
#     'column_views.txt',
#     'data_type_mapping.yaml',
#     'delivery_note_mapping.txt',
#     'delivery_note_status.yaml',
#     'physical_chemical_mapping.txt',
#
#     'ids/epibenthos_id.yaml',
#     'ids/physicalchemichal_id.yaml',
#     'ids/phytoplankton_id.yaml',
#     'ids/zoobenthos_id.yaml',
#
#     'import_matrix/import_matrix_bacterioplankton.txt',
#     'import_matrix/import_matrix_chlorophyll.txt',
#     'import_matrix/import_matrix_eelgrass.txt',
#     'import_matrix/import_matrix_epibenthos.txt',
#     'import_matrix/import_matrix_epibenthos_dropvideo.txt',
#     'import_matrix/import_matrix_greyseal.txt',
#     'import_matrix/import_matrix_harbourporpoise.txt',
#     'import_matrix/import_matrix_harbourseal.txt',
#     'import_matrix/import_matrix_jellyfish.txt',
#     'import_matrix/import_matrix_physicalchemical.txt',
#     'import_matrix/import_matrix_phytoplankton.txt',
#     'import_matrix/import_matrix_picoplankton.txt',
#     'import_matrix/import_matrix_plankton_barcoding.txt',
#     'import_matrix/import_matrix_primaryproduction.txt',
#     'import_matrix/import_matrix_profile.txt',
#     'import_matrix/import_matrix_ringedseal.txt',
#     'import_matrix/import_matrix_sealpathology.txt',
#     'import_matrix/import_matrix_sedimentation.txt',
#     'import_matrix/import_matrix_zoobenthos.txt',
#     'import_matrix/import_matrix_zooplankton.txt',
#
#     'workflow/workflow_dv.yaml',
#     'workflow/workflow_dv_phytoplankton.yaml',
#     'workflow/workflow_dv_validation.yaml',
#     'workflow/workflow_ifcb_visualization.yaml',
#     ]


CONFIG_DIRECTORY = None
if os.getenv(CONFIG_ENV) and pathlib.Path(os.getenv(CONFIG_ENV)).exists():
    CONFIG_DIRECTORY = pathlib.Path(os.getenv(CONFIG_ENV))


def get_config_path(name: str = None) -> pathlib.Path:
    if not CONFIG_DIRECTORY:
        raise NotADirectoryError(f'Config directory not found. Environment path {CONFIG_ENV} does not seem to be set.')
    if not name:
        return CONFIG_DIRECTORY
    path = CONFIG_DIRECTORY / name
    if not path.exists():
        raise FileNotFoundError(f'Could not find config file {name}')
    return path


class DataHolderProtocol(Protocol):
    data_type = None
    header_mapper = None


# # @functools.cache
# def get_column_info_config(path: str | pathlib.Path = None) -> ColumnInfoConfig:
#     path = path or adm_config_paths('column_info')
#     return ColumnInfoConfig(path)


# @functools.cache
def get_column_views_config(path: str | pathlib.Path = None) -> ColumnViews:
    path = path or DEFAULT_COLUMN_VIEWS_PATH
    return ColumnViews(path)


# def get_import_matrix_list(directory: str | pathlib.Path = None) -> list[str]:
#     directory = directory or DEFAULT_IMPORT_MATRIX_DIRECTORY
#     return [path.stem.split('_', 2)[-1] for path in pathlib.Path(directory).iterdir()]

# @functools.cache
def get_import_matrix_config(data_type: str) -> ImportMatrixConfig | None:
    for name, path in import_matrix_paths.items():
        if data_type == name:
            return ImportMatrixConfig(path,
                                      data_type=data_type,
                                      )


# @functools.cache
def get_import_matrix_mapper(data_type: str, import_column: str, directory: str | pathlib.Path = None, **kwargs) -> ImportMatrixMapper | None:
    config = get_import_matrix_config(data_type)
    if not config:
        return
    return config.get_mapper(import_column)


# @functools.cache
def get_header_mapper_from_data_holder(data_holder: DataHolderProtocol, import_column: str) -> ImportMatrixMapper | None:
    if import_column == 'original':
        return data_holder.header_mapper
    return get_import_matrix_mapper(data_holder.data_type, import_column)


# @functools.cache
def get_custom_id_handler(config_directory: str | pathlib.Path = None):
    config_directory = config_directory or adm_config_paths('ids')
    return CustomIdsHandler(config_directory)


# @functools.cache
def get_delivery_note_mapper(path: str | pathlib.Path = None) -> DeliveryNoteMapper:
    path = path or adm_config_paths('delivery_note_mapping')
    return DeliveryNoteMapper(path)


def get_data_type_mapper(path: str | pathlib.Path = None) -> DataTypeMapper:
    path = path or adm_config_paths('data_type_mapping')
    return DataTypeMapper(path)


def get_mapper_data_type_to_internal(path: str | pathlib.Path = None) -> DataTypeMapper:
    path = path or adm_config_paths('mapper_data_type_to_internal')
    return DataTypeMapper(path)


# @functools.cache
def get_all_data_types() -> list[str]:
    # return [path.stem.split('_', 2)[-1].lower() for path in adm_config_paths('import_matrix').iterdir()]
    return [path.stem.split('_', 2)[-1].lower() for path in import_matrix_paths.values()]


# # @functools.cache
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


def get_adm_config_paths():
    root = CONFIG_DIRECTORY / 'sharkadm'
    return config_paths.ConfigPaths(root)


def get_import_matrix_config_paths() -> dict[str, pathlib.Path]:
    paths = {}
    for path in CONFIG_DIRECTORY.iterdir():
        if 'import_matrix' not in path.name:
            continue
        # key = path.stem.split('import_matrix_')[1].replace('_', '').lower()
        # paths[key] = path
        key = path.stem.split('_', 2)[-1]
        # This key is the valid "data_type_internal
        paths[key] = path
    return paths


DEFAULT_COLUMN_VIEWS_PATH = CONFIG_DIRECTORY / 'column_views.txt'
adm_config_paths = get_adm_config_paths()
import_matrix_paths = get_import_matrix_config_paths()
mapper_data_type_to_internal = get_mapper_data_type_to_internal()


if __name__ == '__main__':
    pass