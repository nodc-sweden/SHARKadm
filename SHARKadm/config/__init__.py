import pathlib
import functools
import sys

from .import_config import ImportMatrixConfig
from .column_info import ColumnInfoConfig
from .column_views import ColumnViews
from .sharkadm_id import SharkadmIdsHandler
from .data_type_mapper import DataTypeMapper
from .physical_chemical_mapper import PhysicalChemicalMapper


if getattr(sys, 'frozen', False):
    THIS_DIR = pathlib.Path(sys.executable).parent
else:
    THIS_DIR = pathlib.Path(__file__).parent


ID_CONFIG_DIRECTORY = pathlib.Path(THIS_DIR, 'etc', 'ids')
DEFAULT_IMPORT_MATRIX_DIRECTORY = pathlib.Path(THIS_DIR, 'etc', 'import_matrix')

DEFAULT_PHYSICAL_CHEMICAL_MAPPER = pathlib.Path(THIS_DIR, 'etc', 'physical_chemical_mapping.txt')

DEFAULT_COLUMN_INFO_PATH = pathlib.Path(THIS_DIR, 'etc', 'column_info.yaml')
DEFAULT_COLUMN_VIEWS_PATH = pathlib.Path(THIS_DIR, 'etc', 'column_views.txt')

# DEFAULT_DATA_TYPE_MAPPING_PATH = pathlib.Path(THIS_DIR, 'etc', 'data_type_mapping.yaml')


@functools.cache
def get_physical_chemical_mapper(path: str | pathlib.Path = None) -> PhysicalChemicalMapper:
    path = path or DEFAULT_PHYSICAL_CHEMICAL_MAPPER
    return PhysicalChemicalMapper(path)


@functools.cache
def get_column_info_config(path: str | pathlib.Path = None) -> ColumnInfoConfig:
    path = path or DEFAULT_COLUMN_INFO_PATH
    return ColumnInfoConfig(path)


@functools.cache
def get_column_views_config(path: str | pathlib.Path = None) -> ColumnViews:
    path = path or DEFAULT_COLUMN_VIEWS_PATH
    return ColumnViews(path)


# def get_import_matrix_list(directory: str | pathlib.Path = None) -> list[str]:
#     directory = directory or DEFAULT_IMPORT_MATRIX_DIRECTORY
#     return [path.stem.split('_', 2)[-1] for path in pathlib.Path(directory).iterdir()]

@functools.cache
def get_import_matrix_config(data_type: str, directory: str | pathlib.Path = None,
                             data_type_mapping_path: str | pathlib.Path = None) -> ImportMatrixConfig:
    directory = directory or DEFAULT_IMPORT_MATRIX_DIRECTORY
    # d_type_mapper = get_data_type_mapper(data_type_mapping_path)
    for path in pathlib.Path(directory).iterdir():
        # if d_type_mapper.get(data_type) in path.name:
        if data_type.lower() in path.name.lower():
            return ImportMatrixConfig(path,
                                      data_type=data_type.lower(),
                                      #data_type_mapper=d_type_mapper
                                      )


@functools.cache
def get_sharkadm_id_handler(config_directory: str | pathlib.Path = None):
    print(f'{ID_CONFIG_DIRECTORY=}')
    config_directory = config_directory or ID_CONFIG_DIRECTORY
    return SharkadmIdsHandler(config_directory)


# def get_data_type_mapper(path: str | pathlib.Path = None) -> DataTypeMapper:
#     path = path or DEFAULT_DATA_TYPE_MAPPING_PATH
#     return DataTypeMapper(path)


if __name__ == '__main__':
    data = {}
    lev = 'visit'
    h = get_sharkadm_id_handler()
    lh = h.get_level_handler('phytoplankton', 'visit')