import pathlib
import functools
import sys

from SHARKadm import adm_config_paths
from .import_config import ImportMatrixConfig
from .column_info import ColumnInfoConfig
from .column_views import ColumnViews
from .sharkadm_id import SharkadmIdsHandler
from .data_type_mapper import DataTypeMapper
from .delivery_note_mapper import DeliveryNoteMapper
from .physical_chemical_mapper import PhysicalChemicalMapper


if getattr(sys, 'frozen', False):
    THIS_DIR = pathlib.Path(sys.executable).parent
else:
    THIS_DIR = pathlib.Path(__file__).parent



# ID_CONFIG_DIRECTORY = pathlib.Path(THIS_DIR, 'etc', 'ids')
# DEFAULT_IMPORT_MATRIX_DIRECTORY = pathlib.Path(THIS_DIR, 'etc', 'import_matrix')
#
# DEFAULT_PHYSICAL_CHEMICAL_MAPPER = pathlib.Path(THIS_DIR, 'etc', 'physical_chemical_mapping.txt')
#
# DEFAULT_DELIVERY_NOTE_MAPPER = pathlib.Path(THIS_DIR, 'etc', 'delivery_note_mapping.txt')
#
# DEFAULT_COLUMN_INFO_PATH = pathlib.Path(THIS_DIR, 'etc', 'column_info.yaml')
# DEFAULT_COLUMN_VIEWS_PATH = pathlib.Path(THIS_DIR, 'etc', 'column_views.txt')

# DEFAULT_DATA_TYPE_MAPPING_PATH = pathlib.Path(THIS_DIR, 'etc', 'data_type_mapping.yaml')


@functools.cache
def get_physical_chemical_mapper(path: str | pathlib.Path = None) -> PhysicalChemicalMapper:
    path = path or adm_config_paths.get('physical_chemical_mapping')
    return PhysicalChemicalMapper(path)


@functools.cache
def get_column_info_config(path: str | pathlib.Path = None) -> ColumnInfoConfig:
    path = path or adm_config_paths.get('column_info')
    return ColumnInfoConfig(path)


@functools.cache
def get_column_views_config(path: str | pathlib.Path = None) -> ColumnViews:
    path = path or adm_config_paths.get('column_views')
    return ColumnViews(path)


# def get_import_matrix_list(directory: str | pathlib.Path = None) -> list[str]:
#     directory = directory or DEFAULT_IMPORT_MATRIX_DIRECTORY
#     return [path.stem.split('_', 2)[-1] for path in pathlib.Path(directory).iterdir()]

@functools.cache
def get_import_matrix_config(data_type: str, directory: str | pathlib.Path = None,
                             data_type_mapping_path: str | pathlib.Path = None) -> ImportMatrixConfig:
    directory = directory or adm_config_paths.get('import_matrix')
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
    config_directory = config_directory or adm_config_paths.get('ids')
    return SharkadmIdsHandler(config_directory)


@functools.cache
def get_delivery_note_mapper(path: str | pathlib.Path = None) -> DeliveryNoteMapper:
    path = path or adm_config_paths.get('delivery_note_mapping')
    return DeliveryNoteMapper(path)


@functools.cache
def get_all_data_types() -> list[str]:
    return [path.stem.split('_', 2)[-1].lower() for path in adm_config_paths.get('import_matrix').iterdir()]


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


if __name__ == '__main__':
    data = {}
    lev = 'visit'
    h = get_sharkadm_id_handler()
    lh = h.get_level_handler('phytoplankton', 'visit')