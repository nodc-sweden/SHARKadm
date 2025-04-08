import logging
import os
import pathlib
from typing import Protocol

from sharkadm import config_paths
from sharkadm.config.column_views import ColumnViews
from sharkadm.config.custom_id import CustomIdsHandler
from sharkadm.config.data_type_mapper import DataTypeMapper
from sharkadm.config.delivery_note_mapper import DeliveryNoteMapper
from sharkadm.config.import_matrix import ImportMatrixConfig, ImportMatrixMapper

logger = logging.getLogger(__name__)

DATA_STRUCTURES = ["row", "column"]

CONFIG_ENV = "NODC_CONFIG"

home = pathlib.Path.home()
OTHER_CONFIG_SOURCES = [
    home / "NODC_CONFIG",
    home / ".NODC_CONFIG",
    home / "nodc_config",
    home / ".nodc_config",
]

CONFIG_DIRECTORY = None
if os.getenv(CONFIG_ENV) and pathlib.Path(os.getenv(CONFIG_ENV)).exists():
    CONFIG_DIRECTORY = pathlib.Path(os.getenv(CONFIG_ENV))
else:
    for directory in OTHER_CONFIG_SOURCES:
        if directory.exists():
            CONFIG_DIRECTORY = directory
            break


def get_config_path(name: str | None = None) -> pathlib.Path:
    if not CONFIG_DIRECTORY:
        raise NotADirectoryError(
            f"Config directory not found. Environment path {CONFIG_ENV} does not seem to "
            f"be set and not other config directory was found. "
        )
    if not name:
        return CONFIG_DIRECTORY
    path = CONFIG_DIRECTORY / name
    if not path.exists():
        raise FileNotFoundError(f"Could not find config file {name}")
    return path


class DataHolderProtocol(Protocol):
    data_type = None
    data_type_internal = None
    header_mapper = None


def get_column_views_config(path: str | pathlib.Path | None = None) -> ColumnViews:
    path = path or DEFAULT_COLUMN_VIEWS_PATH
    return ColumnViews(path)


def get_import_matrix_config(data_type: str) -> ImportMatrixConfig | None:
    for name, path in import_matrix_paths.items():
        if data_type == name:
            return ImportMatrixConfig(
                path,
                data_type=data_type,
            )


def get_import_matrix_mapper(
    data_type: str,
    import_column: str,
    directory: str | pathlib.Path | None = None,
    **kwargs,
) -> ImportMatrixMapper | None:
    config = get_import_matrix_config(data_type)
    if not config:
        return
    return config.get_mapper(import_column)


def get_header_mapper_from_data_holder(
    data_holder: DataHolderProtocol, import_column: str
) -> ImportMatrixMapper | None:
    if import_column == "original":
        return data_holder.header_mapper
    return get_import_matrix_mapper(data_holder.data_type_internal, import_column)


def get_custom_id_handler(config_directory: str | pathlib.Path | None = None):
    config_directory = config_directory or adm_config_paths("ids")
    return CustomIdsHandler(config_directory)


def get_delivery_note_mapper(
    path: str | pathlib.Path | None = None,
) -> DeliveryNoteMapper:
    path = path or adm_config_paths("delivery_note_mapping")
    return DeliveryNoteMapper(path)


def get_data_type_mapper(path: str | pathlib.Path | None = None) -> DataTypeMapper:
    path = path or adm_config_paths("data_type_mapping")
    return DataTypeMapper(path)


def get_mapper_data_type_to_internal(
    path: str | pathlib.Path | None = None,
) -> DataTypeMapper:
    if not any((path, adm_config_paths)):
        return None

    path = path or adm_config_paths("mapper_data_type_to_internal")
    if not path:
        return None
    return DataTypeMapper(path)


def get_all_data_types() -> list[str]:
    return [path.stem.split("_", 2)[-1].lower() for path in import_matrix_paths.values()]


def get_valid_data_types(
    valid: list[str] | None = None, invalid: list[str] | None = None
) -> list[str]:
    if not any([valid, invalid]):
        return get_all_data_types()
    if valid:
        return [item.lower() for item in valid if item.lower() in get_all_data_types()]
    elif invalid:
        invalid_lower = [item.lower() for item in invalid]
        return [item for item in get_all_data_types() if item not in invalid_lower]


def get_valid_data_structures(
    valid: list[str] | None = None, invalid: list[str] | None = None
) -> list[str]:
    if not any([valid, invalid]):
        return DATA_STRUCTURES
    if valid:
        return [item.lower() for item in valid if item.lower() in DATA_STRUCTURES]
    elif invalid:
        invalid_lower = [item.lower() for item in invalid]
        return [item for item in DATA_STRUCTURES if item not in invalid_lower]


def get_adm_config_paths(config_directory=None):
    if not config_directory:
        return None

    root = config_directory / "sharkadm"
    return config_paths.ConfigPaths(root)


def get_import_matrix_config_paths(
    config_directory: pathlib.Path | None = None,
) -> dict[str, pathlib.Path]:
    paths = {}
    if not config_directory:
        return paths

    for path in config_directory.iterdir():
        if "import_matrix" not in path.name:
            continue
        key = path.stem.split("_", 2)[-1]
        paths[key] = path
    return paths


DEFAULT_COLUMN_VIEWS_PATH = (
    CONFIG_DIRECTORY / "column_views.txt" if CONFIG_DIRECTORY else None
)
adm_config_paths = get_adm_config_paths(CONFIG_DIRECTORY)
import_matrix_paths = get_import_matrix_config_paths(CONFIG_DIRECTORY)
mapper_data_type_to_internal = get_mapper_data_type_to_internal()
