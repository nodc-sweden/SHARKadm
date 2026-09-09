import os
import pathlib
from typing import Protocol

from nodc_config import nodc_conf

from sharkadm.config import utils
from sharkadm.config.column_views import ColumnViews
from sharkadm.config.custom_id import CustomIdsHandler
from sharkadm.config.data_type_mapper import DataTypeMapper
from sharkadm.config.delivery_note_mapper import DeliveryNoteMapper
from sharkadm.config.import_matrix import ImportMatrixConfig, ImportMatrixMapper
from sharkadm.config.translate_headers import TranslateHeaders
from sharkadm.config.trophic_type_smhi import TrophicTypeSMHI

DATA_STRUCTURES = ["row", "column", "profile"]


class DataHolderProtocol(Protocol):
    data_type = None
    data_type_internal = None
    header_mapper = None


def get_column_views_config(path: str | pathlib.Path | None = None) -> ColumnViews:
    path = path or nodc_conf.get_path("column_views")
    return ColumnViews(path)


def get_translate_headers_config(
    path: str | pathlib.Path | None = None,
) -> TranslateHeaders:
    path = path or nodc_conf.get_path("translate_headers")
    return TranslateHeaders(path)


def get_trophic_type_smhi_object(
    path: str | pathlib.Path | None = None,
) -> TrophicTypeSMHI:
    path = path or nodc_conf.get_path("trophictype_smhi")
    return TrophicTypeSMHI(path)


def get_import_matrix_config(data_type: str, **kwargs) -> ImportMatrixConfig | None:
    path = get_import_matrix_config_paths().get(data_type)
    if not path:
        return
    return ImportMatrixConfig(path, data_type=data_type, **kwargs)
    # for name, path in get_import_matrix_config_paths().items():
    #     if data_type == name:
    #         return ImportMatrixConfig(
    #             path,
    #             data_type=data_type,
    #         )


def get_import_matrix_mapper(
    data_type: str,
    import_column: str,
    directory: str | pathlib.Path | None = None,
    **kwargs,
) -> ImportMatrixMapper | None:
    config = get_import_matrix_config(data_type, **kwargs)
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
    config_directory = config_directory or nodc_conf("ids")
    return CustomIdsHandler(config_directory) if config_directory else None


def get_delivery_note_mapper(
    path: str | pathlib.Path | None = None,
) -> DeliveryNoteMapper:
    path = path or nodc_conf("delivery_note_mapping")
    return DeliveryNoteMapper(path)


def get_data_type_mapper(path: str | pathlib.Path | None = None) -> DataTypeMapper:
    path = path or nodc_conf("data_type_mapping")
    return DataTypeMapper(path)


def get_mapper_data_type_to_internal(
    path: str | pathlib.Path | None = None,
) -> DataTypeMapper:
    if not any((path, nodc_conf)):
        return None

    path = path or nodc_conf("mapper_data_type_to_internal")
    if not path:
        return None
    return DataTypeMapper(path)


def get_all_data_types() -> list[str]:
    return [
        path.stem.split("_", 2)[-1].lower()
        for path in get_import_matrix_config_paths().values()
    ]


def get_all_data_structures() -> list[str]:
    return DATA_STRUCTURES


def get_valid_data_types(
    valid: tuple[str, ...] | None = None, invalid: tuple[str, ...] | None = None
) -> list[str]:
    if not any([valid, invalid]):
        return get_all_data_types()
    if valid:
        return [item.lower() for item in valid if item.lower() in get_all_data_types()]
    elif invalid:
        invalid_lower = [item.lower() for item in invalid]
        return [item for item in get_all_data_types() if item not in invalid_lower]


def get_valid_data_structures(
    valid: tuple[str, ...] | None = None, invalid: tuple[str, ...] | None = None
) -> list[str]:
    if not any([valid, invalid]):
        return get_all_data_structures()
    if valid:
        return [
            item.lower() for item in valid if item.lower() in get_all_data_structures()
        ]
    elif invalid:
        invalid_lower = [item.lower() for item in invalid]
        return [item for item in get_all_data_structures() if item not in invalid_lower]


def get_import_matrix_config_paths() -> dict[str, pathlib.Path]:
    paths = {}
    for name, path in nodc_conf.get_paths("import_matrix").items():
        key = path.stem.split("_", 2)[-1]
        paths[key] = path
    return paths


# Fixa vidare med nodc_config. Synca mikrotjänster och sharkadm. se över svn-funktionen
