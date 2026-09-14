import pathlib
from typing import Protocol

from nodc_config import Config

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
    config: Config


def get_column_views_config(nodc_conf: Config) -> ColumnViews:
    path = nodc_conf.get_path("column_views")
    return ColumnViews(path)


def get_translate_headers_config(
    nodc_conf: Config,
) -> TranslateHeaders:
    path = nodc_conf.get_path("translate_headers")
    return TranslateHeaders(path)


def get_trophic_type_smhi_object(
    nodc_conf: Config,
) -> TrophicTypeSMHI:
    path = nodc_conf.get_path("trophictype_smhi")
    return TrophicTypeSMHI(path)


def get_import_matrix_config(
    nodc_conf: Config, data_type: str, **kwargs
) -> ImportMatrixConfig | None:
    path = get_import_matrix_config_paths(nodc_conf).get(data_type)
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
    nodc_conf: Config,
    data_type: str,
    import_column: str,
    **kwargs,
) -> ImportMatrixMapper | None:
    config = get_import_matrix_config(nodc_conf, data_type, **kwargs)
    if not config:
        return
    return config.get_mapper(import_column)


def get_header_mapper_from_data_holder(
    data_holder: DataHolderProtocol, import_column: str
) -> ImportMatrixMapper | None:
    if import_column == "original":
        return data_holder.header_mapper
    return get_import_matrix_mapper(
        data_holder.config, data_holder.data_type_internal, import_column
    )


def get_custom_id_handler(nodc_conf: Config):
    config_directory = nodc_conf.get_directory("ids")
    return CustomIdsHandler(config_directory) if config_directory else None


def get_delivery_note_mapper(
    nodc_conf: Config,
) -> DeliveryNoteMapper:
    path = nodc_conf("delivery_note_mapping")
    return DeliveryNoteMapper(path)


def get_data_type_mapper(nodc_conf: Config) -> DataTypeMapper:
    path = nodc_conf("data_type_mapping")
    return DataTypeMapper(path)


def get_mapper_data_type_to_internal(
    nodc_conf: Config,
) -> DataTypeMapper | None:
    path = nodc_conf("mapper_data_type_to_internal")
    if not path:
        return None
    return DataTypeMapper(path)


def get_all_data_types(nodc_conf: Config) -> list[str]:
    return [
        path.stem.split("_", 2)[-1].lower()
        for path in get_import_matrix_config_paths(nodc_conf).values()
    ]


def get_all_data_structures() -> list[str]:
    return DATA_STRUCTURES


def get_valid_data_types(
    nodc_conf: Config,
    valid: tuple[str, ...] | None = None,
    invalid: tuple[str, ...] | None = None,
) -> list[str]:
    if not any([valid, invalid]):
        return get_all_data_types(nodc_conf=nodc_conf)
    if valid:
        return [
            item.lower()
            for item in valid
            if item.lower() in get_all_data_types(nodc_conf)
        ]
    elif invalid:
        invalid_lower = [item.lower() for item in invalid]
        return [
            item for item in get_all_data_types(nodc_conf) if item not in invalid_lower
        ]


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


def get_import_matrix_config_paths(nodc_conf: Config) -> dict[str, pathlib.Path]:
    paths = {}
    try:
        for name, path in nodc_conf.get_paths("import_matrix").items():
            key = path.stem.split("_", 2)[-1]
            paths[key] = path
        return paths
    except AttributeError:
        return dict()
