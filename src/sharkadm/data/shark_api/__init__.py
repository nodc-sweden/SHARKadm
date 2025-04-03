from sharkadm import config
from .shark_api import SHARKapiDataHolder


def get_shark_api_data_holder(**kwargs) -> SHARKapiDataHolder:
    mapper = config.get_import_matrix_mapper(**kwargs)
    return SHARKapiDataHolder(header_mapper=mapper, **kwargs)
