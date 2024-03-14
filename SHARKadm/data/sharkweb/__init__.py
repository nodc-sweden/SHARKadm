from SHARKadm import config
from .sharkweb import SHARKwebDataHolder


def get_sharkweb_data_holder(**kwargs) -> SHARKwebDataHolder:
    mapper = config.get_import_matrix_mapper(**kwargs)
    return SHARKwebDataHolder(header_mapper=mapper, **kwargs)

