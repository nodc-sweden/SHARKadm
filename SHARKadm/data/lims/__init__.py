from SHARKadm import config
from .lims_data_holder import LimsDataHolder
import pathlib


def get_lims_data_holder(path: str | pathlib.Path) -> LimsDataHolder:
    mapper = config.get_physical_chemical_mapper()
    return LimsDataHolder(lims_root_directory=path, header_mapper=mapper)
