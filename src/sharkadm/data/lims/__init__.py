import os
from typing import Union

from sharkadm import config
from .lims_data_holder import LimsDataHolder
import pathlib


def get_lims_data_holder(path: str | pathlib.Path) -> LimsDataHolder:
    # mapper = config.get_physical_chemical_mapper()
    mapper = config.get_import_matrix_mapper(data_type='PhysicalChemical', import_column='SHARKarchive')
    return LimsDataHolder(lims_root_directory=path, header_mapper=mapper)


def directory_is_lims(directory: str | pathlib.Path) -> Union[pathlib.Path, False]:
    """Returns path to lims directory if it is recognised as a lims directory. Else returns False
    directory"""
    directory = pathlib.Path(directory)
    for root, dirs, files in os.walk(directory, topdown=False):
        for name in dirs:
            if name.lower() == 'raw_data':
                return pathlib.Path(root, name).parent
    return False
