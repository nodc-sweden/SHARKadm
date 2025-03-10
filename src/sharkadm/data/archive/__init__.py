import os
import pathlib
from typing import Union

from sharkadm import sharkadm_exceptions
from sharkadm import utils
from .archive_data_holder import ArchiveDataHolder
from .bacterioplankton import BacterioplanktonArchiveDataHolder
from .chlorophyll import ChlorophyllArchiveDataHolder
from .delivery_note import DeliveryNote
# from .epibenthos import EpibenthosArchiveDataHolder
from .epibenthos import EpibenthosMartransArchiveDataHolder
# from .ifcb import IfcbArchiveDataHolder
from .jellyfish import JellyfishArchiveDataHolder
from .physicalchemical import PhysicalChemicalArchiveDataHolder
from .phytoplankton import PhytoplanktonArchiveDataHolder
from .zoobenthos import ZoobenthosArchiveDataHolder
from .zoobenthos import ZoobenthosBedaArchiveDataHolder
from .zoobenthos import ZoobenthosBiomadArchiveDataHolder
from .zooplankton import ZooplanktonArchiveDataHolder
from .plankton_imaging import PlanktonImagingArchiveDataHolder


def all_subclasses(cls):
    return set(cls.__subclasses__()).union(
        [s for c in cls.__subclasses__() for s in all_subclasses(c)])


object_mapping = dict((cls._data_format, cls) for cls in all_subclasses(ArchiveDataHolder))
# object_mapping = dict((cls._data_format.lower(), cls) for cls in ArchiveDataHolder.__subclasses__())


def get_archive_data_holder(path: str | pathlib.Path) -> ArchiveDataHolder:
    path = pathlib.Path(path)
    d_note = DeliveryNote.from_txt_file(path / 'processed_data/delivery_note.txt')
    print(f'{object_mapping=}')
    print(f'{d_note.data_format=}')
    d_holder = object_mapping.get(d_note.data_format)
    if not d_holder:
        raise sharkadm_exceptions.ArchiveDataHolderError(d_note.data_format)
    return d_holder(path)


def directory_is_archive(directory: str | pathlib.Path) -> Union[pathlib.Path, False]:
    """Returns path to archive directory if it is recognised as an archive directory. Else returns False
    directory"""
    directory = pathlib.Path(directory)
    for root, dirs, files in os.walk(directory, topdown=False):
        for name in files:
            if name == 'delivery_note.txt':
                return pathlib.Path(root, name).parent.parent
    return False


def get_archive_data_holder_names() -> list[str]:
    return utils.get_all_class_children_names(ArchiveDataHolder)

