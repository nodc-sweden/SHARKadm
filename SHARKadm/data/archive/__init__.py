import os
import pathlib
from typing import Union

from .archive_data_holder import ArchiveDataHolder
from .epibenthos import EpibenthosMartransArchiveDataHolder
from .pythonplankton import PhytoplanktonArchiveDataHolder
from .zoobenthos import ZoobenthosArchiveSkvDataHolder
from .delivery_note import DeliveryNote
from SHARKadm import utils

object_mapping = dict((cls._data_format.lower(), cls) for cls in ArchiveDataHolder.__subclasses__())


def get_archive_data_holder(path: str | pathlib.Path) -> ArchiveDataHolder:
    path = pathlib.Path(path)
    d_note = DeliveryNote.from_txt_file(path / 'processed_data/delivery_note.txt')
    # print(f'{object_mapping=}')
    # print(f'{d_note.data_format=}')
    return object_mapping.get(d_note.data_format)(path)


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

