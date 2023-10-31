import pathlib
from typing import Type

from .archive_data_holder import ArchiveDataHolder
from .epibenthos import EpibenthosMartransArchiveDataHolder
from .pythonplankton import PhytoplanktonArchiveDataHolder
from .zoobenthos import ZoobenthosArchiveSkvDataHolder
from .delivery_note import DeliveryNote

object_mapping = dict((cls._data_format.lower(), cls) for cls in ArchiveDataHolder.__subclasses__())


def get_archive_data_holder(path: str | pathlib.Path) -> ArchiveDataHolder:
    path = pathlib.Path(path)
    d_note = DeliveryNote(path / 'processed_data/delivery_note.txt')
    # print(f'{object_mapping=}')
    # print(f'{d_note.data_format=}')
    return object_mapping.get(d_note.data_format)(path)
