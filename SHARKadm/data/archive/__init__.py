import pathlib
from typing import Type

from .base import ArchiveBase
from .epibenthos import EpibenthosMartransArchive
from .pythonplankton import PhytoplanktonArchive
from .zoobenthos import ZoobenthosArchiveSkv
from .delivery_note import DeliveryNote

object_mapping = dict((cls._data_format.lower(), cls) for cls in ArchiveBase.__subclasses__())


def get_archive_data_holder(path: str | pathlib.Path) -> ArchiveBase:
    path = pathlib.Path(path)
    d_note = DeliveryNote(path / 'processed_data/delivery_note.txt')
    # print(f'{object_mapping=}')
    # print(f'{d_note.data_format=}')
    return object_mapping.get(d_note.data_format)(path)
