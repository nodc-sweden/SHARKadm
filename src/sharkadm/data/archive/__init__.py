# ruff: noqa: F401
import os
import pathlib
from typing import Union

from sharkadm import sharkadm_exceptions, utils
from sharkadm.data.archive.archive_data_holder import (
    ArchiveDataHolder,
    PolarsArchiveDataHolder,
)
from sharkadm.data.archive.bacterioplankton import BacterioplanktonArchiveDataHolder
from sharkadm.data.archive.chlorophyll import ChlorophyllArchiveDataHolder
from sharkadm.data.archive.delivery_note import DeliveryNote
from sharkadm.data.archive.epibenthos import EpibenthosMartransArchiveDataHolder
from sharkadm.data.archive.jellyfish import JellyfishArchiveDataHolder
from sharkadm.data.archive.physicalchemical import PhysicalChemicalArchiveDataHolder
from sharkadm.data.archive.phytoplankton import PhytoplanktonArchiveDataHolder
from sharkadm.data.archive.plankton_imaging import PlanktonImagingArchiveDataHolder
from sharkadm.data.archive.zoobenthos import (
    ZoobenthosArchiveDataHolder,
    ZoobenthosBedaArchiveDataHolder,
    ZoobenthosBiomadArchiveDataHolder,
)
from sharkadm.data.archive.zooplankton import ZooplanktonArchiveDataHolder


def all_subclasses(cls):
    return set(cls.__subclasses__()).union(
        [s for c in cls.__subclasses__() for s in all_subclasses(c)]
    )


object_mapping = dict(
    (cls._data_format, cls) for cls in all_subclasses(ArchiveDataHolder)
)
polars_object_mapping = dict(
    (cls._data_format, cls) for cls in all_subclasses(PolarsArchiveDataHolder)
)
# object_mapping = dict(
#     (cls._data_format.lower(), cls) for cls in ArchiveDataHolder.__subclasses__()
# )


def get_archive_data_holder(path: str | pathlib.Path) -> ArchiveDataHolder:
    path = pathlib.Path(path)
    d_note = DeliveryNote.from_txt_file(path / "processed_data/delivery_note.txt")
    d_holder = object_mapping.get(d_note.data_format)
    if not d_holder:
        raise sharkadm_exceptions.ArchiveDataHolderError(d_note.data_format)
    return d_holder(path)


def get_polars_archive_data_holder(path: str | pathlib.Path) -> PolarsArchiveDataHolder:
    path = pathlib.Path(path)
    d_note = DeliveryNote.from_txt_file(path / "processed_data/delivery_note.txt")
    d_holder = polars_object_mapping.get(d_note.data_format)
    if not d_holder:
        raise sharkadm_exceptions.ArchiveDataHolderError(d_note.data_format)
    return d_holder(path)


def directory_is_archive(directory: str | pathlib.Path) -> Union[pathlib.Path, False]:
    """Returns path to archive directory if it is recognised as an archive directory.
    Else returns False."""
    directory = pathlib.Path(directory)
    for root, dirs, files in os.walk(directory, topdown=False):
        for name in files:
            if name == "delivery_note.txt":
                return pathlib.Path(root, name).parent.parent
    return False


def get_archive_data_holder_names() -> list[str]:
    return utils.get_all_class_children_names(ArchiveDataHolder)
