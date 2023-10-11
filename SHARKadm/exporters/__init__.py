from typing import Type

from .base import Exporter
from .txt_file import TxtAsIs
from .print_on_screen import PrintDataFrame


def get_exporter_list() -> list[str]:
    """Returns a sorted list of name of all available transformers"""
    print(f'{sorted([cls.__name__ for cls in Exporter.__subclasses__()])=}')
    return sorted([cls.__name__ for cls in Exporter.__subclasses__()])


def get_exporters() -> dict[str, Type[Exporter]]:
    """Returns a dictionary with exporters"""
    exporters = {}
    for cls in Exporter.__subclasses__():
        exporters[cls.__name__] = cls
    return exporters


def get_exporter_object(exporter_name: str, **kwargs) -> Exporter:
    """Returns Exporter object that matches teh given exporter name"""
    all_exporters = get_exporters()
    exporter = all_exporters[exporter_name]
    return exporter(**kwargs)
