import pathlib
from typing import Type

from .base import Exporter
from .print_on_screen import PrintDataFrame
from .shark_data_txt_file import SHARKdataTxt
from .shark_metadata_auto import SHARKMetadataAuto
from .txt_file import TxtAsIs
from .zip_archive import ZipArchive
from .html_station_map import HtmlStationMap


def get_exporter_list() -> list[str]:
    """Returns a sorted list of name of all available exporters"""
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


def get_exporters_description() -> dict[str, str]:
    """Returns a dictionary with exporter name as key and the description as value"""
    result = dict()
    for name, tran in get_exporters().items():
        result[name] = tran.get_exporter_description()
    return result


def get_exporters_description_text() -> str:
    info = get_exporters_description()
    line_length = 100
    lines = list()
    lines.append('=' * line_length)
    lines.append('Available exporters are:')
    lines.append('-' * line_length)
    for key in sorted(info):
        lines.append(f'{key.ljust(30)}{info[key]}')
    lines.append('=' * line_length)
    return '\n'.join(lines)


def print_exporters_description() -> None:
    """Prints all exporters on screen"""
    print(get_exporters_description_text())


def write_exporters_description_to_file(path: str | pathlib.Path) -> None:
    """Prints all exporters on screen"""
    with open(path, 'w') as fid:
        fid.write(get_exporters_description_text())
