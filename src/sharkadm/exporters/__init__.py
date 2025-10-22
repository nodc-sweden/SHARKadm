# ruff: noqa: I001
import functools
import pathlib
from typing import Type

from sharkadm import utils
from sharkadm.exporters.base import Exporter, PolarsExporter
from sharkadm.exporters.columns import ExportColumnViewsColumnsNotInData
from sharkadm.exporters.dataframe import DataFrame, PolarsDataFrame
from sharkadm.exporters.html_station_map import (
    PolarsHtmlMap,
    PolarsHtmlMapR,
    PolarsHtmlMapRred,
)
from sharkadm.exporters.ifcb_visualization import IfcbVisualizationFiles
from sharkadm.exporters.jellyfish import ExportJellyfishRowsFromLimsExport
from sharkadm.exporters.print_on_screen import PrintDataFrame
from sharkadm.exporters.shark_data_txt_file import (
    PolarsSHARKdataTxtAsGiven,
    SHARKdataTxt,
    SHARKdataTxtAsGiven,
    PolarsSHARKdataTxt,
)
from sharkadm.exporters.shark_metadata_auto import (
    SHARKMetadataAuto,
    PolarsSHARKMetadataAuto,
)
from sharkadm.exporters.species_translation import SpeciesTranslationTxt
from sharkadm.exporters.standard_format import PolarsStandardFormat
from sharkadm.exporters.statistics import PolarsPrintStatistics, PolarsStatisticsToTxt
from sharkadm.exporters.system import (
    ExportersSummaryFile,
    TransformersSummaryFile,
    ValidatorsSummaryFile,
)
from sharkadm.exporters.txt_file import TxtAsIs, PolarsTxtAsIs
from sharkadm.exporters.zip_archive import ZipArchive
from sharkadm.utils.inspect_kwargs import get_kwargs_for_class


@functools.cache
def get_exporter_list() -> list[str]:
    """Returns a sorted list of name of all available exporters"""
    return sorted(utils.get_all_class_children_names(PolarsExporter))


@functools.cache
def get_exporters() -> dict[str, Type[PolarsExporter]]:
    """Returns a dictionary with exporters"""
    return utils.get_all_class_children(PolarsExporter)
    # exporters = {}
    # for cls in PolarsExporter.__subclasses__():
    #     exporters[cls.__name__] = cls
    # return exporters


def get_exporter_object(name: str, **kwargs) -> PolarsExporter:
    """Returns PolarsExporter object that matches the given exporter name"""
    all_exporters = get_exporters()
    exporter = all_exporters[name]
    return exporter(**kwargs)


def get_exporters_description() -> dict[str, str]:
    """Returns a dictionary with exporter name as key and the description as value"""
    result = dict()
    for name, tran in get_exporters().items():
        result[name] = tran.get_exporter_description()
    return result


def get_exporters_info() -> dict:
    result = dict()
    for name, exp in get_exporters().items():
        result[name] = dict()
        result[name]["name"] = name
        result[name]["description"] = exp.get_exporter_description()
        result[name]["kwargs"] = get_kwargs_for_class(exp)
        # result[name]['kwargs'] = dict()
        # for key, value in inspect.signature(exp.__init__).parameters.items():
        #     if key in ['self', 'kwargs']:
        #         continue
        #     val = value.default
        #     if type(val) == type:
        #         val = None
        #     result[name]['kwargs'][key] = val
    return result


def get_exporters_description_text() -> str:
    info = get_exporters_description()
    line_length = 100
    lines = list()
    lines.append("=" * line_length)
    lines.append("Available exporters:")
    lines.append("-" * line_length)
    for key in sorted(info):
        lines.append(f"{key.ljust(30)}{info[key]}")
    lines.append("=" * line_length)
    return "\n".join(lines)


def print_exporters_description() -> None:
    """Prints all exporters on screen"""
    print(get_exporters_description_text())


def write_exporters_description_to_file(path: str | pathlib.Path) -> None:
    """Prints all exporters on screen"""
    with open(path, "w") as fid:
        fid.write(get_exporters_description_text())
