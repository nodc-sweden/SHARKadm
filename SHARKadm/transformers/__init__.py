import functools
import pathlib
from typing import Type

from SHARKadm import utils
from .aphia import AddAphiaId
from .arithmetic import Divide
from .arithmetic import Multiply
from .base import Transformer
from .boolean import FixYesNo
from .bvol import AddBvolAphiaId
from .bvol import AddBvolRefList
from .bvol import AddBvolScientificName
from .bvol import AddBvolSizeClass
from .columns import AddColumnViewsColumns
from .columns import AddDEPHqcColumn
from .copy_variable import CopyVariable
from .cruise import AddCruiseId
from .custom_id import AddCustomId
from .dataset_name import AddDatasetName
from .datatype import AddDatatype
from .date_and_time import AddDateAndTimeToAllLevels
from .date_and_time import AddDatetime
from .date_and_time import AddMonth
from .delivery_note_info import AddStatus
from .depth import AddSampleMinAndMaxDepth
from .depth import AddSectionStartAndEndDepth
from .depth import ReorderSampleMinAndMaxDepth
from .dyntaxa import AddDyntaxaId
from .dyntaxa import AddDyntaxaId
from .dyntaxa import AddDyntaxaScientificName
from .fake import FakeAddCTDtagToColumns
from .fake import FakeAddPressureFromDepth
from .laboratory import AddEnglishAnalyticalLaboratory
from .laboratory import AddEnglishSamplingLaboratory
from .laboratory import AddSwedishAnalyticalLaboratory
from .laboratory import AddSwedishSamplingLaboratory
from .lims import MoveLessThanFlag
from .lims import RemoveNonDataLines
from .location import AddLocationCounty
from .location import AddLocationHelcomOsparArea
from .location import AddLocationMunicipality
from .location import AddLocationNation
from .location import AddLocationSeaBasin
from .location import AddLocationTypeArea
from .location import AddLocationWaterDistrict
from .map_header import ArchiveMapper
from .map_header import PhysicalChemicalMapper
from .orderer import AddEnglishSampleOrderer
from .orderer import AddSwedishSampleOrderer
from .parameter_unit_value import AddParameterUnitValueFromReported
from .parameter_unit_value import RemoveRowsWithNoParameterValue
from .position import AddPositionDD
from .position import AddPositionDM
from .position import AddPositionSweref99tm
from .position import AddPositionToAllLevels
from .project_code import AddEnglishProjectName
from .project_code import AddSwedishProjectName
from .qc import AddColumnsForAutomaticQC
from .red_list import AddRedList
from .replace_comma_with_dot import ReplaceCommaWithDot
from .reporting_institute import AddEnglishReportingInstitute
from .reporting_institute import AddSwedishReportingInstitute
from .row import AddRowNumber
from .sampler_area import AddCalculatedSamplerArea
from .sampling_info import AddSamplingInfo
from .scientific_name import AddReportedScientificName
from .scientific_name import AddScientificName
from .shark_id import AddSharkId
from .sort_data import SortData
from .static_data_holding_center import AddStaticDataHoldingCenter
from .static_internet_access import AddStaticInternetAccessInfo
from .static_internet_access import AddStaticInternetAccessInfo
from .station import AddStationInfo
from .station import AddStationVissEuId
from .taxon_rank import AddTaxonRanks
from .visit import AddVisitKey
from .wide_to_long import WideToLong
from ..utils.inspect_kwargs import get_kwargs_for_class


@functools.cache
def get_transformer_list() -> list[str]:
    """Returns a sorted list of name of all available transformers"""
    return sorted(utils.get_all_class_children_names(Transformer))


def get_transformers() -> dict[str, Type[Transformer]]:
    """Returns a dictionary with transformers"""
    return utils.get_all_class_children(Transformer)


def get_transformer_object(trans_name: str, **kwargs) -> Transformer:
    """Returns Transformer object that matches the given transformer names"""
    all_trans = get_transformers()
    tran = all_trans[trans_name]
    return tran(**kwargs)


def get_transformers_description() -> dict[str, str]:
    """Returns a dictionary with transformer name as key and the description as value"""
    result = dict()
    for name, tran in get_transformers().items():
        result[name] = tran.get_transformer_description()
    return result


def get_transformers_info() -> dict:
    result = dict()
    for name, tran in get_transformers().items():
        result[name] = dict()
        result[name]['name'] = name
        result[name]['description'] = tran.get_transformer_description()
        result[name]['kwargs'] = get_kwargs_for_class(tran)
        # result[name]['kwargs'] = dict()
        # for key, value in inspect.signature(tran.__init__).parameters.items():
        #     if key in ['self', 'kwargs']:
        #         continue
        #     result[name]['kwargs'][key] = value.default
    return result


def get_transformers_description_text() -> str:
    info = get_transformers_description()
    line_length = 100
    lines = list()
    lines.append('=' * line_length)
    lines.append('Available transformers are:')
    lines.append('-' * line_length)
    for key in sorted(info):
        lines.append(f'{key.ljust(30)}{info[key]}')
    lines.append('=' * line_length)
    return '\n'.join(lines)


def print_transformers_description() -> None:
    """Prints all transformers on screen"""
    print(get_transformers_description_text())


def write_transformers_description_to_file(path: str | pathlib.Path) -> None:
    """Prints all transformers on screen"""
    with open(path, 'w') as fid:
        fid.write(get_transformers_description_text())



