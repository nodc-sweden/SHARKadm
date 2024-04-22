import functools
import pathlib
from typing import Type

from sharkadm import utils
from .arithmetic import Divide
from .arithmetic import Multiply
from .analyse_info import AddAnalyseInfo
from .bacteria import AddBacteriaAsReportedScientificName
from .base import Transformer
from .boolean import FixYesNo
from .columns import AddColumnViewsColumns
from .columns import AddDEPHqcColumn
from .columns import RemoveColumns
from .cruise import AddCruiseId
from .custom_id import AddCustomId
from .dataset_name import AddDatasetName
from .datatype import AddDatatype
from .date_and_time import FixTimeFormat
from .date_and_time import FixDateFormat
from .date_and_time import AddSampleDate
from .date_and_time import AddSampleTime
from .date_and_time import AddDatetime
from .date_and_time import AddMonth
from .delivery_note_info import AddStatus
from .delivery_note_info import AddDeliveryNoteInfo
from .depth import AddSampleMinAndMaxDepth
from .depth import AddSectionStartAndEndDepth
from .depth import ReorderSampleMinAndMaxDepth
from .fake import FakeAddCTDtagToColumns
from .fake import FakeAddPressureFromDepth
from .lims import MoveLessThanFlagColumnFormat
from .lims import MoveLessThanFlagRowFormat
from .lims import RemoveNonDataLines
from .map_header import ArchiveMapper
from .parameter_unit_value import AddParameterUnitValueFromReported
from .parameter_unit_value import RemoveRowsWithNoParameterValue
from .position import AddSamplePosition
from .position import AddSamplePositionDM
from .position import AddSamplePositionSweref99tm
from .qc import AddColumnsForAutomaticQC
from .replace_comma_with_dot import ReplaceCommaWithDot
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
from .visit import AddVisitKey
from .wide_to_long import WideToLong
from ..utils.inspect_kwargs import get_kwargs_for_class
from .map_parameter_column import MapperParameterColumn
from .flags import ConvertFlagsFromLIMStoSDN

#######
from .aphia import AddAphiaId
from .bvol import AddBvolAphiaId
from .bvol import AddBvolRefList
from .bvol import AddBvolScientificName
from .bvol import AddBvolSizeClass
from .dyntaxa import AddDyntaxaId
from .dyntaxa import AddDyntaxaId
from .dyntaxa import AddDyntaxaScientificName
from .laboratory import AddEnglishAnalyticalLaboratory
from .laboratory import AddEnglishSamplingLaboratory
from .laboratory import AddSwedishAnalyticalLaboratory
from .laboratory import AddSwedishSamplingLaboratory
from .location import AddLocationCounty
from .location import AddLocationHelcomOsparArea
from .location import AddLocationMunicipality
from .location import AddLocationNation
from .location import AddLocationSeaBasin
from .location import AddLocationTypeArea
from .location import AddLocationWaterDistrict
from .orderer import AddEnglishSampleOrderer
from .orderer import AddSwedishSampleOrderer
from .project_code import AddEnglishProjectName
from .project_code import AddSwedishProjectName
from .red_list import AddRedList
from .reporting_institute import AddEnglishReportingInstitute
from .reporting_institute import AddSwedishReportingInstitute
from .station import AddStationInfo
from .taxon_rank import AddTaxonRanks

@functools.cache
def get_transformer_list() -> list[str]:
    """Returns a sorted list of name of all available transformers"""
    return sorted(utils.get_all_class_children_names(Transformer))


def get_transformers() -> dict[str, Type[Transformer]]:
    """Returns a dictionary with transformers"""
    return utils.get_all_class_children(Transformer)


def get_transformer_object(name: str, **kwargs) -> Transformer:
    """Returns Transformer object that matches the given transformer names"""
    all_trans = get_transformers()
    tran = all_trans[name]
    return tran(**kwargs)


def get_transformers_description() -> dict[str, str]:
    """Returns a dictionary with transformer name as key and the description as value"""
    result = dict()
    for name, tran in get_transformers().items():
        if name.startswith('_'):
            continue
        result[name] = tran.get_transformer_description()
    return result


def get_transformers_info() -> dict:
    result = dict()
    for name, tran in get_transformers().items():
        result[name] = dict()
        result[name]['name'] = name
        result[name]['description'] = tran.get_transformer_description()
        result[name]['kwargs'] = get_kwargs_for_class(tran)
    return result


def get_transformers_description_text() -> str:
    info = get_transformers_description()
    line_length = 100
    lines = list()
    lines.append('=' * line_length)
    lines.append('Available transformers:')
    lines.append('-' * line_length)
    for key in sorted(info):
        lines.append(f'{key.ljust(40)}{info[key]}')
    lines.append('=' * line_length)
    return '\n'.join(lines)


def print_transformers_description() -> None:
    """Prints all transformers on screen"""
    print(get_transformers_description_text())


def write_transformers_description_to_file(path: str | pathlib.Path) -> None:
    """Prints all transformers on screen"""
    with open(path, 'w') as fid:
        fid.write(get_transformers_description_text())


def get_physical_chemical_transformer_objects() -> list[Transformer]:
    return [
        AddRowNumber(),
        AddDEPHqcColumn(),
        ReplaceCommaWithDot(),
        FixTimeFormat(),
        # AddSamplePosition(),
        # ChangeDateFormat(),
        AddDatetime(),
        AddSamplePositionDM(),
        AddSamplePositionSweref99tm(),
        MoveLessThanFlagColumnFormat(),
        AddColumnsForAutomaticQC(),
        AddCruiseId(),
        AddVisitKey(),
        RemoveNonDataLines(),  # LIMS
        AddMonth(),
        AddSampleMinAndMaxDepth(),
        ReorderSampleMinAndMaxDepth(),
        AddDatatype(),
        # AddStatus(),
        AddDeliveryNoteInfo(),
        AddStationInfo(),
        AddEnglishProjectName(),
        AddSwedishProjectName(),
        AddEnglishSampleOrderer(),
        AddEnglishSamplingLaboratory(),
        AddSwedishSampleOrderer(),
        AddSwedishSamplingLaboratory(),
        AddSwedishAnalyticalLaboratory(),
        AddEnglishAnalyticalLaboratory(),
        AddLocationCounty(),
        AddLocationHelcomOsparArea(),
        AddLocationMunicipality(),
        AddLocationNation(),
        AddLocationSeaBasin(),
        AddLocationTypeArea(),
        AddLocationWaterDistrict(),
        AddStaticInternetAccessInfo(),
        AddDatasetName(),
        AddSwedishReportingInstitute(),
        AddEnglishReportingInstitute(),
        AddStaticDataHoldingCenter(),
        FixYesNo(),

        Multiply(),
        Divide(),

        WideToLong(),
        #
        AddAnalyseInfo(),
        AddSamplingInfo(),

        AddColumnViewsColumns(),
        AddSharkId(),
        SortData(),
    ]



